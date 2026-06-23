"""
USDA NASS QuickStats Client for Teddy Data Pipeline (Dataset 1 — crop yields & commodity baselines)

This client fetches county-level trailing yields / inventory for a parcel's relevant
commodities from the USDA NASS QuickStats API and stores them in Snowflake, following
the same one-time, claim-time enrichment pattern used by the NLCD land-cover processor.

Source:   USDA NASS QuickStats API  (https://quickstats.nass.usda.gov/api)
          GET https://quickstats.nass.usda.gov/api/api_GET/?key=...&format=JSON&...
          - Free, public, requires an API key (stored in AWS Secrets Manager, NOT in repo).
          - Max 50,000 records per request; use get_counts to size a query first.

Inputs we already have on the parcel record (RAW.PARCEL_PROFILE / CURATED.PARCEL_PROFILES):
          - county_fips (5-digit state+county FIPS)
          - state_code  (2-letter postal abbreviation)
          - land_use_description (optional — used only to prioritise commodities)

Plug-in:  Results are written to TEDDY_DATA.RAW.USDA_CROP_YIELDS, surfaced read-only by
          Teddy as: "Land like yours in {county} averaged {yield}/acre."

Null-safety: a commodity with no county data is silently omitted (never raises). The USDA
          API returns HTTP 400 with {"error": [...]} when a query matches zero records;
          that is treated as "no data", not as a failure.

Environment Variables:
    ENVIRONMENT          dev/prod (default: dev)
    USDA_YIELD_PERIODS   number of trailing reporting periods to keep (default: 3)
    USDA_NASS_API_KEY    fallback API key for local dev (prefer Secrets Manager)
"""

import json
import time
import logging
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field

import boto3
import requests

import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared.utils.snowflake_connector import get_snowflake_connector

logger = logging.getLogger(__name__)

QUICKSTATS_BASE_URL = "https://quickstats.nass.usda.gov/api/api_GET/"
DATA_SOURCE = "usda_nass_quickstats"
DEFAULT_TRAILING_PERIODS = int(os.environ.get("USDA_YIELD_PERIODS", "3"))

# Tokens NASS uses for suppressed / not-available values — never numeric.
NON_NUMERIC_VALUES = {"(D)", "(NA)", "(Z)", "(X)", "(S)", "(L)", "(H)", "", None}


@dataclass
class CommoditySpec:
    """How to query QuickStats for one commodity, and how to describe it to Teddy."""
    commodity_desc: str                     # QuickStats commodity_desc (e.g. "CORN")
    sector_desc: str                        # "CROPS" or "ANIMALS & PRODUCTS"
    statisticcat_desc: str                  # "YIELD" or "INVENTORY"
    source_desc: str                        # "SURVEY" (annual) or "CENSUS" (5-yearly)
    label: str                              # human label for Teddy copy ("corn")
    unit_contains: Optional[str] = None     # keep only series whose unit_desc contains this
    preferred_series: Optional[str] = None  # exact short_desc to prefer when present


# The commodities the spec calls out: corn, soybeans, wheat, hay, cattle.
# Crops use annual SURVEY YIELD; cattle uses county INVENTORY (Census-backed, broadest coverage).
#
# A single (commodity, county) query returns MANY sub-series — by production practice
# (irrigated / all), utilization (grain / silage), and for livestock by class and by
# OPERATIONS (farm counts) vs HEAD. `unit_contains` filters to the comparable unit and
# `preferred_series` pins the headline series, so we never mix or surface a farm-count.
COMMODITY_SPECS: Dict[str, CommoditySpec] = {
    "CORN":     CommoditySpec("CORN", "CROPS", "YIELD", "SURVEY", "corn", unit_contains="ACRE"),
    "SOYBEANS": CommoditySpec("SOYBEANS", "CROPS", "YIELD", "SURVEY", "soybeans", unit_contains="ACRE"),
    "WHEAT":    CommoditySpec("WHEAT", "CROPS", "YIELD", "SURVEY", "wheat", unit_contains="ACRE"),
    "HAY":      CommoditySpec("HAY", "CROPS", "YIELD", "SURVEY", "hay", unit_contains="ACRE"),
    "CATTLE":   CommoditySpec("CATTLE", "ANIMALS & PRODUCTS", "INVENTORY", "CENSUS", "cattle",
                              unit_contains="HEAD",
                              preferred_series="CATTLE, INCL CALVES - INVENTORY"),
}


def get_usda_api_key(environment: Optional[str] = None) -> Optional[str]:
    """
    Resolve the USDA NASS QuickStats API key from AWS Secrets Manager, mirroring the
    REGRID_API_KEY pattern in api_parcel_ingestion.py. Falls back to an env var for local
    development. Returns None if no key is configured (caller treats that as "skip").
    """
    environment = environment or os.environ.get("ENVIRONMENT", "dev")
    try:
        secrets_client = boto3.client("secretsmanager")
        secret_name = f"teddy-data-pipeline-secrets-{environment}"
        response = secrets_client.get_secret_value(SecretId=secret_name)
        secrets = json.loads(response["SecretString"])
        key = secrets.get("USDA_NASS_API_KEY")
        if key:
            return key
    except Exception as e:  # pragma: no cover - exercised only with live AWS
        logger.warning(f"Could not get USDA_NASS_API_KEY from secrets: {e}")
    return os.environ.get("USDA_NASS_API_KEY")


@dataclass
class CommodityYield:
    """Trailing yield/inventory records for one commodity in one county."""
    commodity: str
    label: str
    statistic: str
    unit: Optional[str]
    series: Optional[str] = None                                 # chosen short_desc
    periods: List[Dict[str, Any]] = field(default_factory=list)  # newest first

    def to_dict(self) -> Dict[str, Any]:
        return {
            "commodity": self.commodity,
            "label": self.label,
            "statistic": self.statistic,
            "unit": self.unit,
            "series": self.series,
            "periods": self.periods,
        }


class USDANassClient:
    """Rate-limited HTTP client for the USDA NASS QuickStats API."""

    def __init__(self, api_key: Optional[str], rate_limit_delay: float = 1.0,
                 trailing_periods: int = DEFAULT_TRAILING_PERIODS):
        self.api_key = api_key
        self.trailing_periods = max(1, trailing_periods)
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Teddy-DataPipeline/1.0"})
        self.rate_limit_delay = rate_limit_delay
        self.last_request_time = 0.0

    def _rate_limit(self):
        elapsed = time.time() - self.last_request_time
        if elapsed < self.rate_limit_delay:
            time.sleep(self.rate_limit_delay - elapsed)
        self.last_request_time = time.time()

    @staticmethod
    def _parse_value(raw: Any) -> Optional[float]:
        """Parse a NASS value string into a float, or None if suppressed/non-numeric."""
        if raw in NON_NUMERIC_VALUES:
            return None
        try:
            return float(str(raw).replace(",", "").strip())
        except (ValueError, AttributeError):
            return None

    @staticmethod
    def split_fips(county_fips: Optional[str], state_fips: Optional[str] = None,
                   county_code: Optional[str] = None):
        """
        Derive (state_fips_code, county_code) from a 5-digit county FIPS, with explicit
        overrides honoured. Returns (None, None) when it cannot be determined.
        """
        if state_fips and county_code:
            return state_fips.zfill(2), county_code.zfill(3)
        if county_fips:
            digits = "".join(ch for ch in str(county_fips) if ch.isdigit())
            if len(digits) == 5:
                return digits[:2], digits[2:]
            if len(digits) == 4:  # some sources drop a leading zero on the state
                return digits[:1].zfill(2), digits[1:]
        return None, None

    def get_county_commodity(self, spec: CommoditySpec, state_fips: str,
                             county_code: str) -> Optional[CommodityYield]:
        """
        Fetch the trailing yield/inventory for a single commodity in one county.
        Returns None when the county has no data for that commodity (null-safe / omit).
        """
        if not self.api_key:
            logger.warning("No USDA NASS API key configured; skipping commodity fetch")
            return None

        params = {
            "key": self.api_key,
            "format": "JSON",
            "commodity_desc": spec.commodity_desc,
            "sector_desc": spec.sector_desc,
            "statisticcat_desc": spec.statisticcat_desc,
            "source_desc": spec.source_desc,
            "domain_desc": "TOTAL",   # drop by-size/economic-class breakdown sub-series
            "agg_level_desc": "COUNTY",
            "state_fips_code": state_fips,
            "county_code": county_code,
        }

        self._rate_limit()
        try:
            resp = self.session.get(QUICKSTATS_BASE_URL, params=params, timeout=30)
        except requests.exceptions.RequestException as e:
            logger.warning(f"USDA request failed for {spec.commodity_desc} "
                           f"{state_fips}{county_code}: {e}")
            return None

        # NASS returns HTTP 400 with {"error": [...]} when a query matches zero records.
        if resp.status_code == 400:
            logger.info(f"No USDA data for {spec.commodity_desc} in "
                        f"{state_fips}{county_code} (400 / empty)")
            return None
        if resp.status_code != 200:
            logger.warning(f"USDA returned {resp.status_code} for {spec.commodity_desc} "
                           f"{state_fips}{county_code}; omitting")
            return None

        try:
            records = resp.json().get("data", [])
        except ValueError:
            logger.warning(f"USDA returned non-JSON for {spec.commodity_desc}; omitting")
            return None

        return self._build_commodity_yield(spec, records)

    def _build_commodity_yield(self, spec: CommoditySpec,
                               records: List[Dict[str, Any]]) -> Optional[CommodityYield]:
        """
        Reduce raw NASS records to the trailing N numeric reporting periods of a SINGLE
        comparable series. A county query returns many sub-series (production practice,
        utilization, livestock class, and farm-count "OPERATIONS" series); we filter to
        the comparable unit, group by short_desc, pick one headline series, then trail it.
        """
        # 1. Parse numeric rows, honouring the unit filter (drops e.g. OPERATIONS counts).
        parsed = []
        for rec in records:
            value = self._parse_value(rec.get("Value"))
            if value is None:
                continue
            try:
                year = int(rec.get("year"))
            except (TypeError, ValueError):
                continue
            unit = rec.get("unit_desc")
            if spec.unit_contains and (not unit or spec.unit_contains.upper() not in unit.upper()):
                continue
            parsed.append({
                "year": year,
                "value": value,
                "unit": unit,
                "reference_period": rec.get("reference_period_desc"),
                "short_desc": rec.get("short_desc"),
                "source": rec.get("source_desc"),
            })

        if not parsed:
            return None

        # 2. Group rows into series by short_desc.
        series: Dict[str, List[Dict[str, Any]]] = {}
        for row in parsed:
            series.setdefault(row["short_desc"] or "", []).append(row)

        # 3. Choose the headline series: the preferred one if present, else the series with
        #    the most recent data, tie-broken by longest history (most records).
        chosen_key = None
        if spec.preferred_series and spec.preferred_series in series:
            chosen_key = spec.preferred_series
        else:
            chosen_key = max(
                series,
                key=lambda k: (max(r["year"] for r in series[k]), len(series[k])),
            )
        chosen = series[chosen_key]

        # 4. Newest first; keep only the trailing N.
        chosen.sort(key=lambda p: p["year"], reverse=True)
        periods = chosen[: self.trailing_periods]

        return CommodityYield(
            commodity=spec.commodity_desc,
            label=spec.label,
            statistic=spec.statisticcat_desc,
            unit=periods[0].get("unit"),
            series=chosen_key or None,
            periods=periods,
        )

    def get_county_yields(self, state_fips: str, county_code: str,
                          commodities: Optional[List[str]] = None) -> List[CommodityYield]:
        """Fetch trailing yields for all (or a subset of) configured commodities."""
        keys = commodities or list(COMMODITY_SPECS.keys())
        results = []
        for key in keys:
            spec = COMMODITY_SPECS.get(key.upper())
            if not spec:
                continue
            cy = self.get_county_commodity(spec, state_fips, county_code)
            if cy is not None:  # null-safe: omit commodities with no county data
                results.append(cy)
        return results


class USDACropYieldsIntegration:
    """Integration layer between the USDA NASS API and Snowflake."""

    def __init__(self, environment: str = None,
                 trailing_periods: int = DEFAULT_TRAILING_PERIODS):
        self.environment = environment or os.environ.get("ENVIRONMENT", "dev")
        self.trailing_periods = trailing_periods
        self.api_client = USDANassClient(
            api_key=get_usda_api_key(self.environment),
            trailing_periods=trailing_periods,
        )

    def get_parcel_location(self, parcel_id: str) -> Optional[Dict[str, Any]]:
        """Look up county FIPS + state for a parcel from CURATED.PARCEL_PROFILES."""
        try:
            with get_snowflake_connector(self.environment) as sf:
                # CURATED.PARCEL_PROFILES exposes lowercase `fips_code` and `state`
                # (see dbt/models/marts/parcel_profiles.sql); Snowflake returns the
                # column names upper-cased via DictCursor.
                rows = sf.execute_query(
                    """
                    SELECT PARCEL_ID, FIPS_CODE, STATE
                    FROM TEDDY_DATA.CURATED.PARCEL_PROFILES
                    WHERE PARCEL_ID = %s
                    """,
                    {"1": parcel_id},
                )
                if not rows:
                    logger.warning(f"No parcel found with ID: {parcel_id}")
                    return None
                row = rows[0]
                return {
                    "parcel_id": row.get("PARCEL_ID"),
                    "county_fips": row.get("FIPS_CODE"),
                    "state_code": row.get("STATE"),
                }
        except Exception as e:
            logger.error(f"Error getting parcel location for {parcel_id}: {e}")
            return None

    def check_existing_yields(self, parcel_id: str, max_age_days: int = 365) -> Optional[List[Dict]]:
        """Return stored yields for a parcel if they are recent enough, else None."""
        try:
            with get_snowflake_connector(self.environment) as sf:
                cutoff = datetime.now() - timedelta(days=max_age_days)
                rows = sf.execute_query(
                    """
                    SELECT COMMODITY_DESC, STATISTIC_CATEGORY, UNIT_DESC,
                           YEAR, VALUE, REFERENCE_PERIOD, LAST_PROCESSED_AT
                    FROM TEDDY_DATA.RAW.USDA_CROP_YIELDS
                    WHERE PARCEL_ID = %s AND LAST_PROCESSED_AT > %s
                    ORDER BY COMMODITY_DESC, YEAR DESC
                    """,
                    {"1": parcel_id, "2": cutoff.isoformat()},
                )
                return rows or None
        except Exception as e:
            logger.error(f"Error checking existing USDA yields for {parcel_id}: {e}")
            return None

    def process_parcel_yields(self, parcel_id: str, county_fips: Optional[str] = None,
                              state_code: Optional[str] = None,
                              force_refresh: bool = False) -> Dict[str, Any]:
        """
        Fetch + store trailing county yields for a parcel. Idempotent: re-running replaces
        the parcel's rows. Null-safe end to end — a parcel with no resolvable county, or a
        county with no commodity data, returns success with an empty commodity set rather
        than raising, so Teddy never shows a half-empty stat.
        """
        try:
            if not force_refresh:
                existing = self.check_existing_yields(parcel_id)
                if existing:
                    logger.info(f"Using existing USDA yields for parcel {parcel_id}")
                    return {"success": True, "parcel_id": parcel_id, "source": "cache",
                            "data": self.get_parcel_yields_json(parcel_id)}

            # Resolve county/state if not supplied by the orchestrator payload.
            if not county_fips or not state_code:
                location = self.get_parcel_location(parcel_id)
                if location:
                    county_fips = county_fips or location.get("county_fips")
                    state_code = state_code or location.get("state_code")

            state_fips, county_code = USDANassClient.split_fips(county_fips)
            if not state_fips or not county_code:
                logger.info(f"No usable county FIPS for parcel {parcel_id} "
                            f"(county_fips={county_fips}); storing empty result")
                self._store_yields(parcel_id, county_fips, state_code, [])
                return {"success": True, "parcel_id": parcel_id, "source": "api",
                        "commodities": 0, "reason": "no_county_fips"}

            yields = self.api_client.get_county_yields(state_fips, county_code)
            self._store_yields(parcel_id, county_fips, state_code, yields)

            return {
                "success": True,
                "parcel_id": parcel_id,
                "source": "api",
                "county_fips": county_fips,
                "commodities": len(yields),
                "data": self.get_parcel_yields_json(parcel_id),
            }
        except Exception as e:
            logger.error(f"Error processing USDA yields for {parcel_id}: {e}")
            return {"success": False, "parcel_id": parcel_id, "error": str(e)}

    def _store_yields(self, parcel_id: str, county_fips: Optional[str],
                      state_code: Optional[str], yields: List[CommodityYield]):
        """Replace this parcel's USDA yield rows in Snowflake (one row per period)."""
        try:
            with get_snowflake_connector(self.environment) as sf:
                sf.execute_non_query(
                    "DELETE FROM TEDDY_DATA.RAW.USDA_CROP_YIELDS WHERE PARCEL_ID = %s",
                    {"1": parcel_id},
                )
                for cy in yields:
                    for period in cy.periods:
                        sf.execute_non_query(
                            """
                            INSERT INTO TEDDY_DATA.RAW.USDA_CROP_YIELDS (
                                PARCEL_ID, COUNTY_FIPS, STATE_CODE, COMMODITY_DESC,
                                STATISTIC_CATEGORY, SHORT_DESC, YEAR, REFERENCE_PERIOD,
                                VALUE, UNIT_DESC, SOURCE_DESC, DATA_SOURCE, API_RESPONSE_JSON
                            )
                            SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, PARSE_JSON(%s)
                            """,
                            {
                                "1": parcel_id,
                                "2": county_fips,
                                "3": state_code,
                                "4": cy.commodity,
                                "5": cy.statistic,
                                "6": period.get("short_desc"),
                                "7": period.get("year"),
                                "8": period.get("reference_period"),
                                "9": period.get("value"),
                                "10": period.get("unit"),
                                "11": period.get("source"),
                                "12": DATA_SOURCE,
                                "13": json.dumps(period, default=str),
                            },
                        )
                logger.info(f"Stored USDA yields for parcel {parcel_id}: "
                            f"{len(yields)} commodities")
        except Exception as e:
            logger.error(f"Error storing USDA yields for {parcel_id}: {e}")
            raise

    def get_parcel_yields_json(self, parcel_id: str) -> Optional[Dict[str, Any]]:
        """
        Return a Teddy-ready JSON summary for a parcel, or None if nothing is stored.
        Shape:
            {
              "parcel_id": "...", "county_fips": "48463", "state": "TX",
              "commodities": {
                "CORN": {"label": "corn", "statistic": "YIELD", "unit": "BU / ACRE",
                         "latest": {"year": 2024, "value": 142.0},
                         "periods": [ ... up to 3, newest first ... ]}
              },
              "generated_at": "..."
            }
        """
        try:
            with get_snowflake_connector(self.environment) as sf:
                rows = sf.execute_query(
                    """
                    SELECT COMMODITY_DESC, STATISTIC_CATEGORY, UNIT_DESC, YEAR, VALUE,
                           REFERENCE_PERIOD, COUNTY_FIPS, STATE_CODE
                    FROM TEDDY_DATA.RAW.USDA_CROP_YIELDS
                    WHERE PARCEL_ID = %s
                    ORDER BY COMMODITY_DESC, YEAR DESC
                    """,
                    {"1": parcel_id},
                )
                if not rows:
                    return None
                return self._rows_to_summary(parcel_id, rows)
        except Exception as e:
            logger.error(f"Error getting USDA yields JSON for {parcel_id}: {e}")
            return None

    @staticmethod
    def _rows_to_summary(parcel_id: str, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        commodities: Dict[str, Any] = {}
        county_fips = state_code = None
        for r in rows:
            county_fips = county_fips or r.get("COUNTY_FIPS")
            state_code = state_code or r.get("STATE_CODE")
            commodity = r.get("COMMODITY_DESC")
            spec = COMMODITY_SPECS.get(commodity)
            entry = commodities.setdefault(commodity, {
                "label": spec.label if spec else (commodity or "").lower(),
                "statistic": r.get("STATISTIC_CATEGORY"),
                "unit": r.get("UNIT_DESC"),
                "periods": [],
            })
            value = r.get("VALUE")
            entry["periods"].append({
                "year": r.get("YEAR"),
                "value": float(value) if value is not None else None,
                "reference_period": r.get("REFERENCE_PERIOD"),
            })
        for entry in commodities.values():
            entry["periods"].sort(key=lambda p: (p["year"] or 0), reverse=True)
            entry["latest"] = entry["periods"][0] if entry["periods"] else None
        return {
            "parcel_id": parcel_id,
            "county_fips": county_fips,
            "state": state_code,
            "commodities": commodities,
            "generated_at": datetime.now().isoformat(),
        }


def get_usda_integration(environment: str = None) -> USDACropYieldsIntegration:
    """Convenience factory for Lambda usage."""
    return USDACropYieldsIntegration(environment=environment)


if __name__ == "__main__":  # pragma: no cover - manual smoke test
    logging.basicConfig(level=logging.INFO)
    integration = get_usda_integration()
    test_parcel_id = os.environ.get("TEST_PARCEL_ID", "test_parcel_001")
    result = integration.process_parcel_yields(test_parcel_id, force_refresh=True)
    print(json.dumps(result, indent=2, default=str))
