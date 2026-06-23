"""
Water Rights Client for Teddy Data Pipeline (Dataset 5 — region-gated).

Region-gated, claim-time enrichment that, for parcels in supported states, looks up nearby
water rights from that state's public source and stores a parcel-level summary + detail in
Snowflake. Everywhere else it returns a definitive "not available in this state" — never an
error or a misleading blank.

Proven first with ONE clean source — Colorado DWR (CDSS) REST API:
    GET https://dwr.state.co.us/Rest/GET/api/v2/waterrights/netamount
        ?format=json&latitude=..&longitude=..&radius=..&units=miles
    - Free, public, NO API key required.
    - Spatial radius search; returns appropriationDate (priority), adminNumber (seniority),
      netAbsolute/netConditional (decreed amounts), structureName, waterSource, lat/long
      (point of diversion), wdid, division, waterDistrict, county.

Adding another state = implement a source adapter with the same `get_water_rights(lat, lon,
radius)` shape and register it in SOURCE_REGISTRY. The allowlist (SUPPORTED_STATES) is derived
from the registry, so unsupported states are handled gracefully by construction.

Inputs (from the parcel record / orchestrator payload): latitude, longitude, state_code.
Environment Variables:
    ENVIRONMENT             dev/prod (default: dev)
    WATER_RIGHTS_RADIUS_MI  search radius in miles (default: 0.25)
"""

import json
import logging
import os
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import requests

import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared.utils.snowflake_connector import get_snowflake_connector

logger = logging.getLogger(__name__)

DEFAULT_RADIUS_MILES = float(os.environ.get("WATER_RIGHTS_RADIUS_MI", "0.25"))
DATA_SOURCE_CO = "co_dwr_cdss"


def normalize_state(state: Optional[str]) -> Optional[str]:
    if not state:
        return None
    s = str(state).strip().upper()
    return s if len(s) == 2 else None


def parse_iso_date(value: Optional[str]) -> Optional[str]:
    """Return the YYYY-MM-DD portion of an ISO datetime, or None."""
    if not value or not isinstance(value, str) or len(value) < 10:
        return None
    candidate = value[:10]
    try:
        datetime.strptime(candidate, "%Y-%m-%d")
        return candidate
    except ValueError:
        return None


class ColoradoDWRClient:
    """Adapter for the Colorado DWR (CDSS) water rights net-amounts endpoint."""

    BASE_URL = "https://dwr.state.co.us/Rest/GET/api/v2/waterrights/netamount"
    state = "CO"
    data_source = DATA_SOURCE_CO

    def __init__(self, rate_limit_delay: float = 0.5):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Teddy-DataPipeline/1.0"})
        self.rate_limit_delay = rate_limit_delay
        self.last_request_time = 0.0

    def _rate_limit(self):
        elapsed = time.time() - self.last_request_time
        if elapsed < self.rate_limit_delay:
            time.sleep(self.rate_limit_delay - elapsed)
        self.last_request_time = time.time()

    def get_water_rights(
        self, latitude: float, longitude: float, radius_miles: float
    ) -> List[Dict[str, Any]]:
        """Return parsed water-rights records near a point. Null-safe (empty on error)."""
        params = {
            "format": "json",
            "latitude": latitude,
            "longitude": longitude,
            "radius": radius_miles,
            "units": "miles",
        }
        self._rate_limit()
        try:
            resp = self.session.get(self.BASE_URL, params=params, timeout=30)
        except requests.exceptions.RequestException as e:
            logger.warning(f"CO DWR request failed at {latitude},{longitude}: {e}")
            return []
        if resp.status_code != 200:
            logger.warning(f"CO DWR returned {resp.status_code}; treating as no data")
            return []
        try:
            records = resp.json().get("ResultList", []) or []
        except ValueError:
            logger.warning("CO DWR returned non-JSON; treating as no data")
            return []
        return [self._parse(r) for r in records]

    @staticmethod
    def _parse(r: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "structure_name": r.get("structureName"),
            "structure_type": r.get("structureType"),
            "water_source": r.get("waterSource"),
            "appropriation_date": parse_iso_date(r.get("appropriationDate")),
            "adjudication_date": parse_iso_date(r.get("adjudicationDate")),
            "admin_number": r.get("adminNumber"),
            "priority_number": r.get("priorityNumber"),
            "net_absolute": r.get("netAbsolute"),
            "net_conditional": r.get("netConditional"),
            "decreed_units": r.get("decreedUnits"),
            "decreed_uses": r.get("decreedUses"),
            "latitude": r.get("latitude"),
            "longitude": r.get("longitude"),
            "wdid": r.get("wdid"),
            "division": r.get("division"),
            "water_district": r.get("waterDistrict"),
            "county": r.get("county"),
        }


# state -> source adapter class. Add states here; the allowlist follows automatically.
SOURCE_REGISTRY = {
    "CO": ColoradoDWRClient,
}
SUPPORTED_STATES = set(SOURCE_REGISTRY.keys())


def summarize_rights(rights: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Pure summary of a parcel's water rights for Teddy: count, most-senior priority date."""
    dated = [r for r in rights if r.get("appropriation_date")]
    most_senior = min(dated, key=lambda r: r["appropriation_date"], default=None)
    sources = sorted({r.get("water_source") for r in rights if r.get("water_source")})
    return {
        "rights_count": len(rights),
        "most_senior_priority_date": most_senior["appropriation_date"] if most_senior else None,
        "most_senior_structure": most_senior["structure_name"] if most_senior else None,
        "water_sources": sources,
    }


class WaterRightsIntegration:
    """Integration layer between state water-rights sources and Snowflake."""

    def __init__(self, environment: str = None, radius_miles: float = DEFAULT_RADIUS_MILES):
        self.environment = environment or os.environ.get("ENVIRONMENT", "dev")
        self.radius_miles = radius_miles

    def get_parcel_location(self, parcel_id: str) -> Optional[Dict[str, Any]]:
        try:
            with get_snowflake_connector(self.environment) as sf:
                rows = sf.execute_query(
                    """
                    SELECT PARCEL_ID, LATITUDE, LONGITUDE, STATE
                    FROM TEDDY_DATA.CURATED.PARCEL_PROFILES
                    WHERE PARCEL_ID = %s
                    """,
                    {"1": parcel_id},
                )
                if not rows:
                    return None
                row = rows[0]
                return {
                    "latitude": float(row["LATITUDE"]) if row.get("LATITUDE") else None,
                    "longitude": float(row["LONGITUDE"]) if row.get("LONGITUDE") else None,
                    "state_code": row.get("STATE"),
                }
        except Exception as e:
            logger.error(f"Error getting parcel location for {parcel_id}: {e}")
            return None

    def process_parcel_water_rights(
        self,
        parcel_id: str,
        latitude: Optional[float] = None,
        longitude: Optional[float] = None,
        state_code: Optional[str] = None,
        force_refresh: bool = False,
    ) -> Dict[str, Any]:
        """Region-gated lookup + store. Always null-safe; returns a status dict."""
        try:
            if not force_refresh:
                existing = self.get_parcel_water_rights_json(parcel_id)
                if existing:
                    return {"success": True, "parcel_id": parcel_id,
                            "source": "cache", "data": existing}

            if latitude is None or longitude is None or not state_code:
                loc = self.get_parcel_location(parcel_id)
                if loc:
                    latitude = latitude if latitude is not None else loc.get("latitude")
                    longitude = longitude if longitude is not None else loc.get("longitude")
                    state_code = state_code or loc.get("state_code")

            state = normalize_state(state_code)

            # Gate 1: state not supported -> graceful "not available in this state".
            if state not in SUPPORTED_STATES:
                self._store_status(parcel_id, state, available=False,
                                   reason="state_not_supported", summary=None)
                return {"success": True, "parcel_id": parcel_id, "available": False,
                        "reason": "state_not_supported", "state": state}

            # Gate 2: no coordinates -> can't query.
            if latitude is None or longitude is None:
                self._store_status(parcel_id, state, available=False,
                                   reason="no_coordinates", summary=None)
                return {"success": True, "parcel_id": parcel_id, "available": False,
                        "reason": "no_coordinates", "state": state}

            client = SOURCE_REGISTRY[state]()
            rights = client.get_water_rights(latitude, longitude, self.radius_miles)
            summary = summarize_rights(rights)
            available = summary["rights_count"] > 0
            reason = "ok" if available else "no_rights_found"

            self._store_status(parcel_id, state, available=available,
                               reason=reason, summary=summary)
            self._store_details(parcel_id, state, client.data_source, rights)

            return {"success": True, "parcel_id": parcel_id, "available": available,
                    "reason": reason, "state": state,
                    "data": self.get_parcel_water_rights_json(parcel_id)}
        except Exception as e:
            logger.error(f"Error processing water rights for {parcel_id}: {e}")
            return {"success": False, "parcel_id": parcel_id, "error": str(e)}

    def _store_status(self, parcel_id, state, available, reason, summary):
        try:
            with get_snowflake_connector(self.environment) as sf:
                sf.execute_non_query(
                    "DELETE FROM TEDDY_DATA.RAW.WATER_RIGHTS_STATUS WHERE PARCEL_ID = %s",
                    {"1": parcel_id},
                )
                sf.execute_non_query(
                    """
                    INSERT INTO TEDDY_DATA.RAW.WATER_RIGHTS_STATUS (
                        PARCEL_ID, STATE_CODE, AVAILABLE, REASON, RIGHTS_COUNT,
                        MOST_SENIOR_PRIORITY_DATE, MOST_SENIOR_STRUCTURE
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    {
                        "1": parcel_id, "2": state, "3": available, "4": reason,
                        "5": summary["rights_count"] if summary else 0,
                        "6": summary["most_senior_priority_date"] if summary else None,
                        "7": summary["most_senior_structure"] if summary else None,
                    },
                )
        except Exception as e:
            logger.error(f"Error storing water rights status for {parcel_id}: {e}")
            raise

    def _store_details(self, parcel_id, state, data_source, rights):
        if not rights:
            return
        try:
            with get_snowflake_connector(self.environment) as sf:
                sf.execute_non_query(
                    "DELETE FROM TEDDY_DATA.RAW.WATER_RIGHTS WHERE PARCEL_ID = %s",
                    {"1": parcel_id},
                )
                for r in rights:
                    sf.execute_non_query(
                        """
                        INSERT INTO TEDDY_DATA.RAW.WATER_RIGHTS (
                            PARCEL_ID, STATE_CODE, STRUCTURE_NAME, STRUCTURE_TYPE, WATER_SOURCE,
                            APPROPRIATION_DATE, ADJUDICATION_DATE, ADMIN_NUMBER, PRIORITY_NUMBER,
                            NET_ABSOLUTE, NET_CONDITIONAL, DECREED_USES, LATITUDE, LONGITUDE,
                            WDID, DIVISION, WATER_DISTRICT, COUNTY, DATA_SOURCE, API_RESPONSE_JSON
                        )
                        SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                               %s, %s, PARSE_JSON(%s)
                        """,
                        {
                            "1": parcel_id, "2": state,
                            "3": r.get("structure_name"), "4": r.get("structure_type"),
                            "5": r.get("water_source"), "6": r.get("appropriation_date"),
                            "7": r.get("adjudication_date"), "8": r.get("admin_number"),
                            "9": r.get("priority_number"), "10": r.get("net_absolute"),
                            "11": r.get("net_conditional"), "12": r.get("decreed_uses"),
                            "13": r.get("latitude"), "14": r.get("longitude"),
                            "15": r.get("wdid"), "16": r.get("division"),
                            "17": r.get("water_district"), "18": r.get("county"),
                            "19": data_source, "20": json.dumps(r, default=str),
                        },
                    )
        except Exception as e:
            logger.error(f"Error storing water rights detail for {parcel_id}: {e}")
            raise

    def get_parcel_water_rights_json(self, parcel_id: str) -> Optional[Dict[str, Any]]:
        """Teddy-ready summary, or None if the parcel was never processed."""
        try:
            with get_snowflake_connector(self.environment) as sf:
                rows = sf.execute_query(
                    """
                    SELECT STATE_CODE, AVAILABLE, REASON, RIGHTS_COUNT,
                           MOST_SENIOR_PRIORITY_DATE, MOST_SENIOR_STRUCTURE
                    FROM TEDDY_DATA.RAW.WATER_RIGHTS_STATUS
                    WHERE PARCEL_ID = %s
                    """,
                    {"1": parcel_id},
                )
                if not rows:
                    return None
                s = rows[0]
                return {
                    "parcel_id": parcel_id,
                    "state": s.get("STATE_CODE"),
                    "available": bool(s.get("AVAILABLE")),
                    "reason": s.get("REASON"),
                    "rights_count": s.get("RIGHTS_COUNT"),
                    "priority_date": s.get("MOST_SENIOR_PRIORITY_DATE"),
                    "most_senior_structure": s.get("MOST_SENIOR_STRUCTURE"),
                }
        except Exception as e:
            logger.error(f"Error getting water rights JSON for {parcel_id}: {e}")
            return None


def get_water_rights_integration(environment: str = None) -> WaterRightsIntegration:
    return WaterRightsIntegration(environment=environment)


if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)
    integ = get_water_rights_integration()
    print(json.dumps(
        integ.process_parcel_water_rights(
            os.environ.get("TEST_PARCEL_ID", "test_parcel_001"), force_refresh=True),
        indent=2, default=str))
