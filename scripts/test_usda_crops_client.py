"""
Standalone unit tests for the USDA Crop Yields client (Dataset 1).

Exercises the pure logic — FIPS derivation, NASS value parsing, trailing-period
reduction, null-safety on empty/400 responses, and the Teddy JSON summary shape —
without requiring AWS, Snowflake, or network access. Heavy infra dependencies
(requests / boto3 / the Snowflake connector) are stubbed in sys.modules so the
module imports cleanly anywhere.

Run:  python scripts/test_usda_crops_client.py
"""

import os
import sys
import types

# --- Stub infra dependencies before importing the client ---------------------
_requests = types.ModuleType("requests")
class _Session:
    def __init__(self):
        self.headers = types.SimpleNamespace(update=lambda *a, **k: None)
    def get(self, *a, **k):  # overridden per-test via instance.session
        raise NotImplementedError
_requests.Session = _Session
_requests_exc = types.ModuleType("requests.exceptions")
_requests_exc.RequestException = Exception
_requests.exceptions = _requests_exc
sys.modules["requests"] = _requests
sys.modules["requests.exceptions"] = _requests_exc

_boto3 = types.ModuleType("boto3")
def _no_aws(*a, **k):
    raise RuntimeError("boto3 unavailable in test")
_boto3.client = _no_aws
sys.modules["boto3"] = _boto3

_shared = types.ModuleType("shared"); _shared.__path__ = []
_shared_utils = types.ModuleType("shared.utils"); _shared_utils.__path__ = []
_sfc = types.ModuleType("shared.utils.snowflake_connector")
def _no_snowflake(environment=None):
    raise RuntimeError("snowflake unavailable in test")
_sfc.get_snowflake_connector = _no_snowflake
sys.modules["shared"] = _shared
sys.modules["shared.utils"] = _shared_utils
sys.modules["shared.utils.snowflake_connector"] = _sfc

# --- Import the module under test --------------------------------------------
_HERE = os.path.dirname(os.path.abspath(__file__))
_CLIENT_DIR = os.path.join(os.path.dirname(_HERE), "processors", "usda_crops")
sys.path.insert(0, _CLIENT_DIR)
import usda_crops_client as uc  # noqa: E402


# --- Minimal fake HTTP response/session --------------------------------------
class FakeResp:
    def __init__(self, status, payload=None):
        self.status_code = status
        self._payload = payload
    def json(self):
        if self._payload is None:
            raise ValueError("no json")
        return self._payload
    def raise_for_status(self):
        if self.status_code >= 400:
            raise Exception(f"HTTP {self.status_code}")


class FakeSession:
    def __init__(self, router):
        self.headers = types.SimpleNamespace(update=lambda *a, **k: None)
        self.router = router
    def get(self, url, params=None, timeout=None):
        return self.router(params)


# --- Tiny assertion harness ---------------------------------------------------
_FAILURES = []
def check(name, cond):
    print(("PASS" if cond else "FAIL") + f"  {name}")
    if not cond:
        _FAILURES.append(name)


def make_client():
    c = uc.USDANassClient(api_key="test-key", trailing_periods=3)
    c.rate_limit_delay = 0  # no sleeping in tests
    return c


def test_parse_value():
    pv = uc.USDANassClient._parse_value
    check("parse '12,345' -> 12345.0", pv("12,345") == 12345.0)
    check("parse '142.5' -> 142.5", pv("142.5") == 142.5)
    check("parse '  88 ' -> 88.0", pv("  88 ") == 88.0)
    check("parse '(D)' -> None", pv("(D)") is None)
    check("parse '' -> None", pv("") is None)
    check("parse None -> None", pv(None) is None)
    check("parse 'abc' -> None", pv("abc") is None)


def test_split_fips():
    sf = uc.USDANassClient.split_fips
    check("split '48463' -> ('48','463')", sf("48463") == ("48", "463"))
    check("split explicit overrides", sf(None, state_fips="6", county_code="1") == ("06", "001"))
    check("split 4-digit '6001' -> ('06','001')", sf("6001") == ("06", "001"))
    check("split None -> (None,None)", sf(None) == (None, None))
    check("split 'abc' -> (None,None)", sf("abc") == (None, None))


def test_build_commodity_yield_trailing_and_suppressed():
    c = make_client()
    spec = uc.COMMODITY_SPECS["CORN"]
    records = [
        {"year": "2020", "Value": "120", "unit_desc": "BU / ACRE",
         "reference_period_desc": "YEAR", "short_desc": "CORN", "source_desc": "SURVEY"},
        {"year": "2021", "Value": "(D)", "unit_desc": "BU / ACRE",
         "reference_period_desc": "YEAR", "short_desc": "CORN", "source_desc": "SURVEY"},
        {"year": "2022", "Value": "130", "unit_desc": "BU / ACRE",
         "reference_period_desc": "YEAR", "short_desc": "CORN", "source_desc": "SURVEY"},
        {"year": "2023", "Value": "140", "unit_desc": "BU / ACRE",
         "reference_period_desc": "YEAR", "short_desc": "CORN", "source_desc": "SURVEY"},
        {"year": "2024", "Value": "1,250", "unit_desc": "BU / ACRE",
         "reference_period_desc": "YEAR", "short_desc": "CORN", "source_desc": "SURVEY"},
    ]
    cy = c._build_commodity_yield(spec, records)
    check("keeps trailing 3 periods", cy is not None and len(cy.periods) == 3)
    check("newest period first (2024)", cy.periods[0]["year"] == 2024)
    check("parses comma value 1250.0", cy.periods[0]["value"] == 1250.0)
    years = [p["year"] for p in cy.periods]
    check("suppressed 2021 omitted", 2021 not in years)
    check("unit taken from newest", cy.unit == "BU / ACRE")
    check("label set from spec", cy.label == "corn")


def test_series_selection_cattle_prefers_headline_and_excludes_operations():
    """Cattle returns many sub-series; we must pick HEAD 'INCL CALVES' inventory, never
    a farm-count OPERATIONS series or a sub-class."""
    c = make_client()
    spec = uc.COMMODITY_SPECS["CATTLE"]
    records = [
        # farm counts — must be excluded by unit_contains='HEAD'
        {"year": "2022", "Value": "40", "unit_desc": "OPERATIONS",
         "short_desc": "CATTLE, INCL CALVES - OPERATIONS WITH INVENTORY", "source_desc": "CENSUS"},
        # sub-class HEAD series — should not win over the preferred headline
        {"year": "2022", "Value": "205", "unit_desc": "HEAD",
         "short_desc": "CATTLE, COWS, BEEF - INVENTORY", "source_desc": "CENSUS"},
        # the headline series we want
        {"year": "2022", "Value": "796", "unit_desc": "HEAD",
         "short_desc": "CATTLE, INCL CALVES - INVENTORY", "source_desc": "CENSUS"},
        {"year": "2017", "Value": "812", "unit_desc": "HEAD",
         "short_desc": "CATTLE, INCL CALVES - INVENTORY", "source_desc": "CENSUS"},
    ]
    cy = c._build_commodity_yield(spec, records)
    check("cattle picks INCL CALVES inventory", cy and cy.series == "CATTLE, INCL CALVES - INVENTORY")
    check("cattle unit is HEAD", cy and cy.unit == "HEAD")
    check("cattle latest value 796", cy and cy.periods[0]["value"] == 796.0)
    units = {p["unit"] for p in cy.periods}
    check("no OPERATIONS rows leak in", "OPERATIONS" not in units)


def test_series_selection_crops_picks_series_with_most_history():
    """Corn returns GRAIN (long history) plus a sparse SILAGE series in a different /ACRE
    unit; we must pick the headline GRAIN series, not mix them."""
    c = make_client()
    spec = uc.COMMODITY_SPECS["CORN"]
    records = [
        {"year": "2024", "Value": "240", "unit_desc": "BU / ACRE",
         "short_desc": "CORN, GRAIN - YIELD, MEASURED IN BU / ACRE", "source_desc": "SURVEY"},
        {"year": "2023", "Value": "230", "unit_desc": "BU / ACRE",
         "short_desc": "CORN, GRAIN - YIELD, MEASURED IN BU / ACRE", "source_desc": "SURVEY"},
        {"year": "2022", "Value": "220", "unit_desc": "BU / ACRE",
         "short_desc": "CORN, GRAIN - YIELD, MEASURED IN BU / ACRE", "source_desc": "SURVEY"},
        {"year": "2024", "Value": "22", "unit_desc": "TONS / ACRE",
         "short_desc": "CORN, SILAGE - YIELD, MEASURED IN TONS / ACRE", "source_desc": "SURVEY"},
    ]
    cy = c._build_commodity_yield(spec, records)
    check("corn picks GRAIN series", cy and "GRAIN" in cy.series)
    check("corn unit BU / ACRE", cy and cy.unit == "BU / ACRE")
    check("corn does not mix silage", all(p["unit"] == "BU / ACRE" for p in cy.periods))


def test_build_commodity_yield_all_suppressed_is_none():
    c = make_client()
    spec = uc.COMMODITY_SPECS["WHEAT"]
    records = [{"year": "2023", "Value": "(D)"}, {"year": "2022", "Value": "(NA)"}]
    check("all-suppressed -> None", c._build_commodity_yield(spec, records) is None)


def test_get_county_commodity_400_is_null_safe():
    c = make_client()
    c.session = FakeSession(lambda params: FakeResp(400, {"error": ["no records"]}))
    spec = uc.COMMODITY_SPECS["CORN"]
    check("HTTP 400 -> None (omit)", c.get_county_commodity(spec, "48", "463") is None)


def test_get_county_commodity_200():
    c = make_client()
    payload = {"data": [
        {"year": "2024", "Value": "142", "unit_desc": "BU / ACRE",
         "reference_period_desc": "YEAR", "short_desc": "CORN, GRAIN - YIELD",
         "source_desc": "SURVEY"},
    ]}
    c.session = FakeSession(lambda params: FakeResp(200, payload))
    spec = uc.COMMODITY_SPECS["CORN"]
    cy = c.get_county_commodity(spec, "48", "463")
    check("HTTP 200 -> CommodityYield", cy is not None and cy.commodity == "CORN")
    check("value parsed", cy.periods[0]["value"] == 142.0)


def test_get_county_yields_omits_missing_commodities():
    c = make_client()
    def router(params):
        if params.get("commodity_desc") == "CORN":
            return FakeResp(200, {"data": [
                {"year": "2024", "Value": "142", "unit_desc": "BU / ACRE",
                 "reference_period_desc": "YEAR", "short_desc": "CORN", "source_desc": "SURVEY"}]})
        return FakeResp(400, {"error": ["no records"]})
    c.session = FakeSession(router)
    results = c.get_county_yields("48", "463")
    check("only commodities with data returned", len(results) == 1)
    check("returned commodity is CORN", results[0].commodity == "CORN")


def test_no_api_key_returns_none():
    c = uc.USDANassClient(api_key=None)
    c.rate_limit_delay = 0
    spec = uc.COMMODITY_SPECS["CORN"]
    check("no api key -> None (omit, no raise)", c.get_county_commodity(spec, "48", "463") is None)


def test_rows_to_summary_shape():
    rows = [
        {"COMMODITY_DESC": "CORN", "STATISTIC_CATEGORY": "YIELD", "UNIT_DESC": "BU / ACRE",
         "YEAR": 2024, "VALUE": 142.0, "REFERENCE_PERIOD": "YEAR",
         "COUNTY_FIPS": "48463", "STATE_CODE": "TX"},
        {"COMMODITY_DESC": "CORN", "STATISTIC_CATEGORY": "YIELD", "UNIT_DESC": "BU / ACRE",
         "YEAR": 2023, "VALUE": 138.0, "REFERENCE_PERIOD": "YEAR",
         "COUNTY_FIPS": "48463", "STATE_CODE": "TX"},
    ]
    summary = uc.USDACropYieldsIntegration._rows_to_summary("p1", rows)
    check("summary county_fips", summary["county_fips"] == "48463")
    check("summary state", summary["state"] == "TX")
    corn = summary["commodities"].get("CORN")
    check("summary has CORN", corn is not None)
    check("summary latest year 2024", corn and corn["latest"]["year"] == 2024)
    check("summary periods newest first", corn and corn["periods"][0]["year"] == 2024)


def main():
    test_parse_value()
    test_split_fips()
    test_build_commodity_yield_trailing_and_suppressed()
    test_series_selection_cattle_prefers_headline_and_excludes_operations()
    test_series_selection_crops_picks_series_with_most_history()
    test_build_commodity_yield_all_suppressed_is_none()
    test_get_county_commodity_400_is_null_safe()
    test_get_county_commodity_200()
    test_get_county_yields_omits_missing_commodities()
    test_no_api_key_returns_none()
    test_rows_to_summary_shape()
    print()
    if _FAILURES:
        print(f"{len(_FAILURES)} FAILED: {_FAILURES}")
        sys.exit(1)
    print("ALL TESTS PASSED")


if __name__ == "__main__":
    main()
