"""
Standalone unit tests for the Water Rights client (Dataset 5).

Exercises the pure logic — state normalization, ISO date parsing, the Colorado response
parser, rights summarization (most-senior priority date), and the state-allowlist gate —
without AWS/Snowflake. Heavy infra deps are stubbed in sys.modules.

Run:  python scripts/test_water_rights_client.py
"""

import os
import sys
import types

# --- Stub infra deps before importing the client ---
_requests = types.ModuleType("requests")
class _Session:
    def __init__(self):
        self.headers = types.SimpleNamespace(update=lambda *a, **k: None)
    def get(self, *a, **k):
        raise NotImplementedError
_requests.Session = _Session
_exc = types.ModuleType("requests.exceptions")
_exc.RequestException = Exception
_requests.exceptions = _exc
sys.modules["requests"] = _requests
sys.modules["requests.exceptions"] = _exc

for p in ("shared", "shared.utils", "shared.utils.snowflake_connector"):
    sys.modules[p] = types.ModuleType(p)
sys.modules["shared"].__path__ = []
sys.modules["shared.utils"].__path__ = []
sys.modules["shared.utils.snowflake_connector"].get_snowflake_connector = (
    lambda environment=None: None
)

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(_HERE), "processors", "water_rights"))
import water_rights_client as wr  # noqa: E402


class FakeResp:
    def __init__(self, status, payload=None):
        self.status_code = status
        self._payload = payload
    def json(self):
        if self._payload is None:
            raise ValueError("no json")
        return self._payload


class FakeSession:
    def __init__(self, router):
        self.headers = types.SimpleNamespace(update=lambda *a, **k: None)
        self.router = router
    def get(self, url, params=None, timeout=None):
        return self.router(params)


_FAILURES = []
def check(name, cond):
    print(("PASS" if cond else "FAIL") + f"  {name}")
    if not cond:
        _FAILURES.append(name)


def test_normalize_state():
    check("normalize ' co ' -> CO", wr.normalize_state(" co ") == "CO")
    check("normalize 'Texas' (len!=2) -> None", wr.normalize_state("Texas") is None)
    check("normalize None -> None", wr.normalize_state(None) is None)


def test_parse_iso_date():
    check("parse full iso -> date", wr.parse_iso_date("1920-05-31T00:00:00-06:00") == "1920-05-31")
    check("parse None -> None", wr.parse_iso_date(None) is None)
    check("parse junk -> None", wr.parse_iso_date("not-a-date") is None)


def test_co_parse_and_summary():
    c = wr.ColoradoDWRClient(rate_limit_delay=0)
    payload = {"ResultList": [
        {"structureName": "BAAB WELL", "waterSource": "GROUNDWATER",
         "appropriationDate": "1920-05-31T00:00:00-06:00",
         "adjudicationDate": "1971-12-31T00:00:00-07:00",
         "adminNumber": "12345.00000", "netAbsolute": 1.33, "wdid": "0100",
         "division": 1, "waterDistrict": 1, "county": "WELD",
         "latitude": 40.42, "longitude": -104.66},
        {"structureName": "NEWER DITCH", "waterSource": "SURFACE",
         "appropriationDate": "1985-01-01T00:00:00-07:00", "netAbsolute": 2.0},
    ]}
    c.session = FakeSession(lambda params: FakeResp(200, payload))
    rights = c.get_water_rights(40.42, -104.66, 0.25)
    check("CO parse returns 2 rights", len(rights) == 2)
    check("CO parse priority date", rights[0]["appropriation_date"] == "1920-05-31")
    summary = wr.summarize_rights(rights)
    check("summary count 2", summary["rights_count"] == 2)
    check("most senior is earliest (1920)", summary["most_senior_priority_date"] == "1920-05-31")
    check("most senior structure", summary["most_senior_structure"] == "BAAB WELL")


def test_co_empty_is_null_safe():
    c = wr.ColoradoDWRClient(rate_limit_delay=0)
    c.session = FakeSession(lambda params: FakeResp(200, {"ResultList": []}))
    check("empty -> []", c.get_water_rights(0, 0, 0.25) == [])
    c.session = FakeSession(lambda params: FakeResp(500, None))
    check("HTTP 500 -> [] (null-safe)", c.get_water_rights(0, 0, 0.25) == [])


def test_state_gate():
    check("CO supported", "CO" in wr.SUPPORTED_STATES)
    check("TX not supported", "TX" not in wr.SUPPORTED_STATES)
    # Integration-level gate without touching Snowflake: stub the store + location.
    integ = wr.WaterRightsIntegration(environment="test")
    integ._store_status = lambda *a, **k: None
    integ._store_details = lambda *a, **k: None
    integ.get_parcel_water_rights_json = lambda pid: None
    integ.get_parcel_location = lambda pid: None
    res = integ.process_parcel_water_rights("p1", latitude=1, longitude=1, state_code="TX")
    check("unsupported state -> available False, reason state_not_supported",
          res.get("available") is False and res.get("reason") == "state_not_supported")
    res2 = integ.process_parcel_water_rights("p2", state_code="CO")  # no coords
    check("supported state but no coords -> reason no_coordinates",
          res2.get("available") is False and res2.get("reason") == "no_coordinates")


def main():
    test_normalize_state()
    test_parse_iso_date()
    test_co_parse_and_summary()
    test_co_empty_is_null_safe()
    test_state_gate()
    print()
    if _FAILURES:
        print(f"{len(_FAILURES)} FAILED: {_FAILURES}")
        sys.exit(1)
    print("ALL TESTS PASSED")


if __name__ == "__main__":
    main()
