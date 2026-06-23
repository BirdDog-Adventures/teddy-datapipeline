"""
Water Rights Parcel Processor Lambda Function (Dataset 5 — region-gated).

Looks up nearby water rights for parcels in supported states and stores a parcel-level
summary + detail in Snowflake. Triggered at claim time by the parcel enrichment orchestrator
(async invoke as 'teddy-water-rights-enrichment'); also invokable directly / via API Gateway.

Environment Variables:
- ENVIRONMENT: dev/prod (default: dev)
- WATER_RIGHTS_RADIUS_MI: search radius in miles (default: 0.25)
- FORCE_REFRESH: force refresh even if recent data exists (default: false)
"""

import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, Optional

from water_rights_client import get_water_rights_integration

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    try:
        logger.info(f"Processing water rights request: {json.dumps(event, default=str)}")
        params = parse_event_parameters(event)
        if not params:
            return _resp(400, {"error": "Invalid request parameters"})

        environment = os.environ.get("ENVIRONMENT", "dev")
        default_force_refresh = os.environ.get("FORCE_REFRESH", "false").lower() == "true"
        integration = get_water_rights_integration(environment)

        if "parcel_id" in params:
            result = integration.process_parcel_water_rights(
                parcel_id=params["parcel_id"],
                latitude=params.get("latitude"),
                longitude=params.get("longitude"),
                state_code=params.get("state_code"),
                force_refresh=params.get("force_refresh", default_force_refresh),
            )
            return _resp(200, result)

        if "parcel_ids" in params:
            results = []
            for pid in params["parcel_ids"]:
                results.append(
                    integration.process_parcel_water_rights(
                        parcel_id=pid,
                        force_refresh=params.get("force_refresh", default_force_refresh),
                    )
                )
            return _resp(200, {"results": results, "total": len(results)})

        return _resp(400, {"error": "Missing parcel_id or parcel_ids parameter"})
    except Exception as e:
        logger.error(f"Error processing water rights request: {e}", exc_info=True)
        return _resp(500, {"error": f"Internal server error: {e}"})


def parse_event_parameters(event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        if "pathParameters" in event:
            params: Dict[str, Any] = {}
            if event.get("pathParameters") and "parcel_id" in event["pathParameters"]:
                params["parcel_id"] = event["pathParameters"]["parcel_id"]
            query = event.get("queryStringParameters") or {}
            for k in ("state_code",):
                if k in query:
                    params[k] = query[k]
            for k in ("latitude", "longitude"):
                if k in query:
                    params[k] = float(query[k])
            if "force_refresh" in query:
                params["force_refresh"] = query["force_refresh"].lower() == "true"
            return params or None

        if "parcel_id" in event or "parcel_ids" in event:
            params = {}
            for k in ("parcel_id", "parcel_ids", "state_code"):
                if k in event:
                    params[k] = event[k]
            for k in ("latitude", "longitude"):
                if k in event and event[k] is not None:
                    params[k] = float(event[k])
            if "force_refresh" in event:
                params["force_refresh"] = bool(event["force_refresh"])
            return params

        if "Records" in event:
            for record in event["Records"]:
                if "body" in record:
                    return parse_event_parameters(json.loads(record["body"]))
                if "Sns" in record and "Message" in record["Sns"]:
                    return parse_event_parameters(json.loads(record["Sns"]["Message"]))

        if "Detail" in event:
            detail = event.get("Detail", {})
            if isinstance(detail, str):
                detail = json.loads(detail)
            if detail.get("parcel_id"):
                return parse_event_parameters(detail)

        return None
    except Exception as e:
        logger.error(f"Error parsing event parameters: {e}")
        return None


def _resp(status: int, body: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "statusCode": status,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type",
            "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
        },
        "body": json.dumps(body, default=str),
    }


def health_check() -> Dict[str, Any]:
    try:
        environment = os.environ.get("ENVIRONMENT", "dev")
        get_water_rights_integration(environment).get_parcel_water_rights_json("health_check")
        return {"status": "healthy", "environment": environment,
                "timestamp": str(datetime.now())}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e), "timestamp": str(datetime.now())}


if __name__ == "__main__":  # pragma: no cover
    class MockContext:
        function_name = "water-rights-parcel-processor"
        aws_request_id = "test-request-id"

    test_event = {"parcel_id": os.environ.get("TEST_PARCEL_ID", "test_parcel_001"),
                  "force_refresh": True}
    print(json.dumps(lambda_handler(test_event, MockContext()), indent=2, default=str))
