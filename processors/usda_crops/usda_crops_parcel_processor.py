"""
USDA Crop Yields Parcel Processor Lambda Function (Dataset 1)

Fetches county-level trailing yields/inventory for a parcel's relevant commodities from
the USDA NASS QuickStats API and stores them in Snowflake. Triggered at claim time by the
parcel enrichment orchestrator (async invoke as 'teddy-usda-crop-yields-enrichment'), and
also invokable directly / via API Gateway for backfills.

Environment Variables:
- ENVIRONMENT: dev/prod (default: dev)
- USDA_YIELD_PERIODS: trailing reporting periods to keep (default: 3)
- FORCE_REFRESH: force refresh even if recent data exists (default: false)
"""

import json
import logging
import os
from datetime import datetime
from typing import Dict, Any, Optional

from usda_crops_client import get_usda_integration

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Expected event formats (mirrors the NLCD processor):

    1. Orchestrator / direct invocation:
       {"parcel_id": "123", "county_fips": "48463", "state_code": "TX", "force_refresh": false}

    2. Batch:
       {"parcel_ids": ["123", "456"], "force_refresh": false}

    3. API Gateway:
       {"pathParameters": {"parcel_id": "123"},
        "queryStringParameters": {"force_refresh": "false"}}

    4. SNS / EventBridge wrappers (Records[*].body / Records[*].Sns.Message / Detail).
    """
    try:
        logger.info(f"Processing USDA crop yields request: {json.dumps(event, default=str)}")

        params = parse_event_parameters(event)
        if not params:
            return create_error_response(400, "Invalid request parameters")

        environment = os.environ.get("ENVIRONMENT", "dev")
        default_force_refresh = os.environ.get("FORCE_REFRESH", "false").lower() == "true"

        integration = get_usda_integration(environment)

        if "parcel_id" in params:
            result = process_single_parcel(
                integration,
                params["parcel_id"],
                params.get("county_fips"),
                params.get("state_code"),
                params.get("force_refresh", default_force_refresh),
            )
            return create_success_response(result)

        if "parcel_ids" in params:
            results = process_parcel_batch(
                integration,
                params["parcel_ids"],
                params.get("force_refresh", default_force_refresh),
            )
            return create_success_response(results)

        return create_error_response(400, "Missing parcel_id or parcel_ids parameter")

    except Exception as e:
        logger.error(f"Error processing USDA crop yields request: {e}", exc_info=True)
        return create_error_response(500, f"Internal server error: {e}")


def parse_event_parameters(event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Parse parameters from the various supported event sources."""
    try:
        # API Gateway
        if "pathParameters" in event:
            params: Dict[str, Any] = {}
            if event.get("pathParameters") and "parcel_id" in event["pathParameters"]:
                params["parcel_id"] = event["pathParameters"]["parcel_id"]
            query = event.get("queryStringParameters") or {}
            if "county_fips" in query:
                params["county_fips"] = query["county_fips"]
            if "state_code" in query:
                params["state_code"] = query["state_code"]
            if "force_refresh" in query:
                params["force_refresh"] = query["force_refresh"].lower() == "true"
            return params or None

        # Direct / orchestrator invocation
        if "parcel_id" in event or "parcel_ids" in event:
            params = {}
            for k in ("parcel_id", "parcel_ids", "county_fips", "state_code"):
                if k in event:
                    params[k] = event[k]
            if "force_refresh" in event:
                params["force_refresh"] = bool(event["force_refresh"])
            return params

        # SNS / SQS wrappers
        if "Records" in event:
            for record in event["Records"]:
                if "body" in record:
                    return parse_event_parameters(json.loads(record["body"]))
                if "Sns" in record and "Message" in record["Sns"]:
                    return parse_event_parameters(json.loads(record["Sns"]["Message"]))

        # EventBridge detail wrapper
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


def process_single_parcel(integration, parcel_id: str, county_fips: Optional[str],
                          state_code: Optional[str], force_refresh: bool) -> Dict[str, Any]:
    logger.info(f"Processing USDA yields for parcel {parcel_id} "
                f"(county_fips={county_fips}, state={state_code}, force_refresh={force_refresh})")
    result = integration.process_parcel_yields(
        parcel_id=parcel_id,
        county_fips=county_fips,
        state_code=state_code,
        force_refresh=force_refresh,
    )
    return result


def process_parcel_batch(integration, parcel_ids: list, force_refresh: bool) -> Dict[str, Any]:
    logger.info(f"Processing USDA yields for batch of {len(parcel_ids)} parcels")
    results, successful, failed = [], 0, 0
    for parcel_id in parcel_ids:
        try:
            result = process_single_parcel(integration, parcel_id, None, None, force_refresh)
            results.append(result)
            successful += 1 if result.get("success") else 0
            failed += 0 if result.get("success") else 1
        except Exception as e:
            logger.error(f"Error processing parcel {parcel_id}: {e}")
            results.append({"parcel_id": parcel_id, "success": False, "error": str(e)})
            failed += 1
    return {
        "batch_summary": {"total_parcels": len(parcel_ids),
                          "successful": successful, "failed": failed},
        "results": results,
    }


def create_success_response(data: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "statusCode": 200,
        "headers": _cors_headers(),
        "body": json.dumps(data, default=str),
    }


def create_error_response(status_code: int, message: str) -> Dict[str, Any]:
    return {
        "statusCode": status_code,
        "headers": _cors_headers(),
        "body": json.dumps({"error": message, "statusCode": status_code}),
    }


def _cors_headers() -> Dict[str, str]:
    return {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "Content-Type",
        "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    }


def health_check() -> Dict[str, Any]:
    try:
        environment = os.environ.get("ENVIRONMENT", "dev")
        integration = get_usda_integration(environment)
        integration.check_existing_yields("health_check_test")
        return {"status": "healthy", "environment": environment,
                "timestamp": str(datetime.now())}
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {"status": "unhealthy", "error": str(e), "timestamp": str(datetime.now())}


if __name__ == "__main__":  # pragma: no cover - manual smoke test
    class MockContext:
        function_name = "usda-crops-parcel-processor"
        memory_limit_in_mb = 512
        aws_request_id = "test-request-id"

    test_event = {"parcel_id": os.environ.get("TEST_PARCEL_ID", "test_parcel_001"),
                  "force_refresh": True}
    print(json.dumps(lambda_handler(test_event, MockContext()), indent=2, default=str))
