import json
import logging
from datetime import datetime

import requests
from exploren_rds_models.models import PriceSignal

from config import DCH_API_KEY, DCH_DATA_POOL_ID, DCH_POINT_ID, DCH_UPLOAD_URL
from db_utils import get_last_hour_price_signals

logging.basicConfig(
    format="%(asctime)s | %(name)s | %(lineno)s | %(levelname)s | %(message)s"
)
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)


def calculate_rrp_value(rrp: float) -> int:
    """
    Calculate the DCH value based on RRP (Regional Reference Price).

    Args:
        rrp: The Regional Reference Price value

    Returns:
        0 if RRP < 500
        1 if 500 <= RRP < 1000
        2 if RRP >= 1000
    """
    if rrp < 500:
        return 0
    elif rrp < 1000:
        return 1
    else:
        return 2


def construct_dch_payload(
    price_signals: list[PriceSignal],
) -> dict[str, dict[str, int | str | float | list[int | float]]]:
    """
    Construct the JSON payload for DCH observations upload from price signals.

    Args:
        price_signals: List of PriceSignal objects from the database

    Returns:
        Dictionary formatted according to DCH API specification with metadata and data observations
    """
    if not price_signals:
        LOGGER.warning("No price signals provided to construct_dch_payload")
        return {"metadata": {"points": {}}, "data": []}

    # Set up the payload structure
    composite_point_id = f"evse:{DCH_DATA_POOL_ID}:{DCH_POINT_ID}"
    dch_payload = {"metadata": {"points": {"0": composite_point_id}}, "data": []}

    LOGGER.info("Composite point ID is: %s", composite_point_id)

    # Process each price signal
    for price_signal in price_signals:
        # Format timestamp to ISO8601 format required by DCH
        if isinstance(price_signal.settlementdate, datetime):
            timestamp = price_signal.settlementdate
        else:
            timestamp = datetime.fromisoformat(str(price_signal.settlementdate))

        valid_dch_dt_string = timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")

        # Calculate the value based on RRP
        rrp_value = calculate_rrp_value(float(price_signal.rrp))

        # Create observation object
        observation = {
            "t": valid_dch_dt_string,
            "p": "0",  # Point index from metadata
            "n": rrp_value,
        }

        dch_payload["data"].append(observation)
        LOGGER.debug(
            "Added observation: timestamp=%s, RRP=%s, value=%d",
            valid_dch_dt_string,
            price_signal.rrp,
            rrp_value,
        )

    LOGGER.info(
        "DCH Payload constructed with %d observations:\n%s",
        len(dch_payload["data"]),
        json.dumps(dch_payload, indent=2),
    )

    return dch_payload


def upload_to_dch(payload: dict) -> dict:
    """
    Upload observations to the Data Clearing House API.

    Args:
        payload: The formatted DCH payload

    Returns:
        API response as dictionary

    Raises:
        requests.exceptions.RequestException: If the upload fails
    """
    headers = {
        "X-Api-Key": DCH_API_KEY,
        "Content-Type": "application/json",
    }

    LOGGER.info("Uploading payload to DCH at %s", DCH_UPLOAD_URL)

    try:
        response = requests.post(
            DCH_UPLOAD_URL, json=payload, headers=headers, timeout=30
        )
        response.raise_for_status()

        LOGGER.info("Successfully uploaded to DCH. Status: %d", response.status_code)
        return {"statusCode": response.status_code, "body": response.text}
    except requests.exceptions.RequestException as e:
        LOGGER.exception("Failed to upload to DCH")
        raise


def lambda_handler(event, context):
    """
    AWS Lambda handler function.

    Queries the last hour of NSW1 price signals, formats them for DCH,
    and uploads the observations to the Data Clearing House API.

    Args:
        event: Lambda event object (not used, scheduled via EventBridge)
        context: Lambda context object

    Returns:
        Dictionary with statusCode and body
    """

    try:
        # Query the last hour of price signals for NSW1
        LOGGER.info("Querying price signals for region NSW1 from the last hour...")
        price_signals = get_last_hour_price_signals(region_id="NSW1")

        if not price_signals:
            LOGGER.warning("No price signals found for the last hour")
            return {
                "statusCode": 200,
                "body": json.dumps(
                    {"message": "No price signals found for the last hour"}
                ),
            }

        LOGGER.info("Found %d price signals to process", len(price_signals))

        # Construct the DCH payload
        dch_payload = construct_dch_payload(price_signals)

        # Upload to DCH
        upload_result = upload_to_dch(dch_payload)

        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "message": "Successfully processed and uploaded price signals to DCH",
                    "price_signals_processed": len(price_signals),
                    "dch_response": upload_result,
                }
            ),
        }

    except Exception as e:
        LOGGER.exception("Error processing price signals")
        return {
            "statusCode": 500,
            "body": json.dumps(
                {
                    "error": str(e),
                    "message": "Failed to process and upload price signals",
                }
            ),
        }
