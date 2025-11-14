#!/usr/bin/env python3
"""
Backfill script to upload all historical NSW1 price signals to Data Clearing House.

This script queries all price signals from the database for the NSW1 region
and uploads them to the DCH API in the same format as the Lambda function.
"""

import json
import logging
import sys
from collections.abc import Sequence

from exploren_rds_models.models import PriceSignal
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from config import (
    DB_HOST,
    DB_NAME,
    DB_PASSWORD,
    DB_PORT,
    DB_USER,
)
from main import construct_dch_payload, upload_to_dch

logging.basicConfig(
    format="%(asctime)s | %(name)s | %(lineno)s | %(levelname)s | %(message)s",
    level=logging.INFO,
)
LOGGER = logging.getLogger(__name__)

DB_CONNECTION_TIMEOUT_SECONDS = 10


def get_engine():
    """Create database engine."""
    engine_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

    return create_engine(
        engine_url,
        echo=False,
        echo_pool=True,
        connect_args={
            "connect_timeout": DB_CONNECTION_TIMEOUT_SECONDS,
            "application_name": "MTA to DCH Backfill",
        },
    )


def get_session() -> Session:
    """Get database session."""
    return Session(get_engine())


def get_all_price_signals(region_id: str = "NSW1") -> Sequence[PriceSignal]:
    """
    Query all price signals for a specific region.

    Args:
        region_id: The region identifier (default: NSW1)

    Returns:
        Sequence of all PriceSignal objects for the region
    """
    with get_session() as db:
        price_signals = (
            db.query(PriceSignal)
            .filter(PriceSignal.regionid == region_id)
            .order_by(PriceSignal.settlementdate.asc())
            .all()
        )
        LOGGER.info(
            "Found %d total price signals for region %s",
            len(price_signals),
            region_id,
        )
        return price_signals


def batch_list(items: list, batch_size: int) -> list[list]:
    """
    Split a list into batches of specified size.

    Args:
        items: List to batch
        batch_size: Size of each batch

    Returns:
        List of batches
    """
    batches = []
    for i in range(0, len(items), batch_size):
        batches.append(items[i : i + batch_size])
    return batches


def main():
    """Main backfill function."""
    BATCH_SIZE = 50

    try:
        LOGGER.info("Starting backfill process for NSW1 price signals...")

        # Query all price signals for NSW1
        price_signals = get_all_price_signals(region_id="NSW1")

        if not price_signals:
            LOGGER.warning("No price signals found in the database")
            return 0

        LOGGER.info("Found %d price signals to backfill", len(price_signals))

        # Split into batches
        batches = batch_list(list(price_signals), BATCH_SIZE)
        total_batches = len(batches)
        LOGGER.info(
            "Splitting into %d batches of %d observations each",
            total_batches,
            BATCH_SIZE,
        )

        # Upload each batch
        successful_uploads = 0
        failed_uploads = 0

        for batch_num, batch in enumerate(batches, 1):
            LOGGER.info(
                "Processing batch %d/%d (%d observations)...",
                batch_num,
                total_batches,
                len(batch),
            )

            try:
                # Construct the DCH payload for this batch
                dch_payload = construct_dch_payload(batch)

                # Upload to DCH
                upload_result = upload_to_dch(dch_payload)

                LOGGER.info(
                    "Batch %d/%d uploaded successfully. Status: %s",
                    batch_num,
                    total_batches,
                    upload_result.get("statusCode"),
                )
                successful_uploads += 1

            except Exception as e:
                LOGGER.error(
                    "Failed to upload batch %d/%d: %s",
                    batch_num,
                    total_batches,
                    str(e),
                )
                failed_uploads += 1
                # Continue with next batch instead of failing completely

        LOGGER.info(
            "Backfill completed! Total: %d observations, Successful batches: %d, Failed batches: %d",
            len(price_signals),
            successful_uploads,
            failed_uploads,
        )

        return 0 if failed_uploads == 0 else 1

    except Exception as e:
        LOGGER.exception("Error during backfill process")
        return 1


if __name__ == "__main__":
    sys.exit(main())
