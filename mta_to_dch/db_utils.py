import logging
from datetime import datetime, timedelta

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

LOGGER = logging.getLogger(__name__)
DB_CONNECTION_TIMEOUT_SECONDS = 10


def get_session() -> Session:
    return Session(get_engine())


def get_engine():
    engine_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

    return create_engine(
        engine_url,
        echo=False,
        echo_pool=True,
        connect_args={
            "connect_timeout": DB_CONNECTION_TIMEOUT_SECONDS,
            "application_name": "Kerbside Charger Controller",
        },
    )


def get_last_hour_price_signals(region_id: str = "NSW1") -> list[PriceSignal]:
    """
    Query price signals from the last hour for a specific region.

    Args:
        region_id: The region identifier (default: NSW1)

    Returns:
        Sequence of PriceSignal objects from the last hour
    """
    with get_session() as db:
        # Calculate the timestamp for one hour ago
        one_hour_ago = datetime.now() - timedelta(hours=1)

        price_signals = (
            db.query(PriceSignal)
            .filter(
                PriceSignal.regionid == region_id,
                PriceSignal.settlementdate >= one_hour_ago,
            )
            .order_by(PriceSignal.settlementdate.asc())
            .all()
        )
        LOGGER.info(
            "Found %d price signals for region %s since %s",
            len(price_signals),
            region_id,
            one_hour_ago,
        )
        return price_signals
