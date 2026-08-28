"""Wheel entrypoint for the station-day source profiling job."""

from __future__ import annotations

import argparse
import logging
from collections.abc import Sequence

from pyspark.sql import SparkSession

from gnss_observatory_lake.profiling import (
    ProfileRequest,
    profile_station_day,
    publish_profile,
    read_station_day,
)

LOGGER = logging.getLogger(__name__)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse Databricks wheel-task parameters."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source_path", required=True)
    parser.add_argument("--station", required=True)
    parser.add_argument("--year", required=True, type=int)
    parser.add_argument("--doy", required=True, type=int)
    parser.add_argument("--output_table", default="monitoring.source_profile")
    return parser.parse_args(argv)


def run(spark: SparkSession, args: argparse.Namespace) -> str:
    """Profile and publish one station-day, returning its stable profile key."""

    request = ProfileRequest(
        source_path=args.source_path,
        station=args.station,
        year=args.year,
        doy=args.doy,
    )
    output_table = args.output_table.strip()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    LOGGER.info(
        "Profiling source=%s station=%s year=%s doy=%03d",
        request.source_path,
        request.station,
        request.year,
        request.doy,
    )

    source = read_station_day(spark, request.source_path)
    LOGGER.info("Resolved source schema: %s", source.schema.simpleString())
    computed_profile = profile_station_day(source, request)

    # Materialize once because serverless compute does not support DataFrame cache.
    profile_row = computed_profile.first()
    if profile_row is None:
        raise RuntimeError("Profiling unexpectedly produced no summary row")
    profile = spark.createDataFrame([profile_row], schema=computed_profile.schema)
    publish_profile(profile, output_table)
    LOGGER.info("Published profile_key=%s to %s", profile_row.profile_key, output_table)
    profile.show(truncate=False)
    return profile_row.profile_key


def main(argv: Sequence[str] | None = None) -> None:
    """Execute the Databricks wheel task."""

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    args = parse_args(argv)
    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    run(spark, args)
