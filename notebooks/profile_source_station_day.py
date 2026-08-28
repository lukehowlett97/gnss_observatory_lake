# Databricks notebook source
"""Thin Databricks entrypoint for station-day source profiling."""

# COMMAND ----------

import logging
import sys
from pathlib import Path

# This optional interactive notebook runs from the deployed notebooks directory.
# Databricks notebook execution does not define __file__.
project_root = Path.cwd().parent
sys.path.insert(0, str(project_root / "src"))

from gnss_observatory_lake.profiling import (  # noqa: E402
    ProfileRequest,
    profile_station_day,
    publish_profile,
    read_station_day,
)

# COMMAND ----------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger("profile_source_station_day")
spark.conf.set("spark.sql.session.timeZone", "UTC")

for name, default in (
    ("source_path", ""),
    ("station", ""),
    ("year", ""),
    ("doy", ""),
    ("output_table", "monitoring.source_profile"),
):
    dbutils.widgets.text(name, default)

request = ProfileRequest(
    source_path=dbutils.widgets.get("source_path"),
    station=dbutils.widgets.get("station"),
    year=int(dbutils.widgets.get("year")),
    doy=int(dbutils.widgets.get("doy")),
)
output_table = dbutils.widgets.get("output_table").strip()

logger.info(
    "Profiling source=%s station=%s year=%s doy=%03d",
    request.source_path,
    request.station,
    request.year,
    request.doy,
)
source = read_station_day(spark, request.source_path)
logger.info("Resolved source schema: %s", source.schema.simpleString())
computed_profile = profile_station_day(source, request)
# Materialize the compact row once. Serverless compute does not support DataFrame
# caching, and both the Delta write and display are actions.
profile_row = computed_profile.first()
profile = spark.createDataFrame([profile_row], schema=computed_profile.schema)
publish_profile(profile, output_table)
logger.info("Published profile_key=%s to %s", profile_row.profile_key, output_table)

display(profile)
