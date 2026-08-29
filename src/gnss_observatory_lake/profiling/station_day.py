"""Profile a GNSS station-day source with PySpark and publish it to Delta."""

from __future__ import annotations

import calendar
import logging
import re
from dataclasses import dataclass

from pyspark.sql import Column, DataFrame, SparkSession, functions as F
from pyspark.sql.types import LongType

LOGGER = logging.getLogger(__name__)

# These names come from the inspected PRX station-day Parquet schema. Optional
# fields are resolved explicitly so a reduced source remains profileable.
EVENT_TIME_COLUMN = "time_of_reception_in_receiver_time"
CONSTELLATION_COLUMN = "constellation"
SATELLITE_PRN_COLUMN = "prn"
OBSERVATION_TYPE_COLUMN = "rnx_obs_identifier"

_TABLE_PART = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class ProfileRequest:
    """Validated identity and location of one station-day source."""

    source_path: str
    station: str
    year: int
    doy: int

    def __post_init__(self) -> None:
        source_path = self.source_path.strip()
        station = self.station.strip().upper()
        if not source_path:
            raise ValueError("source_path must be a non-empty Parquet path")
        if not source_path.lower().endswith(".parquet"):
            raise ValueError(
                "source_path must identify one station-day .parquet source file"
            )
        if not station:
            raise ValueError("station must be a non-empty identifier")
        if self.year < 1 or self.year > 9999:
            raise ValueError("year must be between 1 and 9999")
        max_doy = 366 if calendar.isleap(self.year) else 365
        if self.doy < 1 or self.doy > max_doy:
            raise ValueError(f"doy must be between 1 and {max_doy} for {self.year}")
        object.__setattr__(self, "source_path", source_path)
        object.__setattr__(self, "station", station)


def read_station_day(spark: SparkSession, source_path: str) -> DataFrame:
    """Read a Parquet source, raising an actionable error on failure."""

    normalized_path = source_path.strip() if source_path else ""
    if not normalized_path:
        raise ValueError("source_path must be a non-empty Parquet path")
    if not normalized_path.lower().endswith(".parquet"):
        raise ValueError(
            "source_path must identify one station-day .parquet source file"
        )

    # Spark Connect defers Parquet schema analysis until a schema-dependent
    # operation. Configure the legacy physical-type mapping before load, then
    # force analysis inside the protected block. Arrow/Parquet nanoseconds are
    # exposed as epoch-nanosecond longs for explicit conversion below.
    try:
        spark.conf.set("spark.sql.legacy.parquet.nanosAsLong", "true")
        frame = spark.read.format("parquet").load(normalized_path)
        schema = frame.schema
    except Exception as exc:
        raise ValueError(
            f"Unable to read Parquet source_path={source_path!r}: {exc}"
        ) from exc
    if not frame.columns:
        raise ValueError(f"Parquet source has no columns: {source_path}")
    event_field = next(
        (field for field in schema.fields if field.name == EVENT_TIME_COLUMN),
        None,
    )
    if event_field is not None and isinstance(event_field.dataType, LongType):
        LOGGER.warning(
            "Converting %s from epoch nanoseconds to Spark microseconds; "
            "sub-microsecond precision is truncated",
            EVENT_TIME_COLUMN,
        )
        frame = frame.withColumn(
            EVENT_TIME_COLUMN,
            F.expr(f"timestamp_micros(`{EVENT_TIME_COLUMN}` DIV 1000)"),
        )
    return frame


def profile_station_day(source: DataFrame, request: ProfileRequest) -> DataFrame:
    """Return a one-row profile DataFrame for a station-day source."""

    if not source.columns:
        raise ValueError("source DataFrame must contain at least one column")

    columns = set(source.columns)
    event_time = EVENT_TIME_COLUMN if EVENT_TIME_COLUMN in columns else None
    constellation = CONSTELLATION_COLUMN if CONSTELLATION_COLUMN in columns else None
    satellite_prn = SATELLITE_PRN_COLUMN if SATELLITE_PRN_COLUMN in columns else None
    observation_type = (
        OBSERVATION_TYPE_COLUMN if OBSERVATION_TYPE_COLUMN in columns else None
    )
    try:
        source_files = sorted(source.inputFiles())
    except (AttributeError, NotImplementedError):
        LOGGER.warning(
            "Spark did not expose inputFiles(); using source_path provenance"
        )
        source_files = []
    if not source_files:
        source_files = [request.source_path]

    null_expressions = [
        F.coalesce(F.sum(F.when(F.col(name).isNull(), 1).otherwise(0)), F.lit(0))
        .cast("long")
        .alias(name)
        for name in source.columns
    ]
    aggregations: list[Column] = [F.count(F.lit(1)).alias("row_count")]
    aggregations.extend(null_expressions)
    aggregations.extend(
        [
            _min_or_null(event_time).alias("min_event_time"),
            _max_or_null(event_time).alias("max_event_time"),
            _satellite_count(constellation, satellite_prn).alias(
                "distinct_satellite_count"
            ),
            _distinct_count(constellation).alias("distinct_constellation_count"),
            _distinct_count(observation_type).alias("distinct_observation_type_count"),
        ]
    )

    aggregate = source.agg(*aggregations)
    duplicate_count = source.count() - source.dropDuplicates(source.columns).count()
    null_values = [F.col(f"`{name}`") for name in source.columns]
    null_map = F.map_from_arrays(
        F.array(*[F.lit(name) for name in source.columns]), F.array(*null_values)
    )

    # The key is stable across reruns; profiled_at is deliberately not part of it.
    profile_key = F.sha2(
        F.concat_ws(
            "\u001f",
            F.lit(request.source_path),
            F.lit(request.station),
            F.lit(str(request.year)),
            F.lit(f"{request.doy:03d}"),
        ),
        256,
    )

    return aggregate.select(
        profile_key.alias("profile_key"),
        F.lit(request.station).alias("station"),
        F.lit(request.year).cast("int").alias("year"),
        F.lit(request.doy).cast("int").alias("doy"),
        F.lit(request.source_path).alias("source_path"),
        F.array(*[F.lit(path) for path in source_files]).alias("source_files"),
        F.col("row_count"),
        F.lit(len(source.columns)).cast("int").alias("column_count"),
        F.col("min_event_time"),
        F.col("max_event_time"),
        F.col("distinct_satellite_count"),
        F.col("distinct_constellation_count"),
        F.col("distinct_observation_type_count"),
        F.lit(duplicate_count).cast("long").alias("duplicate_row_count"),
        null_map.alias("null_counts"),
        sum(null_values[1:], null_values[0]).cast("long").alias("null_cell_count"),
        F.lit(source.schema.json()).alias("source_schema_json"),
        F.current_timestamp().alias("profiled_at"),
    )


def publish_profile(profile: DataFrame, output_table: str) -> None:
    """Idempotently merge a one-row profile into a managed Delta table."""

    quoted_table, _ = _validated_table_name(output_table)
    spark = profile.sparkSession

    if not spark.catalog.tableExists(output_table):
        LOGGER.info("Creating Delta profile table %s", output_table)
        profile.write.format("delta").mode("errorifexists").saveAsTable(output_table)
        return

    view_name = "_source_station_day_profile_update"
    profile.createOrReplaceTempView(view_name)
    columns = profile.columns
    updates = ", ".join(f"target.`{name}` = source.`{name}`" for name in columns)
    insert_columns = ", ".join(f"`{name}`" for name in columns)
    insert_values = ", ".join(f"source.`{name}`" for name in columns)
    LOGGER.info("Merging station-day profile into %s", output_table)
    spark.sql(f"""
        MERGE INTO {quoted_table} AS target
        USING {view_name} AS source
        ON target.profile_key = source.profile_key
        WHEN MATCHED THEN UPDATE SET {updates}
        WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})
        """)


def _distinct_count(column: str | None) -> Column:
    if column is None:
        return F.lit(None).cast("long")
    return F.countDistinct(F.col(column)).cast("long")


def _satellite_count(constellation: str | None, prn: str | None) -> Column:
    if constellation is None or prn is None:
        return F.lit(None).cast("long")
    identity = F.when(
        F.col(constellation).isNotNull() & F.col(prn).isNotNull(),
        F.struct(F.col(constellation), F.col(prn)),
    )
    return F.countDistinct(identity).cast("long")


def _min_or_null(column: str | None) -> Column:
    if column is None:
        return F.lit(None).cast("timestamp")
    return F.min(F.col(column)).cast("timestamp")


def _max_or_null(column: str | None) -> Column:
    if column is None:
        return F.lit(None).cast("timestamp")
    return F.max(F.col(column)).cast("timestamp")


def _validated_table_name(table_name: str) -> tuple[str, str | None]:
    parts = [part.strip() for part in table_name.split(".")]
    if len(parts) not in (1, 2, 3) or any(not _TABLE_PART.fullmatch(p) for p in parts):
        raise ValueError(
            "output_table must be a one-, two-, or three-part SQL identifier"
        )
    quoted = ".".join(f"`{part}`" for part in parts)
    namespace = ".".join(f"`{part}`" for part in parts[:-1]) or None
    return quoted, namespace
