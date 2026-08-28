from datetime import datetime

import pytest
from pyspark.sql import Row

from gnss_observatory_lake.profiling.station_day import (
    ProfileRequest,
    _validated_table_name,
    profile_station_day,
    read_station_day,
)


def test_profile_real_source_columns(spark):
    timestamp = datetime(2026, 4, 15, 0, 0)
    source = spark.createDataFrame(
        [
            Row(
                time_of_reception_in_receiver_time=timestamp,
                constellation="GPS",
                prn=1,
                rnx_obs_identifier="C1C",
                C_obs_m=20.0,
            ),
            Row(
                time_of_reception_in_receiver_time=timestamp,
                constellation="GPS",
                prn=1,
                rnx_obs_identifier="C1C",
                C_obs_m=20.0,
            ),
            Row(
                time_of_reception_in_receiver_time=datetime(2026, 4, 15, 0, 0, 30),
                constellation="Galileo",
                prn=11,
                rnx_obs_identifier="L1C",
                C_obs_m=None,
            ),
        ]
    )

    result = profile_station_day(
        source,
        ProfileRequest("/Volumes/raw/bele.parquet", "bele", 2026, 105),
    ).first()

    assert result.station == "BELE"
    assert result.row_count == 3
    assert result.column_count == 5
    assert result.duplicate_row_count == 1
    assert result.distinct_satellite_count == 2
    assert result.distinct_constellation_count == 2
    assert result.distinct_observation_type_count == 2
    assert result.null_counts["C_obs_m"] == 1
    assert result.null_cell_count == 1
    assert result.min_event_time == timestamp


def test_profile_handles_missing_optional_gnss_columns(spark):
    source = spark.createDataFrame([Row(value=1), Row(value=None)])

    result = profile_station_day(
        source, ProfileRequest("/Volumes/raw/minimal.parquet", "TEST", 2024, 366)
    ).first()

    assert result.row_count == 2
    assert result.min_event_time is None
    assert result.distinct_satellite_count is None
    assert result.distinct_constellation_count is None
    assert result.distinct_observation_type_count is None
    assert result.null_counts == {"value": 1}


def test_profile_request_validates_leap_year_day():
    with pytest.raises(ValueError, match="between 1 and 365"):
        ProfileRequest("/source", "TEST", 2023, 366)


def test_read_station_day_reports_invalid_source(spark, tmp_path):
    with pytest.raises(ValueError, match="Unable to read Parquet"):
        read_station_day(spark, str(tmp_path / "missing"))


def test_read_station_day_normalizes_epoch_nanoseconds(spark, tmp_path):
    path = str(tmp_path / "epoch-nanos")
    spark.createDataFrame(
        [Row(time_of_reception_in_receiver_time=1_000_000_000)]
    ).write.parquet(path)

    row = read_station_day(spark, path).first()

    assert row.time_of_reception_in_receiver_time.timestamp() == 1


@pytest.mark.parametrize(
    ("name", "quoted", "namespace"),
    [
        ("source_profile", "`source_profile`", None),
        ("monitoring.source_profile", "`monitoring`.`source_profile`", "`monitoring`"),
        (
            "catalog.monitoring.source_profile",
            "`catalog`.`monitoring`.`source_profile`",
            "`catalog`.`monitoring`",
        ),
    ],
)
def test_validated_table_name(name, quoted, namespace):
    assert _validated_table_name(name) == (quoted, namespace)


def test_validated_table_name_rejects_sql():
    with pytest.raises(ValueError, match="SQL identifier"):
        _validated_table_name("monitoring.source-profile; DROP TABLE x")
