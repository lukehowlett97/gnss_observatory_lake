import pytest

from gnss_observatory_lake.jobs.profile_source_station_day import parse_args


def test_parse_args_accepts_wheel_task_named_parameters():
    args = parse_args(
        [
            "--source_path=/Volumes/raw/bele.parquet",
            "--station=BELE",
            "--year=2026",
            "--doy=105",
            "--output_table=workspace.monitoring.source_profile",
        ]
    )

    assert args.source_path == "/Volumes/raw/bele.parquet"
    assert args.station == "BELE"
    assert args.year == 2026
    assert args.doy == 105
    assert args.output_table == "workspace.monitoring.source_profile"


def test_parse_args_requires_source_identity():
    with pytest.raises(SystemExit):
        parse_args([])


def test_parse_args_uses_explicit_workspace_output_table():
    args = parse_args(
        [
            "--source_path=/Volumes/raw/bele.parquet",
            "--station=BELE",
            "--year=2026",
            "--doy=105",
        ]
    )

    assert args.output_table == "workspace.monitoring.source_profile"
