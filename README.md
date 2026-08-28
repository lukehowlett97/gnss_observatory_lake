# GNSS Observatory Lake

Databricks lakehouse project for historical and streaming GNSS observatory data.

The initial architecture and delivery plan are documented in
[`docs/project_plan.md`](docs/project_plan.md).

## Status

Project scaffold. Databricks workspace configuration, pipelines, and data contracts
will be added as the implementation develops.

## First job: source station-day profiling

`profile_source_station_day` is packaged as a Python wheel, runs as a serverless
Python wheel task, reads one GNSS station-day Parquet source with PySpark, and
idempotently publishes a compact profile to a Delta table. The default table is
`monitoring.source_profile`; configure `output_table` with a two- or three-part
name when using a different schema or Unity Catalog catalog.

The inspected PRX source schema supports event-time range, constellation,
satellite `(constellation, prn)`, and RINEX observation-type statistics. Missing
optional GNSS columns produce null metrics instead of failing the run.
The reader also normalizes the source's Arrow nanosecond timestamp to Spark's
microsecond timestamp representation.

The bundle builds `dist/gnss_observatory_lake-*.whl` during deployment and adds
it to the serverless job environment. The source-format notebook remains useful
for interactive development, but the deployed job does not depend on notebook
working-directory or `sys.path` behavior.

### Local tests

Install a Java runtime and the development dependencies, then run:

```bash
python -m pip install -e '.[dev]'
pytest
```

### Bundle lifecycle

Authenticate the Databricks CLI, then validate and deploy the development target:

```bash
cp .env.example .env
set -a; source .env; set +a
databricks auth login --host "$DATABRICKS_HOST"
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

Run the job with values accessible from the workspace:

```bash
databricks bundle run -t dev profile_source_station_day -- \
  --source_path=/Volumes/workspace/default/gnss_source/BELE00BRA_R_20261050000_01D_30S_MO.parquet \
  --station=BELE \
  --year=2026 \
  --doy=105 \
  --output_table=monitoring.source_profile
```
