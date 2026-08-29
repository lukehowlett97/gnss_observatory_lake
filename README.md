# GNSS Observatory Lake

Databricks/PySpark project for profiling GNSS station-day sources. The first
vertical slice is the production-style `profile_source_station_day` wheel task;
Bronze, Silver, and Gold processing are not implemented yet.

## What the first job does

The job reads exactly one station-day `.parquet` source and writes one compact
Delta profile to `workspace.monitoring.source_profile`. The inspected PRX schema
supports event-time range, constellation, satellite `(constellation, prn)`, and
RINEX observation-type statistics. Optional GNSS columns yield null metrics when
absent.

PRX encodes its observation timestamp as Parquet `TIMESTAMP(NANOS,true)`. Spark
cannot represent nanosecond timestamps directly, so the reader asks Spark to
expose the physical epoch-nanosecond value and converts it to a UTC Spark
timestamp with exact integer division. Spark timestamps have microsecond
precision: 0–999 trailing nanoseconds are therefore truncated.

The bundle builds the package as a pure-Python wheel and attaches it to a
serverless Python wheel task. There is intentionally no notebook entrypoint or
`sys.path`/working-directory dependency.

## Prerequisites and local verification

Install Python 3.10 or newer, a Java runtime suitable for local PySpark, `uv`,
and the Databricks CLI. From the repository root:

```bash
uv sync --extra dev
uv run pytest
uv run black --check src tests
uv build --wheel
```

`uv.lock` pins the local development and build environment. Application runtime
dependencies remain supplied by the Databricks serverless environment.

## Authentication and bundle validation

Copy the example and replace only its workspace-host placeholder:

```bash
cp .env.example .env
set -a; source .env; set +a
databricks auth login --host "$DATABRICKS_HOST" --profile "$DATABRICKS_CONFIG_PROFILE"
databricks bundle validate -t dev
```

OAuth is preferred. Do not add a token to a committed file.

## Workspace bootstrap and deployment

The bundle owns and creates the Unity Catalog schema `workspace.monitoring` on
deployment. The application only creates/merges its Delta table; it does not
create catalogs or schemas as a side effect.

The source Volume and BELE file are external inputs. Verify them before running:

```bash
databricks volumes read workspace.default.gnss_source
databricks fs ls dbfs:/Volumes/workspace/default/gnss_source
```

If the managed Volume does not exist, create `workspace.default.gnss_source` in
Catalog Explorer (or execute
`CREATE VOLUME IF NOT EXISTS workspace.default.gnss_source` with a SQL
warehouse), then upload the BELE Parquet file to it. Deploy and verify the
bundle-owned schema:

```bash
databricks bundle deploy -t dev
databricks schemas get workspace.monitoring
```

## Run the BELE sample

Station and date inputs have deliberately empty deployment defaults. A run that
does not supply meaningful values fails instead of profiling a bare directory
as an `UNKNOWN` station.

```bash
databricks bundle run -t dev profile_source_station_day -- \
  --source_path=/Volumes/workspace/default/gnss_source/BELE00BRA_R_20261050000_01D_30S_MO.parquet \
  --station=BELE \
  --year=2026 \
  --doy=105
```

The optional `--output_table` parameter defaults explicitly to
`workspace.monitoring.source_profile`.

Query the result in the Databricks SQL editor or a notebook:

```sql
SELECT *
FROM workspace.monitoring.source_profile
WHERE station = 'BELE' AND year = 2026 AND doy = 105;
```

Run the same bundle command again, then verify idempotency:

```sql
SELECT profile_key, count(*) AS rows_per_key
FROM workspace.monitoring.source_profile
GROUP BY profile_key
HAVING count(*) <> 1;
```

The second query should return no rows. The stable key is derived from source
path, station, year, and day-of-year; a Delta `MERGE` updates that profile on a
rerun rather than appending a duplicate.

## Free Edition notes

This slice uses serverless compute and Unity Catalog, which are appropriate for
Free Edition. Compute quotas and automatic shutdown can make startup slower or
temporarily prevent runs. The workspace catalog and permissions must allow the
deploying identity to create the `monitoring` schema and managed Delta table.
SQL queries require an available SQL warehouse or an interactive notebook.

The broader architecture notes remain in
[`docs/project_plan.md`](docs/project_plan.md).
