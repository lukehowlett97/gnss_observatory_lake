# GNSS Observatory Lake — Project Plan

## Project objective

Build **GNSS Observatory Lake** as a portfolio-grade, production-style Databricks and PySpark project using real GNSS data.

The purpose of the project is to demonstrate that I can design, build, test, optimise, operate and explain a modern data platform using:

- Databricks
- PySpark
- Spark SQL
- Delta Lake
- Medallion architecture
- Incremental processing
- Data quality and validation
- Workflow orchestration
- Performance optimisation
- Observability
- Batch and streaming concepts

The project should be strong enough to support Databricks and PySpark as genuine capabilities on my CV and in interviews.

The target story is:

> I took an existing GNSS analytics workload containing tens of gigabytes of historical data and redesigned part of it as a production-style Databricks lakehouse. I built incremental ingestion, PySpark transformations, Delta models, data-quality validation, orchestration, testing, performance optimisation and analytical outputs, then validated the new implementation against the existing system.

---

## Project context

The wider GNSS repository already contains:

- Historical RINEX processing
- Approximately 5 GB of Parquet station-day data
- Approximately 27 GB of detailed CSV observation data
- Existing station-day and satellite-day analytics
- Data-quality and anomaly logic
- CDDIS/Earthdata acquisition code
- A real-time NTRIP/RTCM ingestion path
- Existing GNSS Observatory monitoring work

GNSS Observatory Lake will sit alongside the existing Observatory project and focus specifically on the lakehouse and distributed data-engineering layer.

---

## Guiding principle

Do not optimise for:

> How many Databricks features can I use?

Optimise for:

> Could another engineer reasonably believe this system was built for a real internal customer?

Databricks is the implementation platform. The project should demonstrate end-to-end data-engineering capability.

---

# Phase 0 — Define the project

## Goals

Create a concise project brief before implementation.

Document:

- Problem statement
- Target users
- Primary use cases
- Data sources
- Expected data volumes
- Batch requirements
- Incremental processing requirements
- Analytical outputs
- Success criteria
- Explicit non-goals

## Example problem statement

A GNSS organisation operates a network of reference stations and consumes historical and near-real-time GNSS data.

Engineering teams currently rely on fragmented scripts and datasets to understand network quality.

Build a scalable data platform that:

- Ingests historical and operational GNSS datasets
- Standardises them into trusted analytical models
- Calculates repeatable quality metrics
- Detects degraded stations and network anomalies
- Supports historical backfills and incremental updates
- Exposes useful outputs to engineers

## Example acceptance criteria

- Historical data can be backfilled
- New observations can be processed incrementally
- Reprocessing does not create uncontrolled duplicates
- Raw data remains reproducible
- Gold metrics can be traced back to source data
- Spark outputs can be validated against the existing implementation
- Pipeline failures are visible
- Data-quality failures are visible
- Deployment and execution are reproducible

---

# Phase 1 — Databricks foundation

## Goals

Create the initial Databricks development environment and project structure.

## Tasks

- Create Databricks workspace
- Understand workspace organisation
- Configure Git integration
- Create project repository structure
- Establish development and execution workflow
- Configure schemas/catalogues where available
- Establish environment/configuration handling
- Confirm how local code and Databricks code will interact
- Document Free Edition limitations where relevant

## Capabilities demonstrated

- Databricks workspace fundamentals
- Repository integration
- Environment organisation
- Project structure
- Reproducible development

---

# Phase 2 — Dataset profiling

## Goals

Understand the source datasets using PySpark before designing transformations.

## Profile

- File counts
- Total data volumes
- Schemas
- Column types
- Date ranges
- Station counts
- Satellite counts
- Constellation coverage
- Observation types
- Null rates
- Duplicate rates
- File-size distribution
- Partition distribution
- Station/date skew
- Invalid values
- Timestamp representations

## Produce

A reproducible profiling report describing the source data.

## Capabilities demonstrated

- Spark DataFrame exploration
- Schema inspection
- Data profiling
- Distributed aggregation
- Data-quality assessment
- Engineering before implementation

---

# Phase 3 — Bronze ingestion

## Goals

Create the raw lakehouse layer.

Initial source:

- Existing Parquet station-day files

Later sources:

- Raw CSV observation corpus
- Real-time RTCM-derived files

## Bronze requirements

Preserve source data as closely as possible.

Add metadata such as:

- `source_filename`
- `source_path`
- `ingested_at`
- `ingestion_run_id`
- `source_year`
- `source_doy`

## Initial tables

Potential tables:

- `bronze_gnss_observations`
- `bronze_station_metadata`
- `bronze_pipeline_events`
- `bronze_rtcm_frames`
- `bronze_ssr_corrections`

## Engineering requirements

- Explicit schemas where appropriate
- Schema enforcement
- Corrupt-record handling
- Provenance
- Repeatable ingestion
- Idempotent behaviour where possible
- Year/day-of-year partition safety

## Capabilities demonstrated

- Delta Lake
- Spark ingestion
- Schema handling
- Metadata and provenance
- Raw-data modelling
- Idempotent engineering

---

# Phase 4 — Silver modelling

## Goals

Create canonical GNSS analytical datasets.

## Canonical fields

Standardise fields such as:

- `station`
- `satellite`
- `constellation`
- `observation_type`
- `event_time_utc`
- `arrival_time_utc`
- `event_date`
- `year`
- `doy`
- `gps_week`
- `gnss_time_system`
- `source_filename`
- `ingested_at`

## Important modelling decision

Keep:

- Event time
- Arrival time

as separate concepts.

Scientific analysis should use GNSS event time where available.

Operational ingestion may use arrival time.

## Silver tables

Potential tables:

- `silver_gnss_observations`
- `silver_station_epochs`
- `silver_satellite_observations`
- `silver_stream_outages`
- `silver_orbit_corrections`

## Data quality

Validate:

- Station identifiers
- Satellite identifiers
- Constellations
- Timestamp validity
- Duplicate observations
- Missing event times
- Impossible ranges
- Unexpected observation types

Invalid records should be quarantined rather than silently discarded.

## Capabilities demonstrated

- PySpark transformations
- Spark column expressions
- Timestamp handling
- Data modelling
- Schema standardisation
- Data-quality pipelines

---

# Phase 5 — Gold analytics

## Goals

Rebuild selected existing GNSS analytical products using PySpark.

## Candidate Gold tables

- `gold_station_day_quality`
- `gold_station_satellite_day`
- `gold_network_anomalies`
- `gold_station_availability`
- `gold_cycle_slip_events`

## Candidate metrics

- Observation completeness
- Availability
- Epoch gaps
- Satellite count
- Constellation coverage
- Cycle-slip rate
- Observation dispersion
- Station quality score
- Station deviation from network behaviour

## PySpark techniques to practise

- `groupBy`
- Aggregations
- Multi-column joins
- Window functions
- `lag`
- `lead`
- Rolling calculations
- Conditional expressions
- Deduplication
- Timestamp functions
- Spark SQL

## Example distributed problem

Instead of:

```text
for station:
    for satellite:
        calculate_metrics()
```

design the processing around:

GROUP BY station, satellite, event_date

and window operations over ordered GNSS epochs.

Capabilities demonstrated
Distributed analytical thinking
PySpark
Spark SQL
Window functions
Aggregations
Production analytical modelling
Phase 6 — Validate against the existing pipeline
Goals

Prove that the new Spark implementation is trustworthy.

Approach

Run identical source data through:

Existing Python pipeline
        |
        +---- expected analytical output

Databricks / PySpark pipeline
        |
        +---- new analytical output

Compare both outputs using automated validation.

Validate
Record counts
Station/day keys
Satellite/day keys
Metric values
Null behaviour
Timestamps
Tolerance-based numeric differences
Output

Create a validation report showing:

Number of records checked
Exact matches
Tolerance matches
Failures
Investigated discrepancies
Capabilities demonstrated
Migration engineering
Regression testing
Data validation
Reproducibility
Trustworthy analytical systems
Phase 7 — Incremental processing
Goals

Move beyond static batch processing.

New or changed source data should only trigger the required downstream work.

Target behaviour
New files arrive
      |
      v
Identify affected station/date partitions
      |
      v
Update Bronze/Silver records
      |
      v
Recompute affected Gold outputs
      |
      v
MERGE results into Delta
Important scenario

Handle late-arriving data.

Example:

Station-day metric generated at midnight
Additional observation data arrives several hours later
Existing Gold result must be updated
Unaffected station-days should not be recomputed
Capabilities demonstrated
Delta MERGE
Incremental processing
Idempotency
Late-arriving data
Dependency management
Production data engineering
Phase 8 — Spark performance work
Goals

Demonstrate understanding of why Spark behaves the way it does.

Do not stop at working code.

Benchmark areas

Compare:

CSV vs Parquet vs Delta
Naive Spark vs optimised Spark
Different partition strategies
Different join strategies
Different file sizes
Investigate
Spark execution plans
Shuffles
Partition counts
Skew
Predicate pushdown
Partition pruning
Broadcast joins
Caching
Adaptive execution
Small-files problems
Required case study

Pick at least one real transformation and document:

Initial implementation
        |
Observed performance problem
        |
Spark execution evidence
        |
Engineering change
        |
Measured result
Capabilities demonstrated
Spark optimisation
Execution-plan analysis
Distributed-system reasoning
Performance benchmarking
Phase 9 — Workflow orchestration
Goals

Move execution out of manual notebooks.

Create a repeatable Databricks workflow.

Example workflow
Ingest source
    |
Validate Bronze
    |
Build Silver
    |
Validate Silver
    |
Build Gold
    |
Run Gold tests
    |
Publish analytical outputs
Requirements

Support:

Date parameters
Station parameters
Backfills
Incremental runs
Failure handling
Dependency ordering

A failed upstream quality check should prevent invalid downstream publication.

Capabilities demonstrated
Databricks Workflows
Pipeline orchestration
Job dependencies
Parameterisation
Operational data engineering
Phase 10 — Operational observability
Goals

Make the pipeline observable.

This should be separate from GNSS analytical monitoring.

Track
Latest successful run
Latest failed run
Pipeline duration
Rows processed
Files processed
Rows rejected
Data freshness
Processing lag
Station coverage
Data-quality failures
Potential output

Create an operational Databricks dashboard.

Capabilities demonstrated
Data-pipeline observability
Production ownership
Failure detection
Operational monitoring
Phase 11 — Streaming extension
Goals

Introduce near-real-time GNSS data.

Use the existing RTCM/NTRIP pipeline rather than inventing synthetic streaming data.

Architecture
NTRIP
  |
RTCM decoder
  |
Rust ingestion
  |
Partitioned Parquet / event files
  |
Databricks incremental ingestion
  |
Silver
  |
Gold
Important concepts
Event time
Arrival time
Incremental file processing
Late data
Duplicate handling
Streaming state where appropriate
Watermarks where justified
Capabilities demonstrated
Structured Streaming concepts
Event-time processing
Real-world streaming architecture
Batch/streaming integration
Phase 12 — User-facing analytical product
Goals

Expose Gold outputs as something useful.

Do not make notebooks the main demonstration.

Build a GNSS Observatory Lake dashboard
Network overview

Show:

Number of stations
Number of observations
Network availability
Number of degraded stations
Latest processed timestamp
Network health

Show:

Station/date quality heatmap
Station availability
Network anomalies
Constellation coverage
Station drill-down

Show:

Availability
Observation count
Satellite coverage
Cycle slips
Epoch gaps
Quality score
Anomaly drill-down

Allow:

Network
  |
Station
  |
Date
  |
Satellite
  |
Underlying observations
Capabilities demonstrated
Databricks SQL
Analytical product design
Gold-layer consumption
Turning engineering into user value
Phase 13 — Engineering quality
Goals

Treat the project like maintainable software.

Add
Unit tests
Integration tests
Validation tests
Formatting
Linting
Configuration management
CI
Secrets handling
Clear local-development instructions
Clear Databricks execution instructions

Reusable transformation logic should live in Python modules rather than only in notebooks.

Use notebooks primarily for:

Exploration
Demonstration
Interactive analysis
Capabilities demonstrated
Software engineering
Testable data pipelines
Maintainability
CI/CD
Professional project structure
Phase 14 — Security and governance
Goals

Demonstrate awareness of production governance concerns.

Cover
Secret handling
Source-data access
Data ownership
Table ownership
Schema boundaries
Lineage
Dataset licensing
CDDIS/Earthdata credentials
Retention considerations
Data redistribution restrictions

Do not pretend to have implemented enterprise controls that are unavailable in the development environment.

Document how they would be handled in production.

Capabilities demonstrated
Data governance
Security awareness
Production architecture judgement
Phase 15 — Portfolio packaging
Goals

Turn the engineering work into a flagship public project.

README structure

Suggested sections:

Problem
Project goals
Architecture
Source datasets
Bronze/Silver/Gold model
PySpark transformations
Data quality
Incremental processing
Validation
Performance optimisation
Orchestration
Dashboard
Key engineering decisions
Running the project
Future work
Engineering documentation

Add:

docs/
├── architecture/
├── decisions/
├── benchmarks/
├── validation/
└── diagrams/

Potential architecture decision records:

Event time vs arrival time
Why Delta Lake
Partition strategy
Bronze data retention
Incremental processing strategy
Notebook vs Python module boundary
Late-arriving data handling
Proposed repository structure
GNSS/
├── observatory/
│   └── ...
│
├── gnss-observatory-lake/
│   ├── README.md
│   ├── pyproject.toml
│   │
│   ├── src/
│   │   └── gnss_observatory_lake/
│   │       ├── ingestion/
│   │       ├── bronze/
│   │       ├── silver/
│   │       ├── gold/
│   │       ├── quality/
│   │       └── common/
│   │
│   ├── notebooks/
│   │   ├── exploration/
│   │   └── demos/
│   │
│   ├── tests/
│   │   ├── unit/
│   │   ├── integration/
│   │   └── validation/
│   │
│   ├── sql/
│   ├── resources/
│   ├── config/
│   │
│   ├── docs/
│   │   ├── architecture/
│   │   ├── decisions/
│   │   ├── benchmarks/
│   │   └── validation/
│   │
│   └── sample_data/
PySpark learning checklist

The project should deliberately provide practical experience with:

Core Spark
SparkSession
DataFrames
Schemas
Lazy evaluation
Transformations
Actions
Spark SQL
Column expressions
Filtering
Sorting
Aggregation
Intermediate PySpark
groupBy
Joins
Window functions
lag
lead
Rolling calculations
Timestamp operations
Conditional expressions
Deduplication
Structured/nested fields
Repartitioning
Advanced Spark
Execution plans
Shuffle behaviour
Partition pruning
Predicate pushdown
Broadcast joins
Data skew
Caching
Adaptive execution
Small-files optimisation
File-layout design
Delta Lake
Delta tables
Schema enforcement
Schema evolution
Append
Overwrite
MERGE
Incremental processing
Deduplication
Databricks
Workspace
Repos/Git integration
Notebooks
SQL
Jobs/Workflows
Dashboards
Parameters
Data lineage/catalogue concepts
Monitoring
Development progression

Do not start by ingesting the entire repository.

Scale progressively.

Milestone 1
1 station
x
1 day

Deliver:

Existing Parquet
    |
Bronze Delta
    |
Silver PySpark
    |
Gold station-day metrics
    |
Databricks visualisation

This should be the first complete end-to-end path.

Milestone 2
10 stations
x
30 days

Focus on:

Partitioning
Reusable transformations
Validation
Basic orchestration
Milestone 3

Process the complete existing Parquet corpus.

Focus on:

Scale
Performance
File layout
Benchmarking
Milestone 4

Introduce selected raw CSV data.

Focus on:

CSV ingestion
Schema handling
CSV-to-Delta optimisation
Larger-volume transformations
Milestone 5

Implement proper incremental processing.

Focus on:

Delta MERGE
Late-arriving data
Idempotency
Backfills
Milestone 6

Add near-real-time RTCM ingestion.

Focus on:

Incremental streaming
Event time
Arrival time
Operational freshness
Flagship engineering demonstrations
1. Existing Python vs PySpark validation

Demonstrate that the Spark migration produces trustworthy analytical results.

Target evidence could eventually look like:

619 station-days validated
618 matched
1 discrepancy investigated

Only publish real measured numbers.

2. Spark performance case study

Implement one transformation naively.

Inspect:

df.explain()

Measure it.

Optimise it.

Measure again.

Document:

Problem
Execution plan
Shuffle behaviour
Partition behaviour
Change
Result
3. Late-arriving data

Demonstrate:

Station-day Gold result exists
Additional source data arrives later
Affected Silver records update
Relevant Gold metrics recompute
Unaffected data remains untouched

This provides a strong interview example covering:

Incremental processing
Idempotency
Delta Lake
Event time
Pipeline dependencies
Data correctness
Final demonstration flow

The live demo should start with the product rather than the code.

Screen 1 — Network overview

Example:

GNSS Observatory Lake

40 stations
3 constellations
XX million observations

Network availability       XX.X%
Degraded stations          X
Latest processed data      HH:MM UTC
Screen 2 — Network health

Show a station/date health matrix.

Select a degraded station.

Screen 3 — Station drill-down

Show:

Station: XXXX

Availability
Epoch gaps
Cycle slips
Satellite coverage
Observation count
Quality status
Screen 4 — Anomaly investigation

Show how a user moves from:

Network anomaly
    |
Station
    |
Metric change
    |
Satellite / constellation
    |
Raw evidence
Screen 5 — Databricks workflow

Show:

Successful jobs
Pipeline stages
Run durations
Data freshness
Screen 6 — Lakehouse model

Show:

Bronze
  |
Silver
  |
Gold

Explain lineage and responsibilities of each layer.

Screen 7 — Spark engineering

Only then open relevant PySpark code and explain:

Transformation design
Window logic
Incremental behaviour
Performance decisions
Portfolio positioning
Project title

GNSS Observatory Lake

Suggested subtitle

A production-style Databricks lakehouse for historical and near-real-time GNSS network quality analytics.

Suggested technology line

Databricks · PySpark · Spark SQL · Delta Lake · Python · GNSS

Suggested portfolio summary

GNSS Observatory Lake is an end-to-end lakehouse for transforming historical and near-real-time satellite observations into trusted network-quality analytics. The project uses Databricks, PySpark and Delta Lake to ingest, validate and model GNSS data, calculate station and network quality metrics, handle incremental and late-arriving data, and expose analytical outputs through an operational dashboard.

CV outcome

Once genuinely implemented, the project should support a CV entry similar to:

GNSS Observatory Lake — Databricks / PySpark

Designed and built a Databricks lakehouse for historical and incremental GNSS analytics, using PySpark and Delta Lake to ingest, validate and transform large observation datasets into station and network-quality products. Implemented incremental processing, automated validation against an existing Python analytical pipeline, workflow orchestration and Spark performance optimisation.

Potential skills supported by the completed project:

Databricks
PySpark
Apache Spark
Spark SQL
Delta Lake
Lakehouse architecture
Medallion architecture
Incremental processing
Data-quality engineering
Workflow orchestration
Distributed data processing
Definition of done

GNSS Observatory Lake is considered portfolio-ready when:

A real GNSS dataset can run end-to-end through Bronze, Silver and Gold
Core transformations are implemented with PySpark
The implementation is validated against existing analytical outputs
Incremental processing works
Late-arriving data is handled correctly
Data-quality checks exist
Pipeline execution is orchestrated
Operational pipeline health is visible
At least one Spark performance investigation is documented
Gold outputs are visible through a useful dashboard
The repository can be understood by another engineer
The README clearly explains the engineering decisions
The project can be demonstrated live
All published performance and scale claims are based on actual measurements
Immediate first milestone

Build the smallest meaningful complete path:

1 station
x
1 day
x
real GNSS data

Existing Parquet
       |
       v
Bronze Delta
       |
       v
Silver PySpark
       |
       v
Gold station-day metrics
       |
       v
Databricks visualisation

Do not move on to streaming, optimisation or the full dataset until this path works end-to-end.

Once this exists, grow the project progressively:

1 station/day
      |
10 stations / 30 days
      |
Full Parquet corpus
      |
Large CSV ingestion
      |
Incremental processing
      |
Late-arriving data
      |
Streaming RTCM
      |
Portfolio-ready platform