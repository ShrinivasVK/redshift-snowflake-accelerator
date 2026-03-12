# ArisData — Redshift to Snowflake Migration Accelerator

## Overview

The **ArisData Migration Accelerator** is a comprehensive, enterprise-grade framework designed to automate and streamline the migration of Amazon Redshift data warehouses to Snowflake's Data Cloud platform. Built by ArisData—a consultancy that believes Snowflake is a complete Data & AI Platform, not just a warehouse—this accelerator transforms what is traditionally a months-long manual effort into a repeatable, auditable, and largely automated process.

This solution is purpose-built for:
- **Data engineering teams** executing large-scale cloud migrations
- **Consulting firms** delivering Redshift-to-Snowflake projects for clients
- **Enterprise architects** seeking a standardized, repeatable migration methodology
- **Cloud migration specialists** who need to minimize risk and maximize velocity

### The Problem It Solves

Migrating from Redshift to Snowflake typically involves:
- Manually analyzing hundreds of tables and thousands of queries
- Tediously translating Redshift-specific DDL syntax (DISTKEY, SORTKEY, ENCODE) to Snowflake equivalents
- Writing custom scripts to load and validate data with no standardized approach
- Lack of visibility into migration progress and data quality issues

The Migration Accelerator eliminates these pain points by providing a structured, config-driven pipeline that handles schema assessment, DDL translation, data loading, and validation—with full audit logging at every step.

### Built 100% Snowflake-Native

Every component of this accelerator leverages Snowflake's native capabilities:
- **Snowpark** for stored procedure logic and data transformations
- **Cortex AI** for intelligent query classification and complexity analysis
- **Streamlit in Snowflake** for real-time migration dashboards
- **Tasks and Streams** for orchestration and change data capture
- **Dynamic Tables** for incremental data processing

No external tools, no third-party dependencies—just Snowflake.

---

## Architecture Overview

The Migration Accelerator follows a five-phase pipeline architecture, where each phase builds upon the outputs of the previous phase. This sequential approach ensures data integrity and provides clear checkpoints for validation.

```
┌──────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                                                                          │
│   ┌───────────┐      ┌────────────┐      ┌───────┐      ┌───────────┐      ┌────────────┐       │
│   │  ASSESS  │─────▶│ TRANSLATE │─────▶│ LOAD │─────▶│ VALIDATE │─────▶│ DASHBOARD │       │
│   └───────────┘      └────────────┘      └───────┘      └───────────┘      └────────────┘       │
│        │                  │                │                │                │            │
│        ▼                  ▼                ▼                ▼                ▼            │
│   Catalog all        Convert DDL      Move data       Verify data       Visualize        │
│   source tables      and queries      to target       integrity         progress         │
│                                                                                          │
└──────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

### Phase Details and Stored Procedures

| Phase | Snowflake Object | Description |
|-------|------------------|-------------|
| **ASSESSMENT** | `SP_ASSESS_SCHEMA` | Performs a comprehensive scan of the source Redshift schema. Catalogs all tables, columns, data types, and constraints into the `MIGRATION_TABLE_REGISTRY`. Identifies tables that require special handling (e.g., large tables, complex data types like SUPER). Logs assessment metrics including table counts, total columns, and estimated row volumes. |
| **ASSESSMENT** | `SP_PROFILE_QUERIES` | Analyzes historical query patterns from the Redshift query log. Identifies frequently-used tables, complex joins, and query patterns that may require optimization post-migration. Produces a query complexity report to prioritize translation efforts. |
| **TRANSLATION** | `SP_TRANSLATE_DDL` | Converts Redshift DDL statements to Snowflake-compatible syntax. Automatically strips Redshift-specific clauses (DISTKEY, SORTKEY, DISTSTYLE, ENCODE, INTERLEAVED SORTKEY). Maps data types according to the `TRANSLATION_CONFIG` table (e.g., TIMESTAMP → TIMESTAMP_NTZ, SUPER → VARIANT). Generates ready-to-execute CREATE TABLE statements for Snowflake. |
| **TRANSLATION** | `SP_CLASSIFY_QUERIES` | Uses rule-based classification (with optional Cortex AI enhancement) to categorize queries by translation difficulty: **GREEN** (direct translation), **YELLOW** (minor modifications needed), **RED** (significant rewrite required). Enables teams to prioritize manual review efforts on high-complexity queries. |
| **LOAD** | `SP_LOAD_TABLE` | Executes the data load for a single table from staging to target. Handles data type conversions, null handling, and error logging. Supports both full refresh and incremental load patterns depending on table configuration. |
| **LOAD** | `SP_BATCH_LOAD_CONTROLLER` | Orchestrates the parallel loading of multiple tables. Reads the `MIGRATION_TABLE_REGISTRY` to identify tables ready for loading. Manages concurrency, tracks progress, and handles failures gracefully with automatic retry logic. Logs batch-level metrics including tables loaded, rows processed, and elapsed time. |
| **VALIDATION** | `SP_VALIDATE_ROW_COUNTS` | Compares row counts between source (Redshift mirror) and target (Snowflake) tables. Flags any discrepancies that exceed configured thresholds. Records results in `VALIDATION_RESULTS` with PASS/FAIL status for each table. Critical for ensuring no data loss during migration. |
| **VALIDATION** | `SP_VALIDATE_NULL_RATES` | Calculates and compares null percentages for each column between source and target. Detects data quality issues introduced during migration (e.g., improper type conversions causing unexpected nulls). Configurable tolerance thresholds via `VALIDATION_CONFIG`. |
| **DASHBOARD** | `MIGRATION_ACCELERATOR_DASHBOARD` | A Streamlit application providing real-time visibility into migration progress. Displays key metrics: tables migrated, rows loaded, validation pass rates, and error summaries. Enables drill-down into specific table details and audit logs. Accessible directly within Snowsight. |

---

## Repository Structure

The repository follows a clear separation between deployment automation, Snowflake object definitions, and supporting documentation.

```
.
├── .github/
│   └── workflows/
│       └── deploy.yml              # GitHub Actions workflow for CI/CD deployment
│
├── scripts/
│   └── deploy.py                   # Python deployment script using Snowflake connector
│
├── snowflake/
│   ├── config/
│   │   ├── resource_monitor.sql    # Snowflake resource monitor definitions
│   │   └── seed_translation_config.sql  # Initial translation rules (data types, functions)
│   │
│   ├── dashboard/
│   │   └── migration_accelerator_app.py  # Streamlit dashboard source code
│   │
│   ├── databases/
│   │   └── DEV/
│   │       ├── init.sql            # Database and schema creation DDL
│   │       ├── control_layer.sql   # Control tables (MIGRATION_RUN, PIPELINE_RUN_LOG, etc.)
│   │       ├── control_views.sql   # Views over control tables for reporting
│   │       ├── redshift_mirror.sql # External tables pointing to source S3 data
│   │       └── schemas/            # Additional schema-specific objects
│   │
│   ├── procedures/
│   │   ├── assessment/
│   │   │   └── sp_assess_schema.sql       # Schema assessment procedure
│   │   ├── translation/
│   │   │   ├── sp_translate_ddl.sql       # DDL translation procedure
│   │   │   └── sp_classify_queries.sql    # Query classification procedure
│   │   ├── load/
│   │   │   └── sp_load_module.sql         # Data loading procedures
│   │   └── validation/
│   │       └── sp_validation_module.sql   # Validation procedures
│   │
│   ├── warehouses/
│   │   └── init.sql                # Warehouse creation DDL (MIGRATION_WH, etc.)
│   │
│   ├── roles/
│   │   └── (role definitions)      # Custom role hierarchy for migration
│   │
│   └── run/
│       └── end-to-end-run.sql      # Complete pipeline execution script
│
├── docs/
│   └── (additional documentation)
│
├── AGENTS.md                       # Cortex Code agent context and conventions
└── README.md                       # This file
```

### Key Files Explained

| File | Purpose |
|------|---------|
| `deploy.yml` | Defines the GitHub Actions workflow that triggers on pushes to `snowflake/**` and deploys changes to Snowflake |
| `deploy.py` | Python script that connects to Snowflake and executes SQL files in the correct order, handling incremental vs. full deployments |
| `control_layer.sql` | Creates the core control tables: `MIGRATION_RUN`, `MIGRATION_TABLE_REGISTRY`, `PIPELINE_RUN_LOG`, `TRANSLATION_CONFIG`, `VALIDATION_CONFIG`, `VALIDATION_RESULTS` |
| `seed_translation_config.sql` | Populates `TRANSLATION_CONFIG` with Redshift-to-Snowflake mappings for data types, functions, and DDL clauses |
| `end-to-end-run.sql` | A ready-to-execute script that runs the complete migration pipeline from assessment through validation |

---

## Snowflake Environment

The accelerator deploys a complete, isolated environment within your Snowflake account. All objects are created under a single database with purpose-specific schemas, ensuring clean separation of concerns.

### Database

| Object | Name | Description |
|--------|------|-------------|
| Database | `MIGRATION_ACCELERATOR_DEV` | The primary database containing all migration objects. Suffix indicates environment (DEV/PROD). Enables complete environment isolation for testing and production workloads. |

### Schemas

| Schema | Purpose | Key Objects |
|--------|---------|-------------|
| `CONTROL` | **Pipeline orchestration and audit logging.** Contains all configuration tables and execution logs. This is the "brain" of the migration—every procedure reads config from here and writes logs back. | `MIGRATION_RUN`, `MIGRATION_TABLE_REGISTRY`, `PIPELINE_RUN_LOG`, `TRANSLATION_CONFIG`, `VALIDATION_CONFIG` |
| `ASSESSMENT` | **Assessment phase outputs.** Stores the results of schema analysis including table inventories, column profiles, and complexity scores. | Assessment result tables, profiling views |
| `TRANSLATION` | **Translated DDL and query mappings.** Contains the Snowflake-compatible DDL generated from Redshift sources, plus query classification results. | Translated DDL storage, query classification results |
| `STAGING` | **Intermediate data landing zone.** Raw data from Redshift lands here before final transformation and loading. Enables data quality checks before committing to target. | Staging tables, external stages |
| `TARGET` | **Final migrated tables.** Production-ready tables with Snowflake-optimized structures. This schema mirrors the source Redshift schema structure. | Migrated production tables |
| `VALIDATION` | **Validation results and reports.** Stores all validation check results with PASS/FAIL status, enabling audit and remediation tracking. | `VALIDATION_RESULTS`, validation report views |
| `APP` | **Streamlit dashboard application.** Contains the Streamlit app and any supporting objects (stages, UDFs) required by the dashboard. | `MIGRATION_ACCELERATOR_DASHBOARD` |
| `REDSHIFT_MIRROR` | **Read-only mirror of source Redshift data.** External tables pointing to the source S3 bucket, enabling Snowflake to query source data directly for validation comparisons. | External tables, S3 stage definitions |

### Warehouses

| Warehouse | Size | Auto-Suspend | Purpose |
|-----------|------|--------------|---------|
| `MIGRATION_WH` | X-Small | 60 seconds | Primary compute for migration workloads including assessment, translation, and data loading. Sized for cost efficiency during POC; scale up for production volumes. |
| `VALIDATION_WH` | X-Small | 60 seconds | Dedicated compute for validation queries. Separated from migration warehouse to prevent resource contention and enable independent scaling during heavy validation periods. |
| `APP_WH` | X-Small | 60 seconds | Powers the Streamlit dashboard. Isolated to ensure dashboard responsiveness is not impacted by migration workloads. |

---

## How to Run the Pipeline

This section provides step-by-step instructions for executing a complete migration. Each step includes the SQL command to run and explains what happens behind the scenes.

### Prerequisites

Before running the pipeline, ensure:
1. All Snowflake objects have been deployed (database, schemas, warehouses, procedures)
2. The `TRANSLATION_CONFIG` table has been seeded with translation rules
3. Source data is accessible via the `REDSHIFT_MIRROR` schema
4. You have the `SYSADMIN` role (or equivalent privileges)

### Step 1 — Create a New Migration Run

Every migration execution is tracked by a parent record in `MIGRATION_RUN`. This provides a unique `RUN_ID` that links all related logs and results together.

```sql
-- Insert a new migration run record
INSERT INTO MIGRATION_ACCELERATOR_DEV.CONTROL.MIGRATION_RUN (RUN_NAME, STATUS)
VALUES ('TICKIT Migration - March 2026', 'STARTED');

-- Retrieve the auto-generated RUN_ID for use in subsequent steps
SELECT RUN_ID, RUN_NAME, CREATED_AT 
FROM MIGRATION_ACCELERATOR_DEV.CONTROL.MIGRATION_RUN
WHERE RUN_NAME = 'TICKIT Migration - March 2026';
```

**What happens:** A new row is inserted with status `STARTED`. The `RUN_ID` (typically a UUID or auto-increment) becomes the correlation key for all pipeline steps.

### Step 2 — Assess the Source Schema

The assessment phase scans the source schema and builds an inventory of all tables to migrate.

```sql
CALL MIGRATION_ACCELERATOR_DEV.ASSESSMENT.SP_ASSESS_SCHEMA('<YOUR_RUN_ID>');
```

**What happens:**
- Queries `REDSHIFT_MIRROR` information schema to catalog all tables
- Populates `MIGRATION_TABLE_REGISTRY` with one row per table
- Records column counts, estimated row counts, and data type complexity
- Logs `STARTED` and `SUCCESS`/`FAILED` entries to `PIPELINE_RUN_LOG`

**Verify results:**
```sql
SELECT * FROM MIGRATION_ACCELERATOR_DEV.CONTROL.MIGRATION_TABLE_REGISTRY
WHERE RUN_ID = '<YOUR_RUN_ID>';
```

### Step 3 — Translate DDL to Snowflake

Converts all Redshift CREATE TABLE statements to Snowflake-compatible syntax.

```sql
CALL MIGRATION_ACCELERATOR_DEV.TRANSLATION.SP_TRANSLATE_DDL('<YOUR_RUN_ID>');
```

**What happens:**
- Reads source DDL from `REDSHIFT_MIRROR` or stored DDL repository
- Applies translation rules from `TRANSLATION_CONFIG`:
  - Strips DISTKEY, SORTKEY, DISTSTYLE, ENCODE clauses
  - Maps data types (TIMESTAMP → TIMESTAMP_NTZ, SUPER → VARIANT, etc.)
  - Converts functions (GETDATE() → CURRENT_TIMESTAMP(), ISNULL() → NVL())
- Stores translated DDL in `TRANSLATION` schema
- Updates `MIGRATION_TABLE_REGISTRY` with translation status

**Verify results:**
```sql
SELECT TABLE_NAME, TRANSLATION_STATUS, TRANSLATED_DDL
FROM MIGRATION_ACCELERATOR_DEV.TRANSLATION.TRANSLATED_OBJECTS
WHERE RUN_ID = '<YOUR_RUN_ID>';
```

### Step 4 — Classify Queries by Complexity

Analyzes queries to determine migration effort required.

```sql
CALL MIGRATION_ACCELERATOR_DEV.TRANSLATION.SP_CLASSIFY_QUERIES('<YOUR_RUN_ID>');
```

**What happens:**
- Reads historical queries from query log or provided query inventory
- Classifies each query into complexity buckets:
  - **GREEN**: Direct translation, no manual intervention needed
  - **YELLOW**: Minor modifications required (e.g., function replacements)
  - **RED**: Significant rewrite needed (e.g., Redshift-specific features)
- Stores classification results for reporting and prioritization

**Verify results:**
```sql
SELECT COMPLEXITY_LEVEL, COUNT(*) AS QUERY_COUNT
FROM MIGRATION_ACCELERATOR_DEV.TRANSLATION.QUERY_CLASSIFICATION
WHERE RUN_ID = '<YOUR_RUN_ID>'
GROUP BY COMPLEXITY_LEVEL;
```

### Step 5 — Load Data into Target Tables

Executes the bulk data movement from staging to target.

```sql
CALL MIGRATION_ACCELERATOR_DEV.STAGING.SP_BATCH_LOAD_CONTROLLER('<YOUR_RUN_ID>');
```

**What happens:**
- Reads `MIGRATION_TABLE_REGISTRY` to identify tables ready for loading
- Creates target tables in `TARGET` schema using translated DDL
- Loads data from `STAGING` (or directly from `REDSHIFT_MIRROR`) into target
- Tracks progress: tables attempted, rows loaded, errors encountered
- Logs comprehensive metrics to `PIPELINE_RUN_LOG`

**Monitor progress:**
```sql
SELECT STEP_NAME, STATUS, ROWS_AFFECTED, DURATION_SECONDS
FROM MIGRATION_ACCELERATOR_DEV.CONTROL.PIPELINE_RUN_LOG
WHERE RUN_ID = '<YOUR_RUN_ID>' AND STEP_NAME LIKE '%LOAD%'
ORDER BY CREATED_AT DESC;
```

### Step 6 — Validate Row Counts

Ensures no data loss by comparing source and target row counts.

```sql
CALL MIGRATION_ACCELERATOR_DEV.VALIDATION.SP_VALIDATE_ROW_COUNTS('<YOUR_RUN_ID>');
```

**What happens:**
- For each migrated table, counts rows in both source (`REDSHIFT_MIRROR`) and target (`TARGET`)
- Compares counts against configured tolerance thresholds
- Records PASS/FAIL results in `VALIDATION_RESULTS`
- Flags any tables exceeding variance thresholds for investigation

**Review validation results:**
```sql
SELECT TABLE_NAME, SOURCE_COUNT, TARGET_COUNT, VARIANCE_PCT, STATUS
FROM MIGRATION_ACCELERATOR_DEV.VALIDATION.VALIDATION_RESULTS
WHERE RUN_ID = '<YOUR_RUN_ID>' AND CHECK_TYPE = 'ROW_COUNT';
```

### Step 7 — Validate Null Rates per Table

Ensures data quality by comparing null percentages across columns.

```sql
-- Run for each table requiring detailed validation
CALL MIGRATION_ACCELERATOR_DEV.VALIDATION.SP_VALIDATE_NULL_RATES('<YOUR_RUN_ID>', 'USERS');
CALL MIGRATION_ACCELERATOR_DEV.VALIDATION.SP_VALIDATE_NULL_RATES('<YOUR_RUN_ID>', 'SALES');
CALL MIGRATION_ACCELERATOR_DEV.VALIDATION.SP_VALIDATE_NULL_RATES('<YOUR_RUN_ID>', 'EVENT');
```

**What happens:**
- Calculates null percentage for every column in the specified table
- Compares source vs. target null rates
- Detects anomalies (e.g., columns that gained unexpected nulls during type conversion)
- Records column-level results in `VALIDATION_RESULTS`

**Review null rate validation:**
```sql
SELECT TABLE_NAME, COLUMN_NAME, SOURCE_NULL_PCT, TARGET_NULL_PCT, STATUS
FROM MIGRATION_ACCELERATOR_DEV.VALIDATION.VALIDATION_RESULTS
WHERE RUN_ID = '<YOUR_RUN_ID>' AND CHECK_TYPE = 'NULL_RATE';
```

### Step 8 — Open the Migration Dashboard

Access the real-time migration dashboard for visual monitoring and reporting.

**Navigation:** Snowsight → Projects → Streamlit → `MIGRATION_ACCELERATOR_DASHBOARD`

**Dashboard Features:**
- **Overview Panel**: Total tables, rows migrated, overall progress percentage
- **Phase Progress**: Visual status of each pipeline phase (Assessment → Validation)
- **Validation Summary**: Pass/fail rates, tables requiring attention
- **Audit Log Viewer**: Searchable, filterable view of `PIPELINE_RUN_LOG`
- **Table Details**: Drill-down into specific table metrics and issues

---

## CI/CD Pipeline

The accelerator includes a fully automated CI/CD pipeline using GitHub Actions, enabling teams to deploy changes to Snowflake with confidence.

### How It Works

```
┌──────────────┐      ┌────────────────────┐     ┌──────────────││───┐
│   Developer │      │  GitHub Actions │     │    Snowflake    │
│   commits   │─────▶│  deploy.yml     │─────▶│    Account     │
│   to main   │      │  triggers       │     │    (DEV/PROD)   │
└──────────────┘      └────────────────────┘     └────────────────││─┘
```

### Deployment Behavior

| Trigger | Behavior | Use Case |
|---------|----------|----------|
| **Push to `snowflake/**`** | Incremental deployment—only changed files are executed | Day-to-day development, bug fixes, procedure updates |
| **Manual `workflow_dispatch`** | Full deployment—all SQL files are executed in order | Initial environment setup, disaster recovery, environment refresh |

### The `deploy.py` Script

The Python deployment script (`scripts/deploy.py`) handles:
- **Connection management**: Connects to Snowflake using credentials from GitHub Secrets
- **Change detection**: Identifies which files changed in the commit (for incremental deploys)
- **Execution ordering**: Runs SQL files in the correct dependency order (databases → schemas → tables → procedures)
- **Error handling**: Logs failures and provides clear error messages
- **Idempotency**: Uses `CREATE OR REPLACE` and `CREATE IF NOT EXISTS` patterns

### Required GitHub Secrets

Configure these secrets in your GitHub repository settings (Settings → Secrets and variables → Actions):

| Secret | Description | Example |
|--------|-------------|---------|
| `SNOWFLAKE_ACCOUNT` | Your Snowflake account identifier | `xy12345.us-east-1` |
| `SNOWFLAKE_USER` | Service account username for deployments | `GITHUB_DEPLOY_USER` |
| `SNOWFLAKE_PASSWORD` | Service account password | `(secure password)` |
| `SNOWFLAKE_ROLE` | Role to use for deployment operations | `SYSADMIN` |

### Security Best Practices

- Use a dedicated service account for CI/CD (not personal credentials)
- Grant minimal required privileges to the deployment role
- Rotate credentials regularly
- Consider using key-pair authentication for production

---

## Design Principles

The Migration Accelerator is built on four core design principles that ensure maintainability, scalability, and reliability.

### 1. Open/Closed Principle

**"Software entities should be open for extension but closed for modification."**

The accelerator achieves this through config-driven behavior:

- **Adding new data type mappings**: Insert a row into `TRANSLATION_CONFIG`—no procedure changes required
- **Adding new validation checks**: Insert a row into `VALIDATION_CONFIG`—no procedure changes required
- **Supporting new source systems**: Add translation rules for the new system's syntax

```sql
-- Example: Adding support for a new Redshift data type
INSERT INTO MIGRATION_ACCELERATOR_DEV.CONTROL.TRANSLATION_CONFIG 
(SOURCE_PATTERN, TARGET_REPLACEMENT, RULE_TYPE)
VALUES ('GEOMETRY', 'GEOGRAPHY', 'DATA_TYPE');
```

**Benefit**: New requirements are met with configuration, not code changes. This reduces risk and testing effort.

### 2. Snowflake-Native First

**"Leverage the platform, don't fight it."**

Every component uses native Snowflake capabilities:

| Capability | Snowflake Feature Used |
|------------|------------------------|
| Stored procedures | Snowpark (Python/Java/Scala) or JavaScript |
| Orchestration | Tasks and DAGs |
| Change tracking | Streams and Dynamic Tables |
| AI/ML | Cortex AI functions |
| Dashboards | Streamlit in Snowflake |
| External data | External tables and stages |

**Benefit**: No external dependencies to manage, upgrade, or secure. Everything runs inside your Snowflake account.

### 3. Audit Trail

**"If it's not logged, it didn't happen."**

Every stored procedure follows a mandatory logging pattern:

```sql
-- At procedure start
INSERT INTO CONTROL.PIPELINE_RUN_LOG (RUN_ID, STEP_NAME, STATUS, MESSAGE)
VALUES (:run_id, 'SP_ASSESS_SCHEMA', 'STARTED', 'Beginning schema assessment');

-- At procedure end (success)
INSERT INTO CONTROL.PIPELINE_RUN_LOG (RUN_ID, STEP_NAME, STATUS, MESSAGE, ROWS_AFFECTED)
VALUES (:run_id, 'SP_ASSESS_SCHEMA', 'SUCCESS', 'Assessment complete', :row_count);

-- At procedure end (failure)
INSERT INTO CONTROL.PIPELINE_RUN_LOG (RUN_ID, STEP_NAME, STATUS, MESSAGE, ERROR_DETAILS)
VALUES (:run_id, 'SP_ASSESS_SCHEMA', 'FAILED', 'Assessment failed', :error_message);
```

**Benefit**: Complete visibility into what happened, when, and why. Essential for debugging, compliance, and client reporting.

### 4. Separation of Concerns

**"Do one thing and do it well."**

Each procedure has a single, focused responsibility:

| Procedure | Single Responsibility |
|-----------|----------------------|
| `SP_ASSESS_SCHEMA` | Catalog source tables—nothing else |
| `SP_TRANSLATE_DDL` | Convert DDL syntax—nothing else |
| `SP_LOAD_TABLE` | Move data for one table—nothing else |
| `SP_VALIDATE_ROW_COUNTS` | Compare counts—nothing else |

**Benefit**: Easier to test, debug, and maintain. Procedures can be run independently or replaced without affecting others.

---

## Sample Data

The accelerator includes a complete sample dataset for demonstration and testing purposes.

### TICKIT Dataset

The **TICKIT** dataset is AWS's standard sample database, representing a fictional ticket sales company. It provides a realistic, multi-table schema that exercises common migration scenarios.

**Source Location:** `s3://awssampledbuswest2/tickit/`

### Dataset Details

| Table | Description | Row Count | Key Columns |
|-------|-------------|-----------|-------------|
| `USERS` | Customer information | ~49,990 | userid, username, firstname, lastname, city, state |
| `VENUE` | Event venue locations | ~202 | venueid, venuename, venuecity, venuestate, venueseats |
| `CATEGORY` | Event categories | ~11 | catid, catgroup, catname, catdesc |
| `DATE` | Calendar dimension | ~365 | dateid, caldate, day, week, month, year, holiday |
| `EVENT` | Scheduled events | ~8,798 | eventid, venueid, catid, dateid, eventname, starttime |
| `LISTING` | Ticket listings for sale | ~192,497 | listid, sellerid, eventid, numtickets, priceperticket |
| `SALES` | Completed ticket sales | ~172,456 | salesid, listid, buyerid, saletime, qtysold, pricepaid |

**Total Volume:** ~624,000 rows across 7 tables

### Why TICKIT?

- **Realistic complexity**: Multiple tables with foreign key relationships
- **Varied data types**: Integers, strings, timestamps, decimals
- **Common patterns**: Fact tables (SALES), dimension tables (DATE, VENUE), bridge tables (LISTING)
- **Public availability**: No authentication required, freely accessible

### Loading Sample Data

The sample data is loaded into the `REDSHIFT_MIRROR` schema via external tables pointing to the S3 bucket:

```sql
-- Example external table definition (created by redshift_mirror.sql)
CREATE OR REPLACE EXTERNAL TABLE REDSHIFT_MIRROR.USERS (
    userid INT,
    username VARCHAR(100),
    ...
)
LOCATION = @TICKIT_STAGE/users/
FILE_FORMAT = (TYPE = CSV);
```

---

## Future Enhancements

The accelerator is designed for extensibility. Below are planned enhancements organized by implementation tier.

### Tier 2 — Near-Term Enhancements

These items build on the existing foundation with moderate effort:

| Enhancement | Description | Benefit |
|-------------|-------------|---------|
| **SchemaChange Integration** | Version control for Snowflake DDL using the SchemaChange framework | Track DDL history, enable rollbacks, audit schema evolution |
| **Pluggable Data Quality Framework** | Extensible validation framework supporting custom checks beyond row counts and null rates | Referential integrity, business rule validation, statistical profiling |
| **Cost Monitoring & Alerting** | Resource monitors with Slack/email alerts for warehouse spend | Budget control, anomaly detection, cost allocation |
| **Terraform Modules** | Infrastructure as Code for all Snowflake objects | Repeatable deployments, environment parity, disaster recovery |

### Tier 3 — Long-Term Vision

These items represent significant enhancements for enterprise scale:

| Enhancement | Description | Benefit |
|-------------|-------------|---------|
| **Comprehensive Test Suites** | pytest for procedures, dbt tests for data quality, integration tests | Confidence in changes, regression prevention |
| **Multi-Cloud Source Support** | Extend beyond Redshift to Azure Synapse, Google BigQuery, Oracle | Single framework for any warehouse migration |
| **Live Redshift Connection** | Direct connection to Redshift Serverless for real-time data access | Eliminate S3 staging, support CDC patterns |
| **Automated Rollback** | Transaction-based rollback with point-in-time recovery | Risk mitigation, faster incident response |
| **Query Performance Benchmarking** | Automated comparison of query performance (Redshift vs. Snowflake) | Validate migration success, identify optimization opportunities |
| **Self-Service Portal** | Web UI for business users to initiate and monitor migrations | Reduce dependency on engineering team |

---

## Support and Contributing

For questions, issues, or contributions:

1. **Issues**: Open a GitHub issue describing the problem or enhancement
2. **Pull Requests**: Fork the repository, make changes, and submit a PR
3. **Documentation**: Improvements to this README or inline documentation are welcome

---

*Built with* ❄️ *by ArisData — Snowflake is a complete Data & AI Platform, not just a warehouse.*
