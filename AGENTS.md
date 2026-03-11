# ArisData — Redshift to Snowflake Migration Accelerator
## Cortex Code Agent Context (Snowsight + CLI)

---

## Project Identity
- Company: ArisData
- Role: Snowflake Data Engineer
- Goal: Build a reusable, sellable Redshift → Snowflake Migration Accelerator
- ArisData belief: Snowflake is a complete Data & AI Platform, not just a warehouse
- Source data: TICKIT dataset from AWS public S3 (simulates Redshift source)

---

## Snowflake Environment
- DEV Database : MIGRATION_ACCELERATOR_DEV
- Schemas      : CONTROL, ASSESSMENT, TRANSLATION, STAGING, TARGET, VALIDATION, APP
- Warehouses   : MIGRATION_WH, VALIDATION_WH, APP_WH (all X-Small, auto-suspend 60s)
- Role         : SYSADMIN

---

## Design Principles (Non-Negotiable)
1. Open/Closed Principle — new behavior = new config row, never modify existing code
2. Everything Snowflake-native — Cortex, Snowpark, Tasks, Streams, Dynamic Tables
3. No hardcoding — use config tables for all rules and thresholds
4. Every procedure MUST log to CONTROL.PIPELINE_RUN_LOG — no exceptions
5. POC-first — demonstrable and clean, not production-hardened

---

## Naming Conventions
- Procedures : SP_<PHASE>_<ACTION>  e.g. SP_ASSESS_SCHEMA, SP_TRANSLATE_DDL
- Tables     : UPPERCASE_UNDERSCORE
- Views      : V_<NAME>
- Phases     : ASSESSMENT, TRANSLATION, LOAD, VALIDATION

---

## Key Control Tables
- CONTROL.MIGRATION_RUN             — parent record per pipeline run
- CONTROL.MIGRATION_TABLE_REGISTRY  — one row per table to migrate
- CONTROL.PIPELINE_RUN_LOG          — step-by-step audit log (write here always)
- CONTROL.TRANSLATION_CONFIG        — Redshift→Snowflake translation rules
- CONTROL.VALIDATION_CONFIG         — validation check definitions
- CONTROL.VALIDATION_RESULTS        — pass/fail per table per check

---

## Redshift → Snowflake Quick Reference
### Data Type Mappings
| Redshift       | Snowflake      |
|----------------|----------------|
| TIMESTAMP      | TIMESTAMP_NTZ  |
| TIMESTAMPTZ    | TIMESTAMP_TZ   |
| SUPER          | VARIANT        |
| DECIMAL(p,s)   | NUMBER(p,s)    |
| BOOL           | BOOLEAN        |

### DDL Clauses to Strip
- DISTKEY, SORTKEY, DISTSTYLE, ENCODE, INTERLEAVED SORTKEY

### Function Mappings
- GETDATE()  → CURRENT_TIMESTAMP()
- ISNULL(    → NVL(
- TOP N      → LIMIT N

---

## Pipeline Phases and Procedures to Build
1. ASSESSMENT  → SP_ASSESS_SCHEMA, SP_PROFILE_QUERIES
2. TRANSLATION → SP_TRANSLATE_DDL, SP_CLASSIFY_QUERIES  
3. LOAD        → SP_LOAD_TABLE, SP_BATCH_LOAD_CONTROLLER
4. VALIDATION  → SP_VALIDATE_ROW_COUNTS, SP_VALIDATE_NULL_RATES
5. DASHBOARD   → Streamlit app in MIGRATION_ACCELERATOR_DEV.APP schema

---

## Every Stored Procedure Must Follow This Pattern
1. Accept RUN_ID and TABLE_NAME as input parameters
2. Write a STARTED log row to CONTROL.PIPELINE_RUN_LOG at the beginning
3. Execute the core logic
4. Write a SUCCESS or FAILED log row to CONTROL.PIPELINE_RUN_LOG at the end
5. Use CONTROL.TRANSLATION_CONFIG or CONTROL.VALIDATION_CONFIG for all rules
   — never hardcode values inside the procedure body
```

---

## Updated Workflow — Snowsight Only, Skills Fully Leveraged
```
┌──────────────────────────────────────────────────────────┐
│                                                          │
│  1. Snowsight Workspace → open/create a .sql file        │
│                                                          │
│  2. Click the Cortex Code star icon                      │
│     AGENTS.md auto-loads — full project context ready    │
│                                                          │
│  3. Inject live table schema:                            │
│     #MIGRATION_ACCELERATOR_DEV.CONTROL.PIPELINE_RUN_LOG  │
│                                                          │
│  4. Natural language prompt:                             │
│     "Generate SP_ASSESS_SCHEMA following the design      │
│      principles and logging pattern in AGENTS.md"        │
│                                                          │
│  5. Review output in Snowsight diff view                 │
│                                                          │
│  6. Copy → paste into VS Code → save as .sql in repo     │
│                                                          │
│  7. git push → GitHub Actions deploys automatically      │
│                                                          │
└──────────────────────────────────────────────────────────┘