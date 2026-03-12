import streamlit as st
from snowflake.snowpark.context import get_active_session

st.set_page_config(
    page_title="ArisData — Migration Accelerator",
    layout="wide"
)

session = get_active_session()

st.sidebar.title("ArisData")
st.sidebar.caption("Redshift → Snowflake Accelerator")

run_options_df = session.sql("""
    SELECT RUN_ID, RUN_NAME 
    FROM MIGRATION_ACCELERATOR_DEV.CONTROL.MIGRATION_RUN
    ORDER BY STARTED_AT DESC
""").to_pandas()

if not run_options_df.empty:
    run_options_df["DISPLAY"] = run_options_df["RUN_NAME"] + " (" + run_options_df["RUN_ID"] + ")"
    selected_display = st.sidebar.selectbox("Select Migration Run", run_options_df["DISPLAY"].tolist())
    selected_run_id = run_options_df[run_options_df["DISPLAY"] == selected_display]["RUN_ID"].values[0]
else:
    st.sidebar.warning("No migration runs found")
    selected_run_id = None

st.header("Migration Overview")

if selected_run_id:
    total_tables = session.sql(f"""
        SELECT COUNT(*) AS CNT 
        FROM MIGRATION_ACCELERATOR_DEV.CONTROL.MIGRATION_TABLE_REGISTRY
        WHERE RUN_ID = '{selected_run_id}'
    """).collect()[0]["CNT"]

    loaded_tables = session.sql(f"""
        SELECT COUNT(*) AS CNT 
        FROM MIGRATION_ACCELERATOR_DEV.CONTROL.MIGRATION_TABLE_REGISTRY
        WHERE RUN_ID = '{selected_run_id}' AND STATUS IN ('LOADED', 'VALIDATED')
    """).collect()[0]["CNT"]

    validated_tables = session.sql(f"""
        SELECT COUNT(*) AS CNT 
        FROM MIGRATION_ACCELERATOR_DEV.CONTROL.MIGRATION_TABLE_REGISTRY
        WHERE RUN_ID = '{selected_run_id}' AND STATUS = 'VALIDATED'
    """).collect()[0]["CNT"]

    validation_rate_result = session.sql(f"""
        SELECT 
            CASE WHEN COUNT(*) > 0 
                 THEN ROUND(COUNT_IF(STATUS = 'PASS') / COUNT(*) * 100, 1)
                 ELSE 0 
            END AS PASS_RATE
        FROM MIGRATION_ACCELERATOR_DEV.CONTROL.VALIDATION_RESULTS
        WHERE RUN_ID = '{selected_run_id}'
    """).collect()[0]["PASS_RATE"]
    validation_pass_rate = float(validation_rate_result) if validation_rate_result else 0.0

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Tables", total_tables)
    col2.metric("Loaded", loaded_tables)
    col3.metric("Validated", validated_tables)
    col4.metric("Validation Pass Rate", f"{validation_pass_rate}%")

    table_status_df = session.sql(f"""
        SELECT SOURCE_TABLE, TARGET_TABLE, STATUS
        FROM MIGRATION_ACCELERATOR_DEV.CONTROL.MIGRATION_TABLE_REGISTRY
        WHERE RUN_ID = '{selected_run_id}'
        ORDER BY SOURCE_TABLE
    """).to_pandas()

    st.dataframe(table_status_df, use_container_width=True)
else:
    st.info("Please select a migration run from the sidebar to view details.")


#
st.header("Validation Results")

tab1, tab2 = st.tabs(["Row Count Checks", "Null Rate Checks"])

with tab1:
    row_count_df = session.sql(f"""
        SELECT
            SPLIT_PART(CHECK_NAME, ':', 2) AS TABLE_NAME,
            SOURCE_VALUE::VARCHAR AS SOURCE_ROWS,
            TARGET_VALUE::VARCHAR AS TARGET_ROWS,
            VARIANCE_PCT,
            STATUS
        FROM MIGRATION_ACCELERATOR_DEV.CONTROL.VALIDATION_RESULTS
        WHERE RUN_ID = '{selected_run_id}'
        AND CHECK_NAME LIKE 'ROW_COUNT:%'
        ORDER BY TABLE_NAME
    """).to_pandas()

    st.dataframe(row_count_df, use_container_width=True)

    passed_rc = len(row_count_df[row_count_df["STATUS"] == "PASS"])
    failed_rc = len(row_count_df[row_count_df["STATUS"] == "FAIL"])

    rc_col1, rc_col2 = st.columns(2)
    rc_col1.metric("Passed", passed_rc)
    if failed_rc > 0:
        rc_col2.metric("Failed", failed_rc, delta=failed_rc, delta_color="inverse")
    else:
        rc_col2.metric("Failed", failed_rc)

with tab2:
    null_tables_df = session.sql(f"""
        SELECT DISTINCT SPLIT_PART(SPLIT_PART(CHECK_NAME, ':', 2), '.', 1) AS TABLE_NAME
        FROM MIGRATION_ACCELERATOR_DEV.CONTROL.VALIDATION_RESULTS
        WHERE RUN_ID = '{selected_run_id}'
        AND CHECK_NAME LIKE 'NULL_RATE:%'
        ORDER BY TABLE_NAME
    """).to_pandas()

    if not null_tables_df.empty:
        selected_table = st.selectbox("Select Table", null_tables_df["TABLE_NAME"].tolist())

        null_rate_df = session.sql(f"""
            SELECT
                SPLIT_PART(SPLIT_PART(CHECK_NAME, ':', 2), '.', 2) AS COLUMN_NAME,
                TARGET_VALUE::VARCHAR AS NULL_RATE,
                VARIANCE_PCT,
                STATUS
            FROM MIGRATION_ACCELERATOR_DEV.CONTROL.VALIDATION_RESULTS
            WHERE RUN_ID = '{selected_run_id}'
            AND CHECK_NAME LIKE 'NULL_RATE:{selected_table}.%'
            ORDER BY VARIANCE_PCT DESC
        """).to_pandas()

        st.dataframe(null_rate_df, use_container_width=True)

        passed_nr = len(null_rate_df[null_rate_df["STATUS"] == "PASS"])
        failed_nr = len(null_rate_df[null_rate_df["STATUS"] == "FAIL"])

        nr_col1, nr_col2 = st.columns(2)
        nr_col1.metric("Passed", passed_nr)
        if failed_nr > 0:
            nr_col2.metric("Failed", failed_nr, delta=failed_nr, delta_color="inverse")
        else:
            nr_col2.metric("Failed", failed_nr)
    else:
        st.info("No null rate checks found for this run.")

#
st.header("Translation Summary")

tab3, tab4 = st.tabs(["DDL Translation", "Query Classification"])

with tab3:
    ddl_df = session.sql(f"""
        SELECT
            SOURCE_TABLE,
            LEFT(ORIGINAL_DDL, 120) AS ORIGINAL_DDL_PREVIEW,
            LEFT(TRANSLATED_DDL, 120) AS TRANSLATED_DDL_PREVIEW,
            TRANSLATED_AT
        FROM MIGRATION_ACCELERATOR_DEV.TRANSLATION.DDL_TRANSLATION_LOG
        WHERE RUN_ID = '{selected_run_id}'
        ORDER BY SOURCE_TABLE
    """).to_pandas()

    st.dataframe(ddl_df, use_container_width=True)

    st.metric("Total DDL Translated", len(ddl_df))

with tab4:
    qc_col1, qc_col2 = st.columns(2)

    with qc_col1:
        st.subheader("Queries by Category")
        category_df = session.sql(f"""
            SELECT
                QUERY_CATEGORY,
                COUNT(*) AS QUERY_COUNT
            FROM MIGRATION_ACCELERATOR_DEV.TRANSLATION.QUERY_CLASSIFICATION_LOG
            WHERE RUN_ID = '{selected_run_id}'
            GROUP BY QUERY_CATEGORY
            ORDER BY QUERY_COUNT DESC
        """).to_pandas()
        st.dataframe(category_df, use_container_width=True)

    with qc_col2:
        st.subheader("Confidence Distribution")
        confidence_df = session.sql(f"""
            SELECT
                CONFIDENCE,
                COUNT(*) AS COUNT
            FROM MIGRATION_ACCELERATOR_DEV.TRANSLATION.QUERY_CLASSIFICATION_LOG
            WHERE RUN_ID = '{selected_run_id}'
            GROUP BY CONFIDENCE
            ORDER BY CONFIDENCE
        """).to_pandas()
        st.dataframe(confidence_df, use_container_width=True)

    st.subheader("All Classified Queries")
    all_queries_df = session.sql(f"""
        SELECT
            QUERY_CATEGORY,
            CONFIDENCE,
            LEFT(QUERY_TEXT, 100) AS QUERY_PREVIEW,
            CLASSIFIED_AT
        FROM MIGRATION_ACCELERATOR_DEV.TRANSLATION.QUERY_CLASSIFICATION_LOG
        WHERE RUN_ID = '{selected_run_id}'
        ORDER BY QUERY_CATEGORY
    """).to_pandas()
    st.dataframe(all_queries_df, use_container_width=True)

#
st.header("Pipeline Log")

total_steps = session.sql(f"""
    SELECT COUNT(*) AS CNT
    FROM MIGRATION_ACCELERATOR_DEV.CONTROL.PIPELINE_RUN_LOG
    WHERE RUN_ID = '{selected_run_id}'
""").collect()[0]["CNT"]

successful_steps = session.sql(f"""
    SELECT COUNT(*) AS CNT
    FROM MIGRATION_ACCELERATOR_DEV.CONTROL.PIPELINE_RUN_LOG
    WHERE RUN_ID = '{selected_run_id}' AND STATUS = 'SUCCESS'
""").collect()[0]["CNT"]

failed_steps = session.sql(f"""
    SELECT COUNT(*) AS CNT
    FROM MIGRATION_ACCELERATOR_DEV.CONTROL.PIPELINE_RUN_LOG
    WHERE RUN_ID = '{selected_run_id}' AND STATUS = 'FAILED'
""").collect()[0]["CNT"]

total_rows_result = session.sql(f"""
    SELECT COALESCE(SUM(ROWS_PROCESSED), 0) AS TOTAL_ROWS
    FROM MIGRATION_ACCELERATOR_DEV.CONTROL.PIPELINE_RUN_LOG
    WHERE RUN_ID = '{selected_run_id}'
""").collect()[0]["TOTAL_ROWS"]
total_rows = int(total_rows_result) if total_rows_result else 0

pl_col1, pl_col2, pl_col3, pl_col4 = st.columns(4)
pl_col1.metric("Total Steps", total_steps)
pl_col2.metric("Successful Steps", successful_steps)
if failed_steps > 0:
    pl_col3.metric("Failed Steps", failed_steps, delta=failed_steps, delta_color="inverse")
else:
    pl_col3.metric("Failed Steps", failed_steps)
pl_col4.metric("Total Rows Processed", total_rows)

phases_df = session.sql(f"""
    SELECT DISTINCT PHASE
    FROM MIGRATION_ACCELERATOR_DEV.CONTROL.PIPELINE_RUN_LOG
    WHERE RUN_ID = '{selected_run_id}'
    ORDER BY PHASE
""").to_pandas()

all_phases = phases_df["PHASE"].tolist() if not phases_df.empty else []
selected_phases = st.multiselect("Filter by Phase", all_phases, default=all_phases)

if selected_phases:
    phases_str = ",".join([f"'{p}'" for p in selected_phases])
    log_df = session.sql(f"""
        SELECT
            PHASE,
            STEP_NAME,
            STATUS,
            ROWS_PROCESSED,
            LOGGED_AT,
            ERROR_MESSAGE
        FROM MIGRATION_ACCELERATOR_DEV.CONTROL.PIPELINE_RUN_LOG
        WHERE RUN_ID = '{selected_run_id}'
        AND PHASE IN ({phases_str})
        ORDER BY LOGGED_AT ASC
    """).to_pandas()

    st.dataframe(log_df, use_container_width=True)

    failed_df = log_df[log_df["STATUS"] == "FAILED"]
    if not failed_df.empty:
        st.warning("The following steps failed:")
        for _, row in failed_df.iterrows():
            st.write(f"- **{row['PHASE']}** / {row['STEP_NAME']}: {row['ERROR_MESSAGE']}")
    else:
        st.success("All pipeline steps completed successfully.")
else:
    st.info("Select at least one phase to view logs.")

st.divider()
st.caption("ArisData Migration Accelerator — POC Dashboard")