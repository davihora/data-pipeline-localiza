"""
Airflow DAG — transactions_pipeline

Topology (dependency order):

  Local execution (SQS_QUEUE_URL not set):
    validate_raw_input → ingest → clean → dq_check → [transform_table1, transform_table2] → export_results

  AWS / event-driven execution (SQS_QUEUE_URL set):
    sqs_listener → validate_raw_input → ingest → clean → dq_check
                → [transform_table1, transform_table2] → export_results → trigger_next_run
         ▲                                                                        │
         └────────────────────── new DAG run ◄──────────────────────────────────┘

  The DAG is self-perpetuating in AWS: after each successful run it re-triggers
  itself so the next run immediately parks at sqs_listener waiting for the next
  S3 event. No Lambda, no external scheduler — just Airflow.

  S3 event notifications must be configured to send messages to the SQS queue
  (S3 → SQS, prefix "raw/", suffix ".csv").

All paths are read from environment variables injected via docker-compose / MWAA,
so the DAG is portable across local and remote deployments.
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

# SQS_QUEUE_URL is only set in AWS environments.
# When absent the sensor and self-trigger tasks are omitted, keeping local
# execution unchanged.
SQS_QUEUE_URL: str | None = os.getenv("SQS_QUEUE_URL")

# Ensure src/ is on the Python path when Airflow imports the DAG
_SRC_DIR = Path(__file__).parent.parent / "src"
if str(_SRC_DIR) not in sys.path:
    sys.path.insert(0, str(_SRC_DIR))

# ---------------------------------------------------------------------------
# Paths (resolved from env, with sensible defaults for local dev)
# ---------------------------------------------------------------------------
RAW_DIR = Path(os.getenv("PIPELINE_RAW_DIR", "/opt/airflow/data/raw"))
STAGING_DIR = Path(os.getenv("PIPELINE_STAGING_DIR", "/opt/airflow/data/staging"))
OUTPUT_DIR = Path(os.getenv("PIPELINE_OUTPUT_DIR", "/opt/airflow/data/output"))
GX_ROOT = Path(os.getenv("PIPELINE_GX_ROOT", "/opt/airflow/great_expectations"))

RAW_PARQUET = STAGING_DIR / "transactions_raw.parquet"
CLEAN_PARQUET = STAGING_DIR / "transactions_clean.parquet"
TABLE1_PARQUET = OUTPUT_DIR / "table1_region_risk.parquet"
TABLE2_PARQUET = OUTPUT_DIR / "table2_top3_receivers.parquet"

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Default DAG args
# ---------------------------------------------------------------------------
default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": False,
    "email_on_retry": False,
}

# ---------------------------------------------------------------------------
# Task functions
# ---------------------------------------------------------------------------


def task_validate_raw_input(**context):
    from ingestion.reader import validate_raw_inputs

    summary = validate_raw_inputs(RAW_DIR)
    logger.info("Validation summary: %s", json.dumps(summary, indent=2))
    context["task_instance"].xcom_push(key="validation_summary", value=summary)


def task_ingest(**context):
    from ingestion.reader import read_partitions_to_parquet

    RAW_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    metadata = read_partitions_to_parquet(RAW_DIR, RAW_PARQUET)
    logger.info("Ingestion metadata: %s", json.dumps(metadata, indent=2))
    context["task_instance"].xcom_push(key="ingestion_metadata", value=metadata)


def task_clean(**context):
    from cleaning.cleaner import clean_transactions

    CLEAN_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    metrics = clean_transactions(RAW_PARQUET, CLEAN_PARQUET)
    logger.info("Cleaning metrics: %s", json.dumps(metrics, indent=2))
    context["task_instance"].xcom_push(key="cleaning_metrics", value=metrics)


def task_dq_check(**context):
    from quality.dq_suite import run_data_quality

    GX_ROOT.mkdir(parents=True, exist_ok=True)
    report = run_data_quality(
        parquet_path=CLEAN_PARQUET,
        gx_root=GX_ROOT,
        raise_on_failure=True,
    )
    logger.info("DQ report: %s", json.dumps(report["custom_metrics"], indent=2))
    context["task_instance"].xcom_push(key="dq_report", value=report["custom_metrics"])


def task_transform_table1(**context):
    from transforms.aggregations import build_table1_region_risk

    TABLE1_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    result = build_table1_region_risk(CLEAN_PARQUET, TABLE1_PARQUET)
    context["task_instance"].xcom_push(key="table1_result", value=result)


def task_transform_table2(**context):
    from transforms.aggregations import build_table2_top3_receivers

    TABLE2_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    result = build_table2_top3_receivers(CLEAN_PARQUET, TABLE2_PARQUET)
    context["task_instance"].xcom_push(key="table2_result", value=result)


def task_export_results(**context):
    """
    Convert Parquet outputs to CSV for human-readable delivery and
    log the final results summary.
    """
    import duckdb

    con = duckdb.connect()

    for parquet_path, csv_name in [
        (TABLE1_PARQUET, "table1_region_risk.csv"),
        (TABLE2_PARQUET, "table2_top3_receivers.csv"),
    ]:
        csv_path = OUTPUT_DIR / csv_name
        con.execute(
            f"COPY (SELECT * FROM read_parquet('{parquet_path}')) "
            f"TO '{csv_path}' (HEADER, DELIMITER ',')"
        )
        logger.info("Exported %s", csv_path)

    # Log final results to Airflow logs for quick inspection
    logger.info("=== TABLE 1: Regions by avg risk_score (DESC) ===")
    rows = con.execute(
        f"SELECT * FROM read_parquet('{TABLE1_PARQUET}')"
    ).fetchall()
    for row in rows:
        logger.info("  %s", row)

    logger.info("=== TABLE 2: Top 3 receiving addresses by amount (latest sale) ===")
    rows = con.execute(
        f"SELECT * FROM read_parquet('{TABLE2_PARQUET}')"
    ).fetchall()
    for row in rows:
        logger.info("  %s", row)

    con.close()


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="transactions_pipeline",
    description="Ingest, clean, validate and transform blockchain transaction data",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,  # triggered manually (local) or self-triggered via SQS (AWS)
    catchup=False,
    tags=["data-engineering", "transactions", "dq"],
) as dag:

    # -- AWS only: wait for an S3 event notification in SQS -----------------
    if SQS_QUEUE_URL:
        from airflow.providers.amazon.aws.sensors.sqs import SqsSensor
        from airflow.operators.trigger_dagrun import TriggerDagRunOperator

        sqs_listener = SqsSensor(
            task_id="sqs_listener",
            sqs_queue=SQS_QUEUE_URL,
            # Each S3 notification is a JSON envelope; the inner Records[0].s3
            # fields (bucket.name, object.key) are available in XCom as
            # "message_ids" / "messages" for downstream tasks if needed.
            max_messages=1,
            num_batches=1,
            # Poke every 30 s; mode=reschedule releases the worker slot between
            # pokes so the DAG does not hold a worker while idle.
            poke_interval=30,
            mode="reschedule",
            timeout=60 * 60 * 24,  # 24 h safety ceiling
        )

    validate = PythonOperator(
        task_id="validate_raw_input",
        python_callable=task_validate_raw_input,
    )

    ingest = PythonOperator(
        task_id="ingest",
        python_callable=task_ingest,
    )

    clean = PythonOperator(
        task_id="clean",
        python_callable=task_clean,
    )

    dq_check = PythonOperator(
        task_id="dq_check",
        python_callable=task_dq_check,
    )

    transform_t1 = PythonOperator(
        task_id="transform_table1",
        python_callable=task_transform_table1,
    )

    transform_t2 = PythonOperator(
        task_id="transform_table2",
        python_callable=task_transform_table2,
    )

    export = PythonOperator(
        task_id="export_results",
        python_callable=task_export_results,
    )

    # -- AWS only: re-trigger itself so next run parks at sqs_listener ------
    if SQS_QUEUE_URL:
        trigger_next_run = TriggerDagRunOperator(
            task_id="trigger_next_run",
            trigger_dag_id="transactions_pipeline",
            # Reset XCom and sensor state for the new run
            reset_dag_run=False,
            wait_for_completion=False,
        )

    # -- Dependency graph ----------------------------------------------------
    if SQS_QUEUE_URL:
        (
            sqs_listener
            >> validate
            >> ingest
            >> clean
            >> dq_check
            >> [transform_t1, transform_t2]
            >> export
            >> trigger_next_run
        )
    else:
        validate >> ingest >> clean >> dq_check >> [transform_t1, transform_t2] >> export
