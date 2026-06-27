import sys
sys.path.insert(0, '/opt/airflow')

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from scripts.extract import extract_daily_sales, extract_historical_transactions
from scripts.transform import (
    clean_and_classify,
    deduplicate_sources,
    assign_categories,
    build_canonical_product_names,
)
from scripts.load import load_dim_date, load_dim_product, load_fact_transactions, load_rejected
from scripts.io_utils import write_intermediate, read_intermediate

default_args = {
    "owner": "DataMart",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


def task_extract_daily(**context):
    df = extract_daily_sales()
    path = write_intermediate(df, context["run_id"], "raw_daily")
    context["ti"].xcom_push(key="raw_daily_path", value=path)


def task_extract_historical(**context):
    df = extract_historical_transactions()
    path = write_intermediate(df, context["run_id"], "raw_historical")
    context["ti"].xcom_push(key="raw_historical_path", value=path)


def task_transform_and_dedup(**context):
    run_id = context["run_id"]
    ti = context["ti"]

    raw_daily = read_intermediate(ti.xcom_pull(task_ids="extract_daily_sales", key="raw_daily_path"))
    raw_historical = read_intermediate(ti.xcom_pull(task_ids="extract_historical_transactions", key="raw_historical_path"))

    valid_daily, rejected_daily = clean_and_classify(raw_daily)
    valid_historical, rejected_historical = clean_and_classify(raw_historical)

    combined_valid = deduplicate_sources(valid_daily, valid_historical)
    combined_valid = assign_categories(combined_valid)

    canonical_products = build_canonical_product_names(combined_valid)

    paths = {
        "combined_valid": write_intermediate(combined_valid, run_id, "combined_valid"),
        "canonical_products": write_intermediate(canonical_products, run_id, "canonical_products"),
        "rejected_daily": write_intermediate(rejected_daily, run_id, "rejected_daily"),
        "rejected_historical": write_intermediate(rejected_historical, run_id, "rejected_historical"),
    }
    for key, path in paths.items():
        ti.xcom_push(key=f"{key}_path", value=path)


def task_load_dimensions(**context):
    from scripts.database import get_engine
    ti = context["ti"]

    combined_valid = read_intermediate(ti.xcom_pull(task_ids="transform_and_dedup", key="combined_valid_path"))
    canonical_products = read_intermediate(ti.xcom_pull(task_ids="transform_and_dedup", key="canonical_products_path"))

    engine = get_engine()
    load_dim_date(combined_valid, engine)
    load_dim_product(canonical_products, engine)


def task_assign_date_id(**context):
    """Asigna date_id (fecha sin hora) a cada transacción, ya con dim_date poblada."""
    ti = context["ti"]
    combined_valid = read_intermediate(ti.xcom_pull(task_ids="transform_and_dedup", key="combined_valid_path"))
    combined_valid["date_id"] = combined_valid["transaction_date_utc"].dt.date

    path = write_intermediate(combined_valid, context["run_id"], "combined_with_date_id")
    ti.xcom_push(key="combined_with_date_id_path", value=path)


def task_load_facts_and_rejected(**context):
    from scripts.database import get_engine
    ti = context["ti"]

    combined = read_intermediate(ti.xcom_pull(task_ids="assign_date_id", key="combined_with_date_id_path"))
    rejected_daily = read_intermediate(ti.xcom_pull(task_ids="transform_and_dedup", key="rejected_daily_path"))
    rejected_historical = read_intermediate(ti.xcom_pull(task_ids="transform_and_dedup", key="rejected_historical_path"))

    engine = get_engine()

    # Idempotencia: cada source_file se borra e inserta de nuevo en su totalidad.
    for source_file in combined["source_file"].unique():
        subset = combined[combined["source_file"] == source_file]
        load_fact_transactions(subset, engine, source_file=source_file)

    load_rejected(rejected_daily, engine, source_file="data_csv")
    load_rejected(rejected_historical, engine, source_file="online_retail_ii")


with DAG(
    dag_id="datamart_retail_pipeline",
    description="Pipeline ETL DataMart S.A.S. — ventas, devoluciones y catálogo",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["etl", "retail", "datamart"],
) as dag:

    extract_daily = PythonOperator(
        task_id="extract_daily_sales",
        python_callable=task_extract_daily,
    )

    extract_historical = PythonOperator(
        task_id="extract_historical_transactions",
        python_callable=task_extract_historical,
    )

    transform_dedup = PythonOperator(
        task_id="transform_and_dedup",
        python_callable=task_transform_and_dedup,
    )

    load_dimensions = PythonOperator(
        task_id="load_dimensions",
        python_callable=task_load_dimensions,
    )

    assign_date_id = PythonOperator(
        task_id="assign_date_id",
        python_callable=task_assign_date_id,
    )

    load_facts = PythonOperator(
        task_id="load_facts_and_rejected",
        python_callable=task_load_facts_and_rejected,
    )

    [extract_daily, extract_historical] >> transform_dedup
    transform_dedup >> load_dimensions >> assign_date_id >> load_facts