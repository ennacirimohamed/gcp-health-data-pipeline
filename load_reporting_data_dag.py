from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

COUNTRIES = ['Canada', 'France', 'USA', 'India', 'Italy','Japan']
PROJECT_ID = 'wired-effect-467812-k2'
STAGING_DATASET = 'stagingdataset'
TRANSFORM_DATASET = 'transformdataset'
REPORTING_DATASET = 'reportingdataset'

with DAG(
    dag_id='load-reporting-data',
    default_args=default_args,
    description='Load a CSV file from GCS to BigQuery and create country-specific tables and views',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['bigquery', 'gcs', 'csv'],
) as dag:

    check_file_exists = GCSObjectExistenceSensor(
        task_id='check_file_exists',
        bucket='global-data-storage',
        object='global_health_data.csv',
        timeout=300,
        poke_interval=30,
        mode='poke',
        google_cloud_conn_id='google_cloud_default',
    )

    load_csv_to_bigquery = GCSToBigQueryOperator(
        task_id='load_csv_to_bq',
        bucket='global-data-storage',
        source_objects=['global_health_data.csv'],
        destination_project_dataset_table=f'{PROJECT_ID}.{STAGING_DATASET}.global_data',
        source_format='CSV',
        allow_jagged_rows=True,
        ignore_unknown_values=True,
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        field_delimiter=',',
        autodetect=True,
        gcp_conn_id='google_cloud_default',
    )

    check_file_exists >> load_csv_to_bigquery

    for country in COUNTRIES:
        table_id = country.lower().replace(" ", "_") + '_data'
        view_id = country.lower().replace(" ", "_") + '_view'

        sql_create_table = f"""
        CREATE OR REPLACE TABLE `{PROJECT_ID}.{TRANSFORM_DATASET}.{table_id}` AS
        SELECT * FROM `{PROJECT_ID}.{STAGING_DATASET}.global_data`
        WHERE LOWER(country) = '{country.lower()}'
        """

        sql_create_view = f"""
        CREATE OR REPLACE VIEW `{PROJECT_ID}.{REPORTING_DATASET}.{view_id}` AS
        SELECT 
            Country,
            Year,
            `Disease Name`,
            `Disease Category`,
            `Prevalence Rate`
        FROM `{PROJECT_ID}.{TRANSFORM_DATASET}.{table_id}`
        """

        transform_task = BigQueryInsertJobOperator(
            task_id=f'transform_{country.lower()}',
            configuration={
                "query": {
                    "query": sql_create_table,
                    "useLegacySql": False,
                }
            },
            gcp_conn_id='google_cloud_default',
        )

        view_task = BigQueryInsertJobOperator(
            task_id=f'create_view_{country.lower()}',
            configuration={
                "query": {
                    "query": sql_create_view,
                    "useLegacySql": False,
                }
            },
            gcp_conn_id='google_cloud_default',
        )

        load_csv_to_bigquery >> transform_task >> view_task
