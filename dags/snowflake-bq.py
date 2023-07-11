from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

args = {
    'owner': 'packt-developer',
}

with DAG(
    dag_id='snowflake-bq',
    default_args=args,
    schedule_interval=None,
    start_date=days_ago(1),
    max_active_runs=1,
    is_paused_upon_creation=False

) as dag:

    bq_ingestion = GoogleCloudStorageToBigQueryOperator(
        task_id="gcs_parquet_ingestion",
        bucket='cf-spark-external',
        source_format='parquet',
        source_objects=[
            'googl-market-data/*.parquet'],
        destination_project_dataset_table='composer_destination.googl_bq_ingestion',
        schema_fields=[
            {
                "mode": "NULLABLE",
                "name": "symbol",
                "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "datetime",
                "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "tm",
                "type": "INTEGER"
            },
            {
                "mode": "NULLABLE",
                "name": "dt",
                "type": "DATE"
            },
            {
                "mode": "NULLABLE",
                "name": "exchange_code",
                "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "trade_price",
                "type": "FLOAT"
            },
            {
                "mode": "NULLABLE",
                "name": "trade_size",
                "type": "INTEGER"
            },
            {
                "fields": [
                    {
                        "fields": [
                            {
                                "mode": "NULLABLE",
                                "name": "element",
                                "type": "STRING"
                            }
                        ],
                        "mode": "REPEATED",
                        "name": "list",
                        "type": "RECORD"
                    }
                ],
                "mode": "NULLABLE",
                "name": "trade_condition",
                "type": "RECORD"
            },
            {
                "mode": "NULLABLE",
                "name": "trade_id",
                "type": "INTEGER"
            },
            {
                "mode": "NULLABLE",
                "name": "tape",
                "type": "STRING"
            }
        ],
        write_disposition='WRITE_TRUNCATE'
    )


    snowflake_export >> bq_ingestion

if __name__ == "__main__":
    dag.cli()
    # dag.test()
