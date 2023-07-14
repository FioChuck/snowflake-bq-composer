from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator, SnowflakeSqlApiOperator

args = {
    'owner': 'packt-developer',
}

sql_statement = "COPY INTO 'gcs://snowflake-landing/faa/' \
FROM ( \
        SELECT DISTINCT object_id, \
            global_id, \
            faa_identifier, \
            name, \
            icao_id, \
            airport_type, \
            service_city, \
            state_abbreviation, \
            country \
        FROM US_AIRPORTS \
    ) OVERWRITE = TRUE FILE_FORMAT = (TYPE = csv COMPRESSION = NONE) STORAGE_INTEGRATION = gcs_integration"


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
        bucket='snowflake-landing',
        quote_character="'",
        source_format='csv',
        source_objects=[
            'faa/*'],
        destination_project_dataset_table='snowflake_ingestion.us_airports',
        schema_fields=[
  {
    "mode": "NULLABLE",
    "name": "object_id",
    "type": "INTEGER"
  },
  {
    "mode": "NULLABLE",
    "name": "global_id",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "faa_identifier",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "name",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "icao_id",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "airport_type",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "service_city",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "state_abbreviation",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "country",
    "type": "STRING"
  }
],
        write_disposition='WRITE_TRUNCATE'
    )

    snowflake_op_sql_str = SnowflakeOperator(task_id="snowflake_op_sql_str", sql=sql_statement)

    snowflake_op_sql_str >> bq_ingestion

if __name__ == "__main__":
    dag.cli()
    # dag.test()
