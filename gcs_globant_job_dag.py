from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

# Defino los parámetros básicos del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Declaro variables
BUCKET_NAME = 'my_bucket_bh'
#FILE_NAME = 'jobs.csv'
BQ_DATASET = 'dev-splice-445621-u2.globant_prueba'
#BQ_TABLE = 'JOBS'

# Creo el DAG 
with DAG(
    'gcs_globant_job_dag',
    default_args=default_args,
    description='Carga datos desde GCS a BigQuery',
    schedule_interval=None,  
    start_date=datetime(2023, 12, 1),
    catchup=False,
) as dag:
    
    # task 1 jobs Operador para cargar datos desde GCS a BigQuery
    load_jobs = GCSToBigQueryOperator(
        task_id='load_jobs',
        bucket=BUCKET_NAME,
        source_objects=['jobs.csv'],
        destination_project_dataset_table=f'{BQ_DATASET}.JOBS',
        source_format='CSV',
        skip_leading_rows=1,  # Omitimos el encabezado del archivo CSV
        write_disposition='WRITE_TRUNCATE',  # Sobrescribe la tabla si ya existe
        create_disposition='CREATE_IF_NEEDED',  # Crea la tabla si no existe
        field_delimiter=',',  # Especificamos que el delimitador es coma
        schema_fields=[{"name": "id", "type": "INTEGER", "mode": "REQUIRED"},{"name": "job", "type": "STRING", "mode": "REQUIRED"},]
    )
     # task 2 departments Operador para cargar datos desde GCS a BigQuery
    load_departments = GCSToBigQueryOperator(
        task_id='load_departments',
        bucket=BUCKET_NAME,
        source_objects=['departments.csv'],
        destination_project_dataset_table=f'{BQ_DATASET}.DEPARTMENTS',
        source_format='CSV',
        skip_leading_rows=1,  # Omitimos el encabezado del archivo CSV
        write_disposition='WRITE_TRUNCATE',  # Sobrescribe la tabla si ya existe
        create_disposition='CREATE_IF_NEEDED',  # Crea la tabla si no existe
        field_delimiter=',',  # Especificamos que el delimitador es coma
        schema_fields=[{"name": "id", "type": "INTEGER", "mode": "REQUIRED"},{"name": "department", "type": "STRING", "mode": "REQUIRED"},]
    )
     # task 3 hired employees Operador para cargar datos desde GCS a BigQuery
    load_hired_employees = GCSToBigQueryOperator(
        task_id='load_hired_employees',
        bucket=BUCKET_NAME,
        source_objects=['hired_employees.csv'],
        destination_project_dataset_table=f'{BQ_DATASET}.HIRED_EMPLOYEES',
        source_format='CSV',
        skip_leading_rows=1,  # Omitimos el encabezado del archivo CSV
        write_disposition='WRITE_TRUNCATE',  # Sobrescribe la tabla si ya existe
        create_disposition='CREATE_IF_NEEDED',  # Crea la tabla si no existe
        field_delimiter=',',  # Especificamos que el delimitador es coma
        schema_fields=[{"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "datetime", "type": "STRING", "mode": "NULLABLE"},
        {"name": "department_id", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "job_id", "type": "INTEGER", "mode": "NULLABLE"},]
    )
    
    load_jobs
    load_departments
    load_hired_employees