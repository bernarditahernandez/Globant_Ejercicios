He creado un nuevo proyecto en mi GCP personal, para poder desarrollar el ejercicio:
1-Cree un bucket en Cloud Storage llamado my_bucket_bh (Para esto active la API de Cloud Storage) donde deje los archivos .csv que despues usare para cargar las tablas solicitadas
2-Cree un dataset de Bigquery llamado Globant_prueba (Para esto active la Api de Bigquery) y cree las tablas con una estrutura tipo, despues en la carga se reescribira por defecto el nombre de las columnas
3-Cree un compouser llamado Compouserbh (Active la API de compouser y airflow) 
  Cree un dag de airflow llamado gcs_globant_job_dag.py que envia por lotes y en forma automatica (lo deje para ejcucion manual para no gastar recursos) la informacion de los archivos a sus respectivas tablas.
4-Finalmente desarrolle las querys necesarias para responder a las preguntas solicitadas en la prueba  
