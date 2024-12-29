--Opcion cambio nombres a ls tablas
SELECT int64_field_0,string_field_1 FROM `dev-splice-445621-u2.globant_prueba.JOBS` LIMIT 10

ALTER TABLE `dev-splice-445621-u2.globant_prueba.JOBS` RENAME COLUMN int64_field_0 TO ID_JOBS
ALTER TABLE `dev-splice-445621-u2.globant_prueba.JOBS` RENAME COLUMN string_field_1 TO JOBS

select * FROM `dev-splice-445621-u2.globant_prueba.JOBS`

SELECT int64_field_0,string_field_1 FROM `dev-splice-445621-u2.globant_prueba.JOBS` LIMIT 10

ALTER TABLE `dev-splice-445621-u2.globant_prueba.JOBS` RENAME COLUMN int64_field_0 TO ID_JOBS;
ALTER TABLE `dev-splice-445621-u2.globant_prueba.JOBS` RENAME COLUMN string_field_1 TO JOBS

select int64_field_0,string_field_1 FROM `dev-splice-445621-u2.globant_prueba.DEPARTMENTS`

ALTER TABLE `dev-splice-445621-u2.globant_prueba.DEPARTMENTS` RENAME COLUMN int64_field_0 TO ID_DEPARTMENTS;
ALTER TABLE `dev-splice-445621-u2.globant_prueba.DEPARTMENTS` RENAME COLUMN string_field_1 TO DEPARTMENTS

select * FROM `dev-splice-445621-u2.globant_prueba.DEPARTMENTS`

select int64_field_0,string_field_1,timestamp_field_2,int64_field_3,int64_field_4 FROM `dev-splice-445621-u2.globant_prueba.HIRED_EMPLOYEES`

ALTER TABLE `dev-splice-445621-u2.globant_prueba.HIRED_EMPLOYEES` RENAME COLUMN int64_field_0 TO ID_DEPARTMENTS;
ALTER TABLE `dev-splice-445621-u2.globant_prueba.HIRED_EMPLOYEES` RENAME COLUMN string_field_1 TO DEPARTMENTS;
ALTER TABLE `dev-splice-445621-u2.globant_prueba.HIRED_EMPLOYEES` RENAME COLUMN timestamp_field_2 TO DEPARTMENTS;
ALTER TABLE `dev-splice-445621-u2.globant_prueba.HIRED_EMPLOYEES` RENAME COLUMN int64_field_3 TO DEPARTMENTS;
ALTER TABLE `dev-splice-445621-u2.globant_prueba.HIRED_EMPLOYEES` RENAME COLUMN int64_field_4 TO DEPARTMENTS;

select * FROM `dev-splice-445621-u2.globant_prueba.HIRED_EMPLOYEES`

--Vista general de la tabla
select t1.int64_field_0 as id_hired_employees,
t1.string_field_1 as hired_employees,
t1.timestamp_field_2 as fecha,
t1.int64_field_3 as id_departments,
t2.string_field_1 as departments,
t1.int64_field_4 as id_jobs,
t3.string_field_1 as jobs
FROM `dev-splice-445621-u2.globant_prueba.HIRED_EMPLOYEES` t1 inner join 
  `dev-splice-445621-u2.globant_prueba.DEPARTMENTS` t2 on 
  (t1.int64_field_3=t2.int64_field_0) inner join 
  `dev-splice-445621-u2.globant_prueba.JOBS` t3 on 
  (t1.int64_field_4=t3.int64_field_0)
  
SELECT t1.int64_field_0 AS id_hired_employees,
t1.string_field_1 AS hired_employees,
t1.int64_field_3 AS id_departments,
t2.string_field_1 AS departments,
t1.int64_field_4 AS id_jobs,
t3.string_field_1 AS jobs,
t1.timestamp_field_2 AS fecha,
EXTRACT(YEAR FROM t1.timestamp_field_2) as ANIO,
  SUM(CASE WHEN EXTRACT(QUARTER FROM t1.timestamp_field_2) = 1 THEN 1 ELSE 0 END) AS Q1,
  SUM(CASE WHEN EXTRACT(QUARTER FROM t1.timestamp_field_2) = 2 THEN 1 ELSE 0 END) AS Q2,
  SUM(CASE WHEN EXTRACT(QUARTER FROM t1.timestamp_field_2) = 3 THEN 1 ELSE 0 END) AS Q3,
  SUM(CASE WHEN EXTRACT(QUARTER FROM t1.timestamp_field_2) = 4 THEN 1 ELSE 0 END) AS Q4
FROM `dev-splice-445621-u2.globant_prueba.HIRED_EMPLOYEES` t1 inner join 
  `dev-splice-445621-u2.globant_prueba.DEPARTMENTS` t2 on 
  (t1.int64_field_3=t2.int64_field_0) inner join 
  `dev-splice-445621-u2.globant_prueba.JOBS` t3 on 
  (t1.int64_field_4=t3.int64_field_0) 
group by 
id_hired_employees,
hired_employees,
id_departments,
departments,
id_jobs,
jobs,
fecha,
ANIO  
  
  --Ejercicio numero empleados por quarters
  