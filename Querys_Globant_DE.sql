--Crado por Bernardita hernandez 
--Ultima actualización 2024-12-29

/*Section 2: SQL
You need to explore the data that was inserted in the previous section. The stakeholders ask
for some specific metrics they need. You should create an end-point for each requirement.
Requirements
1-Number of employees hired for each job and department in 2021 divided by quarter. The
table must be ordered alphabetically by department and job
*/


SELECT   t2.department,
  t3.job,
  SUM(CASE WHEN EXTRACT(QUARTER FROM t1.datetime) = 1 THEN 1 ELSE 0 END) AS Q1,
  SUM(CASE WHEN EXTRACT(QUARTER FROM t1.datetime) = 2 THEN 1 ELSE 0 END) AS Q2,
  SUM(CASE WHEN EXTRACT(QUARTER FROM t1.datetime) = 3 THEN 1 ELSE 0 END) AS Q3,
  SUM(CASE WHEN EXTRACT(QUARTER FROM t1.datetime) = 4 THEN 1 ELSE 0 END) AS Q4
FROM `dev-splice-445621-u2.globant_prueba.HIRED_EMPLOYEES` t1 INNER JOIN 
  `dev-splice-445621-u2.globant_prueba.DEPARTMENTS` t2 ON 
  (t1.department_id=t2.id) INNER JOIN  
  `dev-splice-445621-u2.globant_prueba.JOBS` t3 ON 
  (t1.job_id=t3.id) 
WHERE EXTRACT(YEAR FROM t1.datetime)=2021  
AND IFNULL(t1.id,0)<>0
GROUP BY department,
job

/*2-List of ids, name and number of employees hired of each department that hired more
employees than the mean of employees hired in 2021 for all the departments, ordered
by the number of employees hired (descending).*/

WITH HIRED_EMPLOYEES_AVG AS (SELECT t1.department_id,
  t2.department,
  COUNT(t1.id) AS count_hired
FROM `dev-splice-445621-u2.globant_prueba.HIRED_EMPLOYEES` t1 INNER JOIN 
  `dev-splice-445621-u2.globant_prueba.DEPARTMENTS` t2 ON 
  (t1.department_id=t2.id) 
WHERE EXTRACT(YEAR FROM t1.datetime)=2021  
  AND IFNULL(t1.id,0)<>0
GROUP BY t1.department_id,
t2.department)

SELECT * FROM HIRED_EMPLOYEES_AVG
WHERE count_hired>( SELECT AVG(hired) FROM (--OBTENGO EL PROMEDIO
    SELECT   t1.department_id,
      COUNT(t1.id) AS hired
    FROM `dev-splice-445621-u2.globant_prueba.HIRED_EMPLOYEES` t1 INNER JOIN 
      `dev-splice-445621-u2.globant_prueba.DEPARTMENTS` t2 ON 
      (t1.department_id=t2.id) 
    WHERE EXTRACT(YEAR FROM t1.datetime)=2021  
    AND IFNULL(t1.id,0)<>0
    GROUP BY 1) TAB1)
ORDER BY count_hired DESC  