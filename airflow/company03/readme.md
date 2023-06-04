# Readme

This is a PoC of an ETL (Extract Transform Load) using airflow.
We extract data from one file called jobs.csv, make transformations,
then we load to Postgres.

## prerequisites
install 
docker
docker compose
python
download sample code 


## how to start/run containers

make sure you downloaded the code in your server

open a terminal, change directory, and build containers.
To do that run the next script

```
cd <folder_path>
docker compose up build
```

## how to run airflow DAG
open your web browser and open airflow using the next link
```
http://localhost:8080
```
then flip the off button to on



## how to stop containers
```
docker compose down
```

## how to explore airflow container
open a terminal and 

```
docker exec -it airflow /bin/bash
```

## how to explore postgres container
open a terminal and 

```
docker exec -it postgres /bin/bash
```

### how to connect postgres  database
```
psql -h [host name] -U [user name] -d [database]
```

### how to use  database
```
\c [database]
```

```
\c company01
```

### how to show tables
```
\dt
```

or

```
SELECT * FROM pg_catalog.pg_tables;
```

### how to see our tables
```
SELECT * FROM departments;
```


```
SELECT * FROM jobs;
```


