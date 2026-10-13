# gcp commands Cheatsheet


## Table of Contents
- [gcp commands Cheatsheet](#gcp-commands-cheatsheet)
  - [Table of Contents](#table-of-contents)
  - [Project Info](#project-info)
  - [Buckets Cloud Storage](#buckets-cloud-storage)
    - [create bucket](#create-bucket)
    - [copy file from local to bucket](#copy-file-from-local-to-bucket)
  - [Pub/Sub](#pubsub)
  - [BigQuery](#bigquery)
  - [BigTable](#bigtable)
  - [Spark Dataproc](#spark-dataproc)
  - [Apache Beam Dataflow](#apache-beam-dataflow)
  
  
  (#apache-beam-dataflow)


-------------------------------------------------------------------------------

## Project Info


``` 
gcloud auth list
gcloud config list project

gcloud config set project my_project


export BUCKET_NAME="$(gcloud config get-value project)"
``` 


--------------------------------------------------------------------------------

## Buckets Cloud Storage


### create bucket

### copy file from local to bucket


--------------------------------------------------------------------------------

## Pub/Sub

--------------------------------------------------------------------------------

## BigQuery

--------------------------------------------------------------------------------

## BigTable

--------------------------------------------------------------------------------

## Spark Dataproc

--------------------------------------------------------------------------------

## Apache Beam Dataflow

### IAM permissions

To run dataflow jobs we need:
* dataflow.admin

```
gcloud projects add-iam-policy-binding $PROJECT --member=user:$USER_EMAIL --role=roles/dataflow.admin
```




to run dataflow with Private IPs we need:
* role compute.networkAdmin
* update networks subnets default with flags
  * -- region
  * -- enable-private-ip-google-access

```
gcloud projects add-iam-policy-binding $PROJECT --member=user:$USER_EMAIL --role=roles/compute.networkAdmin

gcloud compute networks subnets update default \
--region=$REGION \
--enable-private-ip-google-access  
```


### dataflow run

run a job
```
gcloud dataflow jobs run job1 \
--gcs-location gs://dataflow-templates-"REGION"/latest/Word_Count \
--region $REGION \
--worker-machine-type e2-standard-2 \
--staging-location gs://$PROJECT/tmp \
--parameters inputFile=gs://dataflow-samples/shakespeare/kinglear.txt,output=gs://$PROJECT/results/outputs
```


run a job with Private IPs
```
gcloud dataflow jobs run job2 \
--gcs-location gs://dataflow-templates-"REGION"/latest/Word_Count \
--region $REGION \
--worker-machine-type e2-standard-2 \
--staging-location gs://$PROJECT/tmp \
--parameters inputFile=gs://dataflow-samples/shakespeare/kinglear.txt,output=gs://$PROJECT/results/outputs \
--disable-public-ips
```








--------------------------------------------------------------------------------