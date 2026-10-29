#!/bin/bash


# Set up environment variables

export BASE_DIR=/home/art/git/data_engineer/beam/stream_examples

# Pipeline args
export PROJECT_ID=$(gcloud config get-value project)
export REGION=Region
export BUCKET=gs://${PROJECT_ID}
export COLDLINE_BUCKET=${BUCKET}-coldline
export PIPELINE_FOLDER=${BUCKET}
export RUNNER=DataflowRunner

# known args
export INPUT_PATH=${PIPELINE_FOLDER}/events.json
export OUTPUT_PATH=${PIPELINE_FOLDER}-coldline/pipeline_output
export TABLE_NAME=${PROJECT_ID}:logs.logs_filtered


cd $BASE_DIR

python3 my_pipeline.py \
--project=${PROJECT_ID} \
--region=${REGION} \
--stagingLocation=${PIPELINE_FOLDER}/staging \
--tempLocation=${PIPELINE_FOLDER}/temp \
--runner=${RUNNER} \
--inputPath=${INPUT_PATH} \
--outputPath=${OUTPUT_PATH} \
--tableName=${TABLE_NAME}
