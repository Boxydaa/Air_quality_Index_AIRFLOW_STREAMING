import json
import json
import os
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

import boto3
client = boto3.client('glue')

glueJobName = "aqi_etl_job"

def lambda_handler(event, context):
    logger.info('## INITIATED BY EVENT:Crawler completed ')
    response = client.start_job_run(JobName = glueJobName)
    logger.info('## STARTED GLUE JOB: ' + glueJobName)
    logger.info('## GLUE JOB RUN ID: ' + response['JobRunId'])
    return response