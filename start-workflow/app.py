import os
import json
import boto3

glue = boto3.client('glue')
    
def lambda_handler(event, context):
    workflow_name = os.environ['WORKFLOW_NAME']
    response = glue.start_workflow_run(Name=workflow_name)