import json
import boto3
import os

def lambda_handler(event, context):

    s3Bucket = event["Profile_Job"]["Outputs"][0]["Location"]["Bucket"]
    s3ObjKey = event["Profile_Job"]["Outputs"][0]["Location"]["Key"]

    s3 =boto3.client('s3')
    glueDataBrewProfileResultFile = s3.get_object(Bucket=s3Bucket, Key=s3ObjKey)
    glueDataBrewProfileResult = json.loads(glueDataBrewProfileResultFile['Body'].read().decode('utf-8'))
    columnsProfiled = glueDataBrewProfileResult["columns"]

    PIIColumnsList = []
    for item in columnsProfiled:
      if "entityTypes" in item["entity"]:
        if (item["entity"]["rowsCount"]/glueDataBrewProfileResult["sampleSize"]) >= int(os.environ.get("threshold"))/100:
          PIIColumnsList.append(item["name"])

    if PIIColumnsList == []:
      return 'No PII columns found.'
    else:
      return PIIColumnsList