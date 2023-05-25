import json
import boto3
import os

def lambda_handler(event, context):
    columns = []
    for data_row in event['QueryResult']['ResultSet']['Rows']:
        if data_row['Data'][0]['VarCharValue'] != 'column_name':
            columns.append(data_row['Data'][0]['VarCharValue'])
    columns_string = ','.join(columns)
    return columns_string