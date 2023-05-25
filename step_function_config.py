def build_reconciliation_step_function(bucket_name, bucket_prefix, result_bucket, lambda_arn, crawler_name, athena_datasource_name, catalog_db_name):
    return {
        "Comment": "Reconciliation state machine",
        "StartAt": "StartCrawler",
        "States": {
            "StartCrawler": {
              "Type": "Task",
              "Next": "Wait (3)",
              "Parameters": {
                "Name": crawler_name
              },
              "Resource": "arn:aws:states:::aws-sdk:glue:startCrawler"
            },
            "Wait (3)": {
              "Type": "Wait",
              "Seconds": 10,
              "Next": "GetCrawler"
            },
            "GetCrawler": {
              "Type": "Task",
              "Next": "Choice (4)",
              "Parameters": {
                "Name": crawler_name
              },
              "Resource": "arn:aws:states:::aws-sdk:glue:getCrawler"
            },
            "Choice (4)": {
              "Type": "Choice",
              "Choices": [
                {
                  "Or": [
                    {
                      "Variable": "$.Crawler.State",
                      "StringEquals": "STOPPING"
                    },
                    {
                      "Variable": "$.Crawler.State",
                      "StringEquals": "READY"
                    }
                  ],
                  "Next": "ListObjects"
                }
              ],
              "Default": "Wait (2)"
            },
            "Wait (2)": {
              "Type": "Wait",
              "Seconds": 25,
              "Next": "GetCrawler"
            },
            "ListObjects": {
                "Type": "Task",
                "Parameters": {
                    "Bucket": bucket_name,
                    "Prefix": f"{bucket_prefix}/DB/",
                    "Delimiter": "/"
                },
                "Resource": "arn:aws:states:::aws-sdk:s3:listObjects",
                "Next": "Map"
            },
            "Map": {
                "Type": "Map",
                "ItemProcessor": {
                    "ProcessorConfig": {
                        "Mode": "DISTRIBUTED",
                        "ExecutionType": "STANDARD"
                    },
                    "StartAt": "Pass",
                    "States": {
                        "Pass": {
                            "Type": "Pass",
                            "Next": "Athena StartQueryExecution",
                            "Parameters": {
                                "Name.$": "States.ArrayGetItem(States.StringSplit($.Prefix, '/'), 2)",
                                "Quote": "'",
                                "Athena_Datasource_Name": athena_datasource_name,
                                "Catalog_Table_Name": catalog_db_name
                            }
                        },
                        "Athena StartQueryExecution": {
                            "Type": "Task",
                            "Resource": "arn:aws:states:::athena:startQueryExecution",
                            "Parameters": {
                                "QueryString.$": "States.Format('Select column_name from \"{}\".\"sys\".\"all_tab_columns\" where table_name = {}{}{} and owner = test',$.Athena_Datasource_Name,$.Quote,$.Name,$.Quote,$.Quote,$.Quote)",
                                "WorkGroup": "primary",
                                "ResultConfiguration": {
                                    "OutputLocation": f"s3://{result_bucket}/athena-result/"
                                }
                            },
                            "Next": "Athena GetQueryExecution",
                            "ResultPath": "$.Query1"
                        },
                        "Athena GetQueryExecution": {
                            "Type": "Task",
                            "Resource": "arn:aws:states:::athena:getQueryExecution",
                            "Parameters": {
                                "QueryExecutionId.$": "$.Query1.QueryExecutionId"
                            },
                            "Next": "Choice (1)",
                            "ResultPath": "$.QueryExecution"
                        },
                        "Choice (1)": {
                            "Type": "Choice",
                            "Choices": [
                                {
                                    "Variable": "$.QueryExecution.QueryExecution.Status.State",
                                    "StringEquals": "SUCCEEDED",
                                    "Next": "Athena GetQueryResults"
                                }
                            ],
                            "Default": "Wait"
                        },
                        "Wait": {
                            "Type": "Wait",
                            "Seconds": 5,
                            "Next": "Athena GetQueryExecution"
                        },
                        "Athena GetQueryResults": {
                            "Type": "Task",
                            "Resource": "arn:aws:states:::athena:getQueryResults",
                            "Parameters": {
                                "MaxResults": 100,
                                "QueryExecutionId.$": "$.Query1.QueryExecutionId"
                            },
                            "Next": "ParseColumns",
                            "ResultPath": "$.QueryResult"
                        },
                        "ParseColumns": {
                            "Type": "Task",
                            "Resource": "arn:aws:states:::lambda:invoke",
                            "Parameters": {
                                "Payload.$": "$",
                                "FunctionName": lambda_arn
                            },
                            "Retry": [
                                {
                                    "ErrorEquals": [
                                        "Lambda.ServiceException",
                                        "Lambda.AWSLambdaException",
                                        "Lambda.SdkClientException",
                                        "Lambda.TooManyRequestsException"
                                    ],
                                    "IntervalSeconds": 2,
                                    "MaxAttempts": 6,
                                    "BackoffRate": 2
                                }
                            ],
                            "Next": "Athena StartQueryExecution (1)",
                            "ResultSelector": {
                                "table_columns.$": "$.Payload"
                            },
                            "ResultPath": "$.LambdaTaskResult"
                        },
                        "Athena StartQueryExecution (1)": {
                            "Type": "Task",
                            "Resource": "arn:aws:states:::athena:startQueryExecution",
                            "Parameters": {
                                "QueryString.$": "States.Format('Select {} from {} EXCEPT SELECT {} from \"AwsDataCatalog\".\"{}\".\"{}\"',$.LambdaTaskResult.table_columns, $.Athena_Datasource_Name, $.Name,$.LambdaTaskResult.table_columns,$.Catalog_Table_Name,$.Name)",
                                "WorkGroup": "primary",
                                "ResultConfiguration": {
                                    "OutputLocation": f"s3://{result_bucket}/athena-result/"
                                }
                            },
                            "Next": "Athena GetQueryExecution (1)",
                            "ResultPath": "$.ComparisonResult"
                        },
                        "Athena GetQueryExecution (1)": {
                            "Type": "Task",
                            "Resource": "arn:aws:states:::athena:getQueryExecution",
                            "Parameters": {
                                "QueryExecutionId.$": "$.ComparisonResult.QueryExecutionId"
                            },
                            "Next": "Choice (2)",
                            "ResultPath": "$.QueryExecution"
                        },
                        "Choice (2)": {
                            "Type": "Choice",
                            "Choices": [
                                {
                                    "Variable": "$.QueryExecution.QueryExecution.Status.State",
                                    "StringEquals": "SUCCEEDED",
                                    "Next": "Athena GetQueryResults (1)"
                                }
                            ],
                            "Default": "Wait (1)"
                        },
                        "Wait (1)": {
                            "Type": "Wait",
                            "Seconds": 5,
                            "Next": "Athena GetQueryExecution (1)"
                        },
                        "Athena GetQueryResults (1)": {
                            "Type": "Task",
                            "Resource": "arn:aws:states:::athena:getQueryResults",
                            "Parameters": {
                                "MaxResults": 10,
                                "QueryExecutionId.$": "$.QueryExecution.QueryExecution.QueryExecutionId"
                            },
                            "Next": "Pass (1)"
                        },
                        "Pass (1)": {
                            "Type": "Pass",
                            "Next": "Choice (3)",
                            "Parameters": {
                                "ArrayLength.$": "States.ArrayLength($.ResultSet.Rows)"
                            }
                        },
                        "Choice (3)": {
                            "Type": "Choice",
                            "Choices": [
                                {
                                    "Variable": "$.ArrayLength",
                                    "NumericGreaterThan": 1,
                                    "Next": "Fail"
                                }
                            ],
                            "Default": "Success"
                        },
                        "Fail": {
                            "Type": "Fail"
                        },
                        "Success": {
                            "Type": "Succeed"
                        }
                    }
                },
                "End": True,
                "Label": "Map",
                "MaxConcurrency": 10,
                "ItemsPath": "$.CommonPrefixes",
                "ToleratedFailurePercentage": 50
            }
        }
    }
