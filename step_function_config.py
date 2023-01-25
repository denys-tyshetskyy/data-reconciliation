"""Triggered by the object being uploaded into the source S3 bucket"""
def build_ongoing_ingest_step_function(lambda_arn, metadata_bucket, source_bucket, target_bucket,
                                       databrew_role_arn, secret_arn):
    return {
        "Comment": "Automatically detect PII columns of data files loaded into S3 and reproduce the data files with PII columns masked.",
        "StartAt": "Choice",
        "States": {
            "Choice": {
                "Type": "Choice",
                "Choices": [
                    {
                        "Variable": "$.detail.object.key",
                        "StringMatches": "*.parquet",
                        "Next": "DescribeDataset"
                    }
                ],
                "Default": "Successfully Mask PII Data"
            },
            "DescribeDataset": {
                "Type": "Task",
                "Next": "Choice Dataset",
                "Parameters": {
                    "Name.$": "States.ArrayGetItem(States.StringSplit($.detail.object.key, '/'), 0)"
                },
                "Resource": "arn:aws:states:::aws-sdk:databrew:describeDataset",
                "ResultPath": "$.Dataset",
                "Catch": [
                    {
                        "ErrorEquals": [
                            "DataBrew.ResourceNotFoundException"
                        ],
                        "ResultPath": "$.error.error_detail",
                        "Next": "Choice Dataset"
                    }
                ],
                "Retry": [
                    {
                        "ErrorEquals": [
                            "DataBrew.DataBrewException"
                        ],
                        "BackoffRate": 2,
                        "IntervalSeconds": 1,
                        "MaxAttempts": 100
                    }
                ],
                "ResultSelector": {
                    "filename.$": "States.ArrayGetItem(States.StringSplit($.Input.S3InputDefinition.Key, '/'), 1)",
                    "Name.$": "$.Name"
                }
            },
            "Choice Dataset": {
                "Type": "Choice",
                "Choices": [
                    {
                        "Variable": "$.Dataset.Name",
                        "IsPresent": True,
                        "Next": "Pass"
                    }
                ],
                "Default": "Create Glue DataBrew Dataset"
            },
            "Create Glue DataBrew Dataset": {
                "Type": "Task",
                "Parameters": {
                    "Input": {
                        "S3InputDefinition": {
                            "Bucket.$": "$.detail.bucket.name",
                            "Key.$": "$.detail.object.key"
                        }
                    },
                    "Name.$": "States.ArrayGetItem(States.StringSplit($.detail.object.key, '/'), 0)"
                },
                "Resource": "arn:aws:states:::aws-sdk:databrew:createDataset",
                "Next": "Create Glue DataBrew Profile Job",
                "ResultPath": "$.Dataset",
                "Retry": [
                    {
                        "ErrorEquals": [
                            "DataBrew.DataBrewException"
                        ],
                        "BackoffRate": 3,
                        "IntervalSeconds": 1,
                        "MaxAttempts": 100
                    }
                ],
                "Catch": [
                    {
                        "ErrorEquals": [
                            "DataBrew.ConflictException"
                        ],
                        "Next": "DescribeDataset",
                        "ResultPath": "$.error.error_detail"
                    }
                ]
            },
            "Create Glue DataBrew Profile Job": {
                "Type": "Task",
                "Parameters": {
                    "DatasetName.$": "$.Dataset.Name",
                    "Name.$": "States.Format('{}-PII-Detection-Job',$.Dataset.Name)",
                    "OutputLocation": {
                        "Bucket": metadata_bucket,
                        "Key": "metadata/"
                    },
                    "Configuration": {
                        "EntityDetectorConfiguration": {
                            "AllowedStatistics": [
                                {
                                    "Statistics": [
                                        "AGGREGATED_GROUP",
                                        "TOP_VALUES_GROUP",
                                        "CONTAINING_NUMERIC_VALUES_GROUP"
                                    ]
                                }
                            ],
                            "EntityTypes": [
                                "USA_ALL",
                                "PERSON_NAME",
                                "EMAIL"
                            ]
                        }
                    },
                    "RoleArn": databrew_role_arn
                },
                "Retry": [
                    {
                        "ErrorEquals": [
                            "DataBrew.DataBrewException"
                        ],
                        "BackoffRate": 2,
                        "IntervalSeconds": 1,
                        "MaxAttempts": 100
                    }
                ],
                "Catch": [
                    {
                        "ErrorEquals": [
                            "DataBrew.ConflictException"
                        ],
                        "Next": "DescribeDataset",
                        "ResultPath": "$.error.error_detail"
                    }
                ],
                "Resource": "arn:aws:states:::aws-sdk:databrew:createProfileJob",
                "Next": "Start Glue DataBrew Profile Job",
                "ResultPath": "$.Profile_Job"
            },
            "Pass": {
                "Type": "Pass",
                "Next": "Start Glue DataBrew Profile Job",
                "Parameters": {
                    "Name.$": "States.Format('{}-PII-Detection-Job',$.Dataset.Name)"
                },
                "ResultPath": "$.Profile_Job"
            },
            "Start Glue DataBrew Profile Job": {
                "Type": "Task",
                "Resource": "arn:aws:states:::databrew:startJobRun.sync",
                "Parameters": {
                    "Name.$": "$.Profile_Job.Name"
                },
                "Next": "Process Profile Result with Lambda Function",
                "ResultSelector": {
                    "Outputs.$": "$.Outputs"
                },
                "Retry": [
                    {
                        "ErrorEquals": [
                            "DataBrew.ServiceQuotaExceededException"
                        ],
                        "BackoffRate": 2,
                        "IntervalSeconds": 1,
                        "MaxAttempts": 100
                    },
                    {
                        "ErrorEquals": [
                            "DataBrew.AWSGlueDataBrewException"
                        ],
                        "BackoffRate": 1.5,
                        "IntervalSeconds": 1,
                        "MaxAttempts": 100
                    },
                    {
                        "ErrorEquals": [
                            "DataBrew.ResourceNotFoundException"
                        ],
                        "BackoffRate": 1.5,
                        "IntervalSeconds": 1,
                        "MaxAttempts": 10
                    },
                    {
                        "ErrorEquals": [
                            "DataBrew.ConflictException"
                        ],
                        "BackoffRate": 1.5,
                        "IntervalSeconds": 1,
                        "MaxAttempts": 100
                    }
                ],
                "ResultPath": "$.Profile_Job"
            },
            "Process Profile Result with Lambda Function": {
                "Type": "Task",
                "Resource": "arn:aws:states:::lambda:invoke",
                "Parameters": {
                    "FunctionName": lambda_arn,
                    "Payload.$": "$"
                },
                "Retry": [
                    {
                        "ErrorEquals": [
                            "Lambda.ServiceException",
                            "Lambda.AWSLambdaException",
                            "Lambda.SdkClientException"
                        ],
                        "IntervalSeconds": 2,
                        "MaxAttempts": 6,
                        "BackoffRate": 2
                    }
                ],
                "Next": "Validate if the Dataset Contains PII Columns",
                "ResultPath": "$.LambdaTaskResult",
                "ResultSelector": {
                    "pii-columns.$": "$.Payload"
                }
            },
            "Validate if the Dataset Contains PII Columns": {
                "Type": "Choice",
                "Choices": [
                    {
                        "Variable": "$.LambdaTaskResult.pii-columns",
                        "StringEquals": "No PII columns found.",
                        "Next": "CopyObject"
                    }
                ],
                "Default": "DescribeRecipe"
            },
            "CopyObject": {
                "Type": "Task",
                "Next": "No PII Data is Found",
                "Parameters": {
                    "Bucket": target_bucket,
                    "CopySource.$": f"States.Format('{source_bucket}/{{}}', $.detail.object.key)",
                    "Key.$": "$.detail.object.key"
                },
                "Resource": "arn:aws:states:::aws-sdk:s3:copyObject"
            },
            "DescribeRecipe": {
                "Type": "Task",
                "Next": "Recipe Exist",
                "Parameters": {
                    "Name.$": "States.Format('{}-PII-Masking-Recipe',$.Dataset.Name)",
                    "RecipeVersion": "0.1"
                },
                "Resource": "arn:aws:states:::aws-sdk:databrew:describeRecipe",
                "Catch": [
                    {
                        "ErrorEquals": [
                            "DataBrew.ResourceNotFoundException"
                        ],
                        "Next": "Recipe Exist",
                        "ResultPath": "$.error.error_detail"
                    }
                ],
                "Retry": [
                    {
                        "ErrorEquals": [
                            "DataBrew.DataBrewException"
                        ],
                        "BackoffRate": 1,
                        "IntervalSeconds": 1,
                        "MaxAttempts": 100
                    }
                ],
                "ResultPath": "$.Recipe"
            },
            "Recipe Exist": {
                "Type": "Choice",
                "Choices": [
                    {
                        "Variable": "$.Recipe.Name",
                        "IsPresent": True,
                        "Next": "UpdateRecipeJob"
                    }
                ],
                "Default": "Create Glue DataBrew PII Data Masking Recipe"
            },
            "UpdateRecipeJob": {
                "Type": "Task",
                "Next": "Pass (1)",
                "Parameters": {
                    "Outputs": [
                        {
                            "Format": "PARQUET",
                            "Location": {
                                "Bucket": target_bucket,
                                "Key.$": "States.Format('{}/{}',States.ArrayGetItem(States.StringSplit($.detail.object.key, '/'), 0),States.ArrayGetItem(States.StringSplit($.detail.object.key, '/'), 1))"
                            }
                        }
                    ],
                    "Name.$": "States.Format('{}-PII-Masking-Job',$.Dataset.Name)",
                    "RoleArn": databrew_role_arn
                },
                "Resource": "arn:aws:states:::aws-sdk:databrew:updateRecipeJob",
                "ResultPath": "$.UpdateRecipe"
            },
            "Pass (1)": {
                "Type": "Pass",
                "Next": "Start Glue DataBrew Recipe Job",
                "Parameters": {
                    "Name.$": "States.Format('{}-PII-Masking-Job',States.ArrayGetItem(States.StringSplit($.Dataset.Name, '-'), 0))"
                }
            },
            "Create Glue DataBrew PII Data Masking Recipe": {
                "Type": "Task",
                "Parameters": {
                    "Name.$": "States.Format('{}-PII-Masking-Recipe',$.Dataset.Name)",
                    "Steps": [
                        {
                            "Action": {
                                "Operation": "CRYPTOGRAPHIC_HASH",
                                "Parameters": {
                                    "secretId": secret_arn,
                                    "sourceColumns.$": "$.LambdaTaskResult.pii-columns"
                                }
                            }
                        }
                    ]
                },
                "Resource": "arn:aws:states:::aws-sdk:databrew:createRecipe",
                "ResultPath": "$.Recipe",
                "Next": "Create Glue DataBrew Project"
            },
            "No PII Data is Found": {
                "Type": "Succeed"
            },
            "Create Glue DataBrew Project": {
                "Type": "Task",
                "Next": "Create Glue DataBrew Recipe Job",
                "Parameters": {
                    "DatasetName.$": "$.Dataset.Name",
                    "Name.$": "States.Format('{}-PII-Project',$.Dataset.Name)",
                    "RecipeName.$": "$.Recipe.Name",
                    "RoleArn": databrew_role_arn
                },
                "Resource": "arn:aws:states:::aws-sdk:databrew:createProject",
                "ResultPath": "$.Project"
            },
            "Create Glue DataBrew Recipe Job": {
                "Type": "Task",
                "Parameters": {
                    "ProjectName.$": "$.Project.Name",
                    "LogSubscription": "DISABLE",
                    "Outputs": [
                        {
                            "Format": "PARQUET",
                            "Location": {
                                "Bucket": target_bucket,
                                "Key.$": "States.Format('{}/{}',States.ArrayGetItem(States.StringSplit($.detail.object.key, '/'), 0),States.ArrayGetItem(States.StringSplit($.detail.object.key, '/'), 1))"
                            }
                        }
                    ],
                    "Name.$": "States.Format('{}-PII-Masking-Job',$.Dataset.Name)",
                    "RoleArn": databrew_role_arn
                },
                "Resource": "arn:aws:states:::aws-sdk:databrew:createRecipeJob",
                "Next": "Start Glue DataBrew Recipe Job"
            },
            "Start Glue DataBrew Recipe Job": {
                "Type": "Task",
                "Resource": "arn:aws:states:::databrew:startJobRun.sync",
                "Parameters": {
                    "Name.$": "$.Name"
                },
                "Retry": [
                    {
                        "ErrorEquals": [
                            "DataBrew.ServiceQuotaExceededException"
                        ],
                        "BackoffRate": 1.5,
                        "IntervalSeconds": 2,
                        "MaxAttempts": 99
                    },
                    {
                        "ErrorEquals": [
                            "DataBrew.AWSGlueDataBrewException"
                        ],
                        "BackoffRate": 2,
                        "IntervalSeconds": 3,
                        "MaxAttempts": 100
                    }
                ],
                "Next": "Successfully Mask PII Data"
            },
            "Successfully Mask PII Data": {
                "Type": "Succeed"
            }
        }
    }


"""Manually triggered for one-off consumption of all objects from source s3"""
def build_history_ingest_step_function(lambda_arn, metadata_bucket, source_bucket, target_bucket, databrew_role_arn, secret_arn):
    return {
        "Comment": "Automatically detect PII columns of data files in existing files located in S3 and reproduce the data files with PII columns masked.",
        "StartAt": "Map",
        "States": {
            "Map": {
                "Type": "Map",
                "ItemProcessor": {
                    "ProcessorConfig": {
                        "Mode": "DISTRIBUTED",
                        "ExecutionType": "STANDARD"
                    },
                    "StartAt": "Choice",
                    "States": {
                        "Choice": {
                            "Type": "Choice",
                            "Choices": [
                                {
                                    "Variable": "$.Key",
                                    "StringMatches": "*.parquet",
                                    "Next": "DescribeDataset"
                                }
                            ],
                            "Default": "Successfully Mask PII Data"
                        },
                        "DescribeDataset": {
                            "Type": "Task",
                            "Parameters": {
                                "Name.$": "States.ArrayGetItem(States.StringSplit($.Key, '/'), 0)"
                            },
                            "Resource": "arn:aws:states:::aws-sdk:databrew:describeDataset",
                            "ResultPath": "$.detail",
                            "Catch": [
                                {
                                    "ErrorEquals": [
                                        "DataBrew.ResourceNotFoundException"
                                    ],
                                    "ResultPath": "$.error_detail",
                                    "Next": "Choice Dataset"
                                }
                            ],
                            "Retry": [
                                {
                                    "ErrorEquals": [
                                        "DataBrew.DataBrewException"
                                    ],
                                    "BackoffRate": 2,
                                    "IntervalSeconds": 1,
                                    "MaxAttempts": 100
                                }
                            ],
                            "Next": "Choice Dataset"
                        },
                        "Choice Dataset": {
                            "Type": "Choice",
                            "Choices": [
                                {
                                    "Variable": "$.detail.Name",
                                    "IsPresent": True,
                                    "Next": "Pass"
                                }
                            ],
                            "Default": "Create Glue DataBrew Dataset"
                        },
                        "Pass": {
                            "Type": "Pass",
                            "Parameters": {
                                "Name.$": "States.Format('{}-PII-Detection-Job',$.detail.Name)"
                            },
                            "Next": "Start Glue DataBrew Profile Job",
                            "ResultPath": "$.detail"
                        },
                        "Create Glue DataBrew Dataset": {
                            "Type": "Task",
                            "Parameters": {
                                "Input": {
                                    "S3InputDefinition": {
                                        "Bucket": source_bucket,
                                        "Key.$": "States.Format('{}/<.*>.parquet',States.ArrayGetItem(States.StringSplit($.Key, '/'), 0))"
                                    }
                                },
                                "Name.$": "States.ArrayGetItem(States.StringSplit($.Key, '/'),0)"
                            },
                            "Resource": "arn:aws:states:::aws-sdk:databrew:createDataset",
                            "Next": "Create Glue DataBrew Profile Job",
                            "Retry": [
                                {
                                    "ErrorEquals": [
                                        "DataBrew.DataBrewException"
                                    ],
                                    "BackoffRate": 3,
                                    "IntervalSeconds": 1,
                                    "MaxAttempts": 100
                                }
                            ],
                            "ResultPath": "$.detail",
                            "Catch": [
                                {
                                    "ErrorEquals": [
                                        "DataBrew.ConflictException"
                                    ],
                                    "Next": "DescribeDataset",
                                    "ResultPath": "$.error_detail"
                                }
                            ]
                        },
                        "Create Glue DataBrew Profile Job": {
                            "Type": "Task",
                            "Parameters": {
                                "DatasetName.$": "$.detail.Name",
                                "Name.$": "States.Format('{}-PII-Detection-Job',$.detail.Name)",
                                "OutputLocation": {
                                    "Bucket": metadata_bucket,
                                    "Key": "metadata/"
                                },
                                "Configuration": {
                                    "EntityDetectorConfiguration": {
                                        "AllowedStatistics": [
                                            {
                                                "Statistics": [
                                                    "AGGREGATED_GROUP",
                                                    "TOP_VALUES_GROUP",
                                                    "CONTAINING_NUMERIC_VALUES_GROUP"
                                                ]
                                            }
                                        ],
                                        "EntityTypes": [
                                            "USA_ALL",
                                            "PERSON_NAME",
                                            "EMAIL"
                                        ]
                                    }
                                },
                                "RoleArn": databrew_role_arn
                            },
                            "Resource": "arn:aws:states:::aws-sdk:databrew:createProfileJob",
                            "Next": "Start Glue DataBrew Profile Job",
                            "Retry": [
                                {
                                    "ErrorEquals": [
                                        "DataBrew.DataBrewException"
                                    ],
                                    "BackoffRate": 2,
                                    "IntervalSeconds": 1,
                                    "MaxAttempts": 100
                                }
                            ],
                            "ResultPath": "$.detail",
                            "Catch": [
                                {
                                    "ErrorEquals": [
                                        "DataBrew.ConflictException"
                                    ],
                                    "Next": "DescribeDataset",
                                    "ResultPath": "$.error_detail"
                                }
                            ]
                        },
                        "Start Glue DataBrew Profile Job": {
                            "Type": "Task",
                            "Resource": "arn:aws:states:::databrew:startJobRun.sync",
                            "Parameters": {
                                "Name.$": "$.detail.Name"
                            },
                            "ResultSelector": {
                                "DatasetName.$": "$.DatasetName",
                                "Outputs.$": "$.Outputs"
                            },
                            "Next": "Process Profile Result with Lambda Function",
                            "Retry": [
                                {
                                    "ErrorEquals": [
                                        "DataBrew.ServiceQuotaExceededException"
                                    ],
                                    "BackoffRate": 2,
                                    "IntervalSeconds": 1,
                                    "MaxAttempts": 100
                                },
                                {
                                    "ErrorEquals": [
                                        "DataBrew.AWSGlueDataBrewException"
                                    ],
                                    "BackoffRate": 1.5,
                                    "IntervalSeconds": 1,
                                    "MaxAttempts": 100
                                },
                                {
                                    "ErrorEquals": [
                                        "DataBrew.ResourceNotFoundException"
                                    ],
                                    "BackoffRate": 1.5,
                                    "IntervalSeconds": 1,
                                    "MaxAttempts": 10
                                },
                                {
                                    "ErrorEquals": [
                                        "DataBrew.ConflictException"
                                    ],
                                    "BackoffRate": 1.5,
                                    "IntervalSeconds": 30,
                                    "MaxAttempts": 100
                                }
                            ],
                            "ResultPath": "$.detail"
                        },
                        "Process Profile Result with Lambda Function": {
                            "Type": "Task",
                            "Resource": "arn:aws:states:::lambda:invoke",
                            "Parameters": {
                                "FunctionName": lambda_arn,
                                "Payload.$": "$"
                            },
                            "Retry": [
                                {
                                    "ErrorEquals": [
                                        "Lambda.ServiceException",
                                        "Lambda.AWSLambdaException",
                                        "Lambda.SdkClientException"
                                    ],
                                    "IntervalSeconds": 2,
                                    "MaxAttempts": 6,
                                    "BackoffRate": 2
                                }
                            ],
                            "ResultPath": "$.LambdaTaskResult",
                            "ResultSelector": {
                                "pii-columns.$": "$.Payload"
                            },
                            "Next": "Validate if the Dataset Contains PII Columns"
                        },
                        "Validate if the Dataset Contains PII Columns": {
                            "Type": "Choice",
                            "Choices": [
                                {
                                    "Variable": "$.LambdaTaskResult.pii-columns",
                                    "StringEquals": "No PII columns found.",
                                    "Next": "CopyObject"
                                }
                            ],
                            "Default": "DescribeRecipe"
                        },
                        "CopyObject": {
                            "Type": "Task",
                            "Next": "No PII Data is Found",
                            "Parameters": {
                                "Bucket": target_bucket,
                                "CopySource.$": "States.Format('uat-stage-clg-datalake-u-clgdatalakeuatrawbucketc-1a0xlon0x63t1/{}', $.Key)",
                                "Key.$": "$.Key",
                                "Acl": "bucket-owner-full-control"
                            },
                            "Resource": "arn:aws:states:::aws-sdk:s3:copyObject"
                        },
                        "DescribeRecipe": {
                            "Type": "Task",
                            "Parameters": {
                                "Name.$": "States.Format('{}-PII-Masking-Recipe',$.detail.DatasetName)",
                                "RecipeVersion": "0.1"
                            },
                            "Resource": "arn:aws:states:::aws-sdk:databrew:describeRecipe",
                            "Catch": [
                                {
                                    "ErrorEquals": [
                                        "DataBrew.ResourceNotFoundException"
                                    ],
                                    "ResultPath": "$.error_detail",
                                    "Next": "Recipe Exist"
                                }
                            ],
                            "ResultPath": "$.DatasetName",
                            "Next": "Recipe Exist",
                            "Retry": [
                                {
                                    "ErrorEquals": [
                                        "DataBrew.DataBrewException"
                                    ],
                                    "BackoffRate": 1,
                                    "IntervalSeconds": 1,
                                    "MaxAttempts": 100
                                }
                            ]
                        },
                        "Recipe Exist": {
                            "Type": "Choice",
                            "Choices": [
                                {
                                    "Variable": "$.DatasetName.Name",
                                    "IsPresent": True,
                                    "Next": "UpdateRecipeJob"
                                }
                            ],
                            "Default": "Create Glue DataBrew PII Data Masking Recipe"
                        },
                        "UpdateRecipeJob": {
                            "Type": "Task",
                            "Next": "Pass (2)",
                            "Parameters": {
                                "Outputs": [
                                    {
                                        "Format": "PARQUET",
                                        "Location": {
                                            "Bucket": target_bucket,
                                            "Key.$": "States.Format('{}/{}',States.ArrayGetItem(States.StringSplit($.Key, '/'), 0),States.ArrayGetItem(States.StringSplit($.Key, '/'), 1))"
                                        }
                                    }
                                ],
                                "Name.$": "States.Format('{}-PII-Masking-Job',$.detail.DatasetName)",
                                "RoleArn": databrew_role_arn
                            },
                            "Resource": "arn:aws:states:::aws-sdk:databrew:updateRecipeJob",
                            "ResultPath": "$.UpdateRecipe"
                        },
                        "Create Glue DataBrew PII Data Masking Recipe": {
                            "Type": "Task",
                            "Parameters": {
                                "Name.$": "States.Format('{}-PII-Masking-Recipe',$.detail.DatasetName)",
                                "Steps": [
                                    {
                                        "Action": {
                                            "Operation": "CRYPTOGRAPHIC_HASH",
                                            "Parameters": {
                                                "secretId": secret_arn,
                                                "sourceColumns.$": "$.LambdaTaskResult.pii-columns"
                                            }
                                        }
                                    }
                                ]
                            },
                            "Resource": "arn:aws:states:::aws-sdk:databrew:createRecipe",
                            "ResultPath": "$.Recipe",
                            "Next": "Create Glue DataBrew Project"
                        },
                        "Create Glue DataBrew Project": {
                            "Type": "Task",
                            "Parameters": {
                                "DatasetName.$": "$.detail.DatasetName",
                                "Name.$": "States.Format('{}-PII-Project',$.detail.DatasetName)",
                                "RecipeName.$": "$.Recipe.Name",
                                "RoleArn": databrew_role_arn
                            },
                            "Resource": "arn:aws:states:::aws-sdk:databrew:createProject",
                            "ResultPath": "$.Project",
                            "Next": "Create Glue DataBrew Recipe Job"
                        },
                        "Create Glue DataBrew Recipe Job": {
                            "Type": "Task",
                            "Parameters": {
                                "ProjectName.$": "$.Project.Name",
                                "LogSubscription": "DISABLE",
                                "Outputs": [
                                    {
                                        "Format": "PARQUET",
                                        "Location": {
                                            "Bucket": target_bucket,
                                            "Key.$": "States.Format('{}/{}',States.ArrayGetItem(States.StringSplit($.Key, '/'), 0),States.ArrayGetItem(States.StringSplit($.Key, '/'), 1))"
                                        }
                                    }
                                ],
                                "Name.$": "States.Format('{}-PII-Masking-Job',$.detail.DatasetName)",
                                "RoleArn": databrew_role_arn
                            },
                            "Resource": "arn:aws:states:::aws-sdk:databrew:createRecipeJob",
                            "Next": "Start Glue DataBrew Recipe Job"
                        },
                        "Start Glue DataBrew Recipe Job": {
                            "Type": "Task",
                            "Resource": "arn:aws:states:::databrew:startJobRun.sync",
                            "Parameters": {
                                "Name.$": "$.Name"
                            },
                            "Next": "Successfully Mask PII Data",
                            "Retry": [
                                {
                                    "ErrorEquals": [
                                        "DataBrew.ServiceQuotaExceededException"
                                    ],
                                    "BackoffRate": 1.5,
                                    "IntervalSeconds": 2,
                                    "MaxAttempts": 99
                                },
                                {
                                    "ErrorEquals": [
                                        "DataBrew.AWSGlueDataBrewException"
                                    ],
                                    "BackoffRate": 2,
                                    "IntervalSeconds": 3,
                                    "MaxAttempts": 100
                                }
                            ]
                        },
                        "Successfully Mask PII Data": {
                            "Type": "Succeed"
                        },
                        "Pass (2)": {
                            "Type": "Pass",
                            "Next": "Start Glue DataBrew Recipe Job",
                            "Parameters": {
                                "Name.$": "States.Format('{}-PII-Masking-Job',$.detail.DatasetName)"
                            }
                        },
                        "No PII Data is Found": {
                            "Type": "Succeed"
                        }
                    }
                },
                "MaxConcurrency": 1000,
                "ItemReader": {
                    "Resource": "arn:aws:states:::s3:listObjectsV2",
                    "Parameters": {
                        "Bucket": source_bucket
                    }
                },
                "End": True,
                "Label": "Map"
            }
        }
    }
