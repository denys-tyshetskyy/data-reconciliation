import aws_cdk
import os

import step_function_config
from data_reconciliation.athena_connector import AthenaConnector

dirname = os.path.dirname(__file__)
from aws_cdk import Stack, aws_lambda
import json
from aws_cdk import aws_iam as iam
from aws_cdk import aws_stepfunctions as sf
import aws_cdk.aws_glue_alpha as glue_alpha
from aws_cdk import aws_glue as glue

from aws_cdk import (
    aws_s3 as s3,
    aws_ec2 as ec2,
    Fn
)

from constructs import Construct

from config import config


class ReconciliationStack(Stack):

    def __init__(
            self,
            scope: Construct,
            construct_id: str,
            **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        vpc_id= config["vpc_id"]
        print(f"VPC id {vpc_id}")

        vpc = ec2.Vpc.from_lookup(
            self,
            "vpc",
            vpc_id=config["vpc_id"]
        )
        sg = ec2.SecurityGroup.from_lookup_by_id(
            self,
            "db-sg",
            security_group_id=config["sg_id"]
        )

        self.connector = AthenaConnector(
            self,
            "AthenaConnector",
            app_name="db-reconciliation",
            vpc=vpc,
            subnet_ids=config["subnets_ids"],
            db_secret_prefix="secret_name",
            db_endpoint=config["server_name"],
            # db_port=config["port"],
            db_port=1521,
            db_name=config["database_name"],
            db_sg=sg
        )
        self._provision_resources()

    def _provision_resources(self):
        names = self._provision_crawler()
        self._provision_stepfunction(names)

    def _provision_crawler(self):
        datatabase = glue_alpha.Database(
            self,
            id=f"db",
            database_name=f"db"
        )
        role = iam.Role(
            self,
            id="glue-crawlerRole",
            role_name="glue-crawlerRole",
            assumed_by=iam.ServicePrincipal('glue.amazonaws.com'),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                ),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonS3FullAccess"
                )
            ]
        )
        self.bucket_name = "your_s3_bucket"
        crawler = glue.CfnCrawler(
            self,
            id="crawler",
            name="crawler",
            description="Crawling S3 bucket with data ingested from DB by DMS",
            configuration="{\"Version\":1.0,\"Grouping\":{\"TableGroupingPolicy\":\"CombineCompatibleSchemas\",\"TableLevelConfiguration\":4},\"CrawlerOutput\": {\"Partitions\": {\"AddOrUpdateBehavior\": \"InheritFromTable\"}}}",
            role=role.role_arn,
            schema_change_policy=glue.CfnCrawler.SchemaChangePolicyProperty(
                update_behavior='LOG',
                delete_behavior='LOG'),
            targets={
                's3Targets': [{"path": f"s3://{self.bucket_name}/"}]
            },
            database_name=datatabase.database_name
        )
        return (crawler.name, datatabase.database_name)

    def _provision_stepfunction(self, names):
        athena_result_bucket = s3.Bucket(self, id='reconciliation-bucket',
                                         public_read_access=False,
                                         enforce_ssl=True,
                                         lifecycle_rules=[
                                             s3.LifecycleRule(
                                                 id="ExpireCurrentObjects",
                                                 prefix=f"reconciliation-bucket/",
                                                 enabled=True,
                                                 expiration=aws_cdk.Duration.days(2),
                                             )
                                         ])
        parsing_lambda = self._create_parsing_lambda()
        step_function_role = self._create_sf_role(parsing_lambda.function_arn, athena_result_bucket.bucket_name)
        source_bucket = self.bucket_name
        bucket_prefix = "bucket_prefix"
        athena_datasource_name = f"reconciliation"
        state_machine = sf.CfnStateMachine(
            self,
            'DataReconciliationStateMachine',
            role_arn=step_function_role.role_arn,
            definition_string=json.dumps(
                step_function_config.build_reconciliation_step_function(source_bucket, bucket_prefix,
                                                                        athena_result_bucket.bucket_name,
                                                                        parsing_lambda.function_arn, names[0],
                                                                        athena_datasource_name, names[1]
                                                                        ))
        )
        return state_machine

    def _create_parsing_lambda(self) -> aws_lambda.Function:
        lambda_role = self._create_parsing_lambda_role()
        return aws_lambda.Function(
            self,
            f"PathParsingLambda",
            handler='handler.lambda_handler',
            runtime=aws_lambda.Runtime.PYTHON_3_9,
            code=aws_lambda.Code.from_asset(os.path.join(dirname, 'handler')),
            role=lambda_role,
            timeout=aws_cdk.Duration.seconds(30),
            environment={'threshold': '5'}
        )

    def _create_parsing_lambda_role(self) -> iam.Role:
        return iam.Role(
            self,
            "ParsingLambdaRole",
            assumed_by=
            iam.ServicePrincipal('lambda.amazonaws.com'),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSLambdaBasicExecutionRole')]
        )

    def _create_sf_role(self, parsing_lambda_arn, athena_bucket_name):
        athena_lambda_arn = self.connector.lambda_function_arn
        athena_spill_bucket_name = self.connector.spill_bucket.bucket_name
        invoke_glue_data_brew_profile_reader = iam.PolicyStatement(
            resources=[parsing_lambda_arn, athena_lambda_arn],
            effect=iam.Effect.ALLOW,
            actions=[
                'lambda:InvokeFunction'
            ]
        )
        s3_access_policy = iam.PolicyStatement(
            resources=[f"arn:aws:s3:::{self.bucket_name}", f"arn:aws:s3:::{self.bucket_name}/*"],
            effect=iam.Effect.ALLOW,
            actions=[
                's3:GetObject',
                's3:ListBucket'
            ]
        )
        athena_s3_access_policy = iam.PolicyStatement(
            resources=[f"arn:aws:s3:::{athena_bucket_name}", f"arn:aws:s3:::{athena_bucket_name}/*",
                       f"arn:aws:s3:::{athena_spill_bucket_name}", f"arn:aws:s3:::{athena_spill_bucket_name}/*"],
            effect=iam.Effect.ALLOW,
            actions=[
                's3:*'
            ]
        )
        states_exec_policy = iam.PolicyStatement(
            resources=["*"],
            effect=iam.Effect.ALLOW,
            actions=[
                'states:StartExecution'
            ]
        )
        account_id = Fn.ref("AWS::AccountId")
        crawler_policy = iam.PolicyStatement(
            resources=[f"arn:aws:glue:ap-southeast-2:{account_id}:crawler/*"],
            effect=iam.Effect.ALLOW,
            actions=[
                "glue:StartCrawler",
                "glue:GetCrawler"
            ]
        )
        athena_policy = iam.PolicyStatement(
            resources=[
                f"arn:aws:athena:ap-southeast-2:{account_id}:workgroup/primary",
                f"arn:aws:athena:ap-southeast-2:{account_id}:datacatalog/*",
                f"arn:aws:athena:ap-southeast-2:{account_id}:*/*"
            ],
            effect=iam.Effect.ALLOW,
            actions=[
                "athena:getQueryResults",
                "athena:startQueryExecution",
                "athena:stopQueryExecution",
                "athena:getQueryExecution",
                "athena:getDataCatalog"
            ]
        )

        glue_policy = iam.PolicyStatement(
            resources=[
                f"arn:aws:glue:ap-southeast-2:{account_id}:catalog",
                f"arn:aws:glue:ap-southeast-2:{account_id}:database/*",
                f"arn:aws:glue:ap-southeast-2:{account_id}:table/*",
                f"arn:aws:glue:ap-southeast-2:{account_id}:userDefinedFunction/*"
            ],
            effect=iam.Effect.ALLOW,
            actions=[
                "glue:GetDatabase",
                "glue:GetDatabases",
                "glue:UpdateDatabase",
                "glue:CreateTable",
                "glue:UpdateTable",
                "glue:GetTable",
                "glue:GetTables",
                "glue:BatchCreatePartition",
                "glue:CreatePartition",
                "glue:UpdatePartition",
                "glue:GetPartition",
                "glue:GetPartitions",
                "glue:BatchGetPartition"
            ]
        )

        policy_document = iam.PolicyDocument(
            statements=[
                invoke_glue_data_brew_profile_reader,
                s3_access_policy,
                athena_s3_access_policy,
                states_exec_policy,
                crawler_policy,
                athena_policy,
                glue_policy
            ]
        )
        return iam.Role(
            self,
            f"ReconciliationStepFunctionRole",
            assumed_by=
            iam.ServicePrincipal('states.amazonaws.com'),
            inline_policies={
                "StepFunctionPolicy": policy_document
            }
        )
