import json
import os

import aws_cdk.aws_events as events
import aws_cdk.aws_secretsmanager as secrets_manager
import aws_cdk.aws_stepfunctions as sf
from aws_cdk import (
    Stack
)
from aws_cdk.aws_iam import Role, PolicyStatement, Effect, PolicyDocument, ServicePrincipal, ManagedPolicy
from aws_cdk.aws_lambda import Function, Runtime, Code
from aws_cdk.aws_s3 import Bucket, BlockPublicAccess, BucketEncryption
from constructs import Construct

import step_function_config


class DataMaskingStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        self._provision_resources()

    def _provision_resources(self):
        self.databrew_metadata_bucket = self._create_bucket("metadata-bucket")
        self.source_bucket = self._create_bucket("source-bucket")
        self.destination_bucket = self._create_bucket("destination-bucket")
        profiling_lambda = self._create_lambda()
        secret = self._create_secret()
        data_brew_role = self._create_databrew_role(secret)
        step_function_role = self._create_sf_role(profiling_lambda.function_arn)
        history_ingestion_state_machine = self._create_state_machine(profiling_lambda.function_arn,
                                                                     data_brew_role.role_arn,
                                                                     secret.secret_arn, step_function_role.role_arn,
                                                                     True)
        ongoing_ingestion_state_machine = self._create_state_machine(profiling_lambda.function_arn,
                                                                     data_brew_role.role_arn,
                                                                     secret.secret_arn, step_function_role.role_arn,
                                                                     False)
        self._create_sf_rule(ongoing_ingestion_state_machine)

    def _create_lambda_role(self) -> Role:
        s3_bucket_access_policy = PolicyStatement(
            resources=[f"arn:aws:s3:::{self.databrew_metadata_bucket}",
                       f"arn:aws:s3:::{self.databrew_metadata_bucket}/*"],
            effect=Effect.ALLOW,
            actions=[
                's3:*'
            ]
        )
        policy_document = PolicyDocument(
            statements=[
                s3_bucket_access_policy
            ]
        )
        return Role(
            self,
            "GlueDataBrewLambdaRole",
            assumed_by=
            ServicePrincipal('lambda.amazonaws.com'),
            inline_policies={
                "LambdaS3BucketAccessPolicy": policy_document
            },
            managed_policies=[
                ManagedPolicy.from_aws_managed_policy_name('service-role/AWSLambdaBasicExecutionRole')]
        )

    def _create_bucket(self, name) -> Bucket:
        return Bucket(self, name,
                         block_public_access=BlockPublicAccess.BLOCK_ALL, encryption=BucketEncryption.S3_MANAGED)

    def _create_lambda(self) -> Function:
        lambda_role = self._create_lambda_role()
        dirname = os.path.dirname(__file__)
        return Function(
            self,
            "GlueDataBrewProfileLambda",
            handler='handler.lambda_handler',
            runtime=Runtime.PYTHON_3_9,
            code=Code.from_asset(os.path.join(dirname, 'handler')),
            role=lambda_role,
            environment={'threshold': '5'}
        )

    def _create_sf_role(self, lambda_arn) -> Role:
        invoke_glue_data_brew_profile_reader = PolicyStatement(
            resources=[lambda_arn],
            effect=Effect.ALLOW,
            actions=[
                'lambda:InvokeFunction'
            ]
        )
        s3_access_policy = PolicyStatement(
            resources=[self.source_bucket.bucket_arn, f"{self.source_bucket.bucket_arn}/*"],
            effect=Effect.ALLOW,
            actions=[
                's3:GetObject',
                's3:ListBucket'
            ]
        )
        target_s3_access_policy = PolicyStatement(
            resources=[self.destination_bucket.bucket_arn,
                       f"{self.destination_bucket.bucket_arn}/*"],
            effect=Effect.ALLOW,
            actions=[
                's3:GetObject',
                's3:PutObject',
                's3:DeleteObject',
                's3:ListBucket',
                's3:PutObjectAcl'
            ]
        )
        states_exec_policy = PolicyStatement(
            resources=["*"],
            effect=Effect.ALLOW,
            actions=[
                'states:StartExecution'
            ]
        )
        policy_document = PolicyDocument(
            statements=[
                invoke_glue_data_brew_profile_reader,
                s3_access_policy,
                target_s3_access_policy,
                states_exec_policy
            ]
        )
        return Role(
            self,
            "StepFunctionPIIRole",
            assumed_by=
            ServicePrincipal('states.amazonaws.com'),
            inline_policies={
                "InvokeLambdaGlueDataBrewProfileReader": policy_document
            },
            managed_policies=[
                ManagedPolicy.from_aws_managed_policy_name('AwsGlueDataBrewFullAccessPolicy')]
        )

    def _create_databrew_role(self, secret) -> Role:
        s3_bucket_access_policy = PolicyStatement(
            resources=[self.source_bucket.bucket_arn, f"{self.source_bucket.bucket_arn}/*",
                       f"arn:aws:s3:::{self.databrew_metadata_bucket}",
                       f"arn:aws:s3:::{self.databrew_metadata_bucket}/*",
                       self.destination_bucket.bucket_arn,
                       f"{self.destination_bucket.bucket_arn}/*"],
            effect=Effect.ALLOW,
            actions=[
                's3:GetObject',
                's3:PutObject',
                's3:DeleteObject',
                's3:ListBucket',
                's3:PutObjectAcl'
            ]
        )
        secrets_access_policy_read = PolicyStatement(
            resources=[secret.secret_arn],
            effect=Effect.ALLOW,
            actions=[
                'secretsmanager:GetResourcePolicy',
                'secretsmanager:GetSecretValue',
                'secretsmanager:DescribeSecret',
                'secretsmanager:ListSecretVersionIds'
            ]
        )
        secrets_access_policy_list = PolicyStatement(
            resources=['*'],
            effect=Effect.ALLOW,
            actions=[
                'secretsmanager:ListSecrets'
            ]
        )
        policy_document = PolicyDocument(
            statements=[
                s3_bucket_access_policy,
                secrets_access_policy_read,
                secrets_access_policy_list
            ]
        )
        return Role(
            self,
            "GlueDataBrewPIIRole",
            assumed_by=
            ServicePrincipal('databrew.amazonaws.com'),
            inline_policies={
                "DataBrewS3BucketsAccess": policy_document
            },
            managed_policies=[
                ManagedPolicy.from_aws_managed_policy_name('service-role/AWSGlueDataBrewServiceRole')]
        )

    def _create_state_machine(self, lambda_arn, databrew_role_arn, secret_arn, sf_role,
                              is_history) -> sf.CfnStateMachine:
        if is_history:
            return sf.CfnStateMachine(
                self,
                'HistoryIngestDataBrewPIIStateMachine',
                role_arn=sf_role,
                definition_string=json.dumps(
                    step_function_config.build_history_ingest_step_function(lambda_arn, self.databrew_metadata_bucket.bucket_name,
                                                                            self.source_bucket.bucket_name, self.destination_bucket.bucket_name,
                                                                            databrew_role_arn, secret_arn))
            )
        else:
            return sf.CfnStateMachine(
                self,
                'OngoingIngestDataBrewPIIStateMachine',
                role_arn=sf_role,
                definition_string=json.dumps(
                    step_function_config.build_ongoing_ingest_step_function(lambda_arn, self.databrew_metadata_bucket.bucket_name,
                                                                            self.source_bucket.bucket_name, self.destination_bucket.bucket_name,
                                                                            databrew_role_arn, secret_arn))
            )

    def _create_secret(self) -> secrets_manager.Secret:
        return secrets_manager.Secret(self, 'GlueDataBrewPIISecret', secret_name='GlueDataBrewPIISecret')

    def _create_sf_rule(self, state_machine):
        step_function_event_bridge_policy = PolicyStatement(
            resources=[state_machine.attr_arn],
            effect=Effect.ALLOW,
            actions=[
                'states:StartExecution'
            ]
        )
        policy_document = PolicyDocument(
            statements=[
                step_function_event_bridge_policy
            ]
        )
        role = Role(
            self,
            "EventBridgePIITaskRole",
            assumed_by=
            ServicePrincipal('events.amazonaws.com'),
            inline_policies={
                "DataBrewS3BucketsAccess": policy_document
            }
        )
        self.source_bucket.enable_event_bridge_notification()

        event_rule = events.CfnRule(self,
                                   id="DataBrewStepFunctionRule",
                                   description="Listen to object creation and update events inside the S3 bucket for Glue DataBrew data input.",
                                   state='ENABLED',
                                   event_pattern={
                                       "detail_type": ["Object Created"],
                                       "source": [
                                           "aws.s3"
                                       ],
                                       "detail": {
                                           "bucket": {
                                               "name": [
                                                   self.source_bucket.bucket_name
                                               ]
                                           }
                                       }
                                   },
                                   name="DataBrewStepFunctionRule",
                                   targets=[
                                       events.CfnRule.TargetProperty(
                                           arn=state_machine.attr_arn,
                                           id="DataBrewStepFunctionRuleTarget",
                                           role_arn=role.role_arn
                                       )
                                   ]
                                   )
