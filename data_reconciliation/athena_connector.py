from aws_cdk import (
    aws_sam as sam,
    aws_s3 as s3,
    aws_ec2 as ec2,
    aws_athena as athena,
    Fn
)
from aws_cdk.aws_ec2 import IVpc
from aws_cdk.aws_s3 import IBucket

from constructs import Construct


class AthenaConnector(Construct):

    spill_bucket: IBucket
    app_name: str

    @property
    def lambda_function_name(self):
        return f"{self.app_name}-athena-connector"

    @property
    def lambda_function_arn(self):
        return Fn.sub(
            "arn:aws:lambda:ap-southeast-2:${AWS::AccountId}:function:${function_name}",
            {
                "function_name": self.lambda_function_name
            }
        )



    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        app_name: str,
        vpc: IVpc,
        subnet_ids: [str],
        db_secret_prefix: str,
        db_endpoint: str,
        db_port: int,
        db_name: str,
        db_sg: ec2.ISecurityGroup,
    ):
        super().__init__(scope, construct_id)

        self.app_name = app_name

        self.security_group = self._create_security_group(app_name, vpc)
        self._grant_access_to_db(self.security_group, db_sg, db_port)
        self._create_athena_resources(app_name, subnet_ids, db_secret_prefix, db_endpoint, db_port, db_name)
        self._create_athena_datacatalog()


    def _create_security_group(self, app_name, vpc: IVpc):
        return ec2.SecurityGroup(
            self,
            "connector-security-group",
            description="Athena Connector Security Group",
            security_group_name=f"{app_name}-sg",
            vpc=vpc,
            allow_all_outbound=True # Required for Lambda access secret manager
        )

    def _grant_access_to_db(self, connector_sg: ec2.ISecurityGroup, db_sg: ec2.ISecurityGroup, port=2484):
        connector_sg.add_egress_rule(
            description="Allow Egress access to RDS from Athena Connector",
            peer=db_sg,
            connection=ec2.Port.tcp(port)
        )
        db_sg.add_ingress_rule(
            description="Allow Ingress access from Athena Connector",
            peer=connector_sg,
            connection=ec2.Port.tcp(port)
        )

    def _create_athena_resources(self, app_name, subnet_ids, db_secret_prefix, db_endpoint, db_port, db_name):
        self.spill_bucket = s3.Bucket(
            self,
            "SpillBucket",
            bucket_name=f"{app_name}-spill-bucket"
        )

        self.sam_app = sam.CfnApplication(
            self,
            "AthenaOracleConnectorApp",
            location=sam.CfnApplication.ApplicationLocationProperty(
                application_id='arn:aws:serverlessrepo:us-east-1:292517598671:applications/AthenaOracleConnector',
                semantic_version='2023.15.1'
            ),
            parameters= {
                "SpillBucket": self.spill_bucket.bucket_name,
                "SpillPrefix": "athena-spill",
                "LambdaFunctionName": self.lambda_function_name,
                "LambdaMemory": "3008",
                "LambdaTimeout": "900",
                "SecretNamePrefix": db_secret_prefix,
                "SecurityGroupIds": self.security_group.security_group_id,
                "SubnetIds": ",".join(subnet_ids),
                "DefaultConnectionString": f"oracle://jdbc:oracle:thin:${{{db_secret_prefix}}}@//{db_endpoint}:{db_port}/{db_name}"
            }
        )

    def _create_athena_datacatalog(self):
        datacatalog = athena.CfnDataCatalog(
            self,
            "DataCatalog",
            name=self.app_name,
            parameters={
                "catalog": self.app_name,
                "metadata-function": self.lambda_function_arn,
                "record-function": self.lambda_function_arn
            },
            type="LAMBDA"
        )
        datacatalog.node.add_dependency(self.sam_app)