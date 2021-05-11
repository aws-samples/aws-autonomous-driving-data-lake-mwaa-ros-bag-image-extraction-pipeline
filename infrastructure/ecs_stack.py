from aws_cdk import (
    aws_ec2 as ec2,
    aws_s3,
    aws_ecs as ecs,
    aws_ecr as ecr,
    aws_iam,
    aws_secretsmanager,
    aws_mwaa,
    aws_dynamodb as dynamodb,
    core,
    aws_logs,
    aws_s3_deployment,
    aws_ssm
)

import boto3
import os

from os import path
from os.path import dirname, abspath

profile = os.environ["AWS_PROFILE"]
boto3.setup_default_session(profile_name=profile)
account = boto3.client("sts").get_caller_identity().get("Account")
region = boto3.session.Session().region_name


class RosbagProcessor(core.Stack):
    def __init__(
            self,
            scope: core.Construct,
            id: str,
            image_name: str,
            ecr_repository_name: str,
            memory_limit_mib: int,
            cpu: int,
            **kwargs,
    ) -> None:
        """
        Creates the following infrastructure:

            2 S3 Buckets
                - "src" bucket will be monitored for incoming data, and each incoming file will trigger an ECS Task
                - "dest" bucket will be the destination for saving processed data from the ECS Task
                - "dags" bucket will contain MWAA dags

                - These bucket names are automatically passed as environment variables to your docker container
                    In your docker container, access these bucket names via:

                    import os
                    src_bucket = os.environ["s3_source"]
                    dest_bucket = os.environ["s3_destination"]

            MWAA environment

            ECS Fargate Cluster
                - Using Fargate, this cluster will not cost any money when no tasks are running

            ECS Fargate Task

            ECS Task Role - used by the docker container
                - Read access to the "-in" bucket and write access to the "-out" bucket

            VPC "MyVpc"
                Task will be run in this VPC's private subnets

            ECR Repository
                - reference to the repository hosting the service's docker image

            ECR Image
                - reference to the service's docker image in the ecr repo

            ECS Log Group for the ECS Task
                f'{image_name}-log-group'

        :param scope:
        :param id:
        :param kwargs:
        """
        super().__init__(scope, id, **kwargs)

        src_bucket = aws_s3.Bucket(
            self,
            removal_policy=core.RemovalPolicy.DESTROY,
            id="src-bucket",
            encryption=aws_s3.BucketEncryption.KMS_MANAGED,
            block_public_access=aws_s3.BlockPublicAccess.BLOCK_ALL
        )

        aws_ssm.CfnParameter(
            self,
            id="rosbag-bag-src",
            type="String",
            value=src_bucket.bucket_name,
            name="/mwaa/rosbag/bag-src"
        )

        dest_bucket = aws_s3.Bucket(
            self,
            id="dest-bucket",
            removal_policy=core.RemovalPolicy.DESTROY,
            encryption=aws_s3.BucketEncryption.KMS_MANAGED,
            block_public_access=aws_s3.BlockPublicAccess.BLOCK_ALL
        )

        aws_ssm.CfnParameter(
            self,
            id="rosbag-bag-dest",
            type="String",
            value=dest_bucket.bucket_name,
            name="/mwaa/rosbag/bag-dest"
        )

        dag_bucket = aws_s3.Bucket(
            self,
            id="airflow-dag-bucket",
            bucket_name=f"airflow-dags-{self.stack_name}-{self.account}",
            removal_policy=core.RemovalPolicy.DESTROY,
            encryption=aws_s3.BucketEncryption.KMS_MANAGED,
            block_public_access=aws_s3.BlockPublicAccess.BLOCK_ALL
        )

        # Upload MWAA files to S3
        s3_deployment = aws_s3_deployment.BucketDeployment(
            self,
            id="airflow-dag-plugins",
            destination_bucket=dag_bucket,
            sources=[aws_s3_deployment.Source.asset(path.join(dirname(abspath(__file__)), '../plugins'))],
            destination_key_prefix="plugins"
        )

        s3_deployment = aws_s3_deployment.BucketDeployment(
            self,
            id="airflow-dag-requirements",
            destination_bucket=dag_bucket,
            sources=[aws_s3_deployment.Source.asset(path.join(dirname(abspath(__file__)), '../requirements'))],
            destination_key_prefix="requirements"
        )

        s3_deployment = aws_s3_deployment.BucketDeployment(
            self,
            id="airflow-dag-dags",
            destination_bucket=dag_bucket,
            sources=[aws_s3_deployment.Source.asset(path.join(dirname(abspath(__file__)), '../dags'))],
            destination_key_prefix="dags"
        )

        # Create VPC and Fargate Cluster
        vpc = ec2.Vpc(self, f"MyVpc", max_azs=2)

        # Add VPC endpoints
        vpc.add_interface_endpoint("ecr", service=ec2.InterfaceVpcEndpointAwsService.ECR)
        vpc.add_interface_endpoint("ecr_docker", service=ec2.InterfaceVpcEndpointAwsService.ECR_DOCKER)
        vpc.add_interface_endpoint("ec2_messages", service=ec2.InterfaceVpcEndpointAwsService.EC2_MESSAGES)
        vpc.add_interface_endpoint("cloudwatch", service=ec2.InterfaceVpcEndpointAwsService.CLOUDWATCH)
        vpc.add_interface_endpoint("cloudwatch_logs", service=ec2.InterfaceVpcEndpointAwsService.CLOUDWATCH_LOGS)
        vpc.add_gateway_endpoint("s3", service=ec2.GatewayVpcEndpointAwsService('s3'))

        private_subnets = ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE)

        # vpc private subnets parameter
        aws_ssm.CfnParameter(
            self,
            id="private-subnets",
            type="String",
            value=",".join(list(map(lambda x: x.subnet_id, vpc.private_subnets))),
            name="/mwaa/rosbag/private-subnets"
        )

        ecs_task_role = aws_iam.Role(
            self,
            "ecs_task_role2",
            assumed_by=aws_iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
            managed_policies=[
                aws_iam.ManagedPolicy.from_aws_managed_policy_name(
                    "CloudWatchFullAccess"
                )
            ],
        )

        ecs_task_role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=[
                    "kms:Decrypt",
                    "kms:Encrypt",
                    "kms:ReEncrypt*",
                    "kms:DescribeKey",
                    "kms:GenerateDataKey",
                ],
                resources=["*"],
            )
        )

        ecs_task_role.add_to_policy(
            aws_iam.PolicyStatement(actions=["s3:Get*", "s3:List*"], resources=["*"])
        )

        ecs_task_role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=["s3:List*", "s3:PutObject*"], resources=["*"]
            )
        )

        # Define task definition with a single container
        # The image is built & published from a local asset directory
        task_definition = ecs.FargateTaskDefinition(
            self,
            f"{image_name}_task_definition",
            family=f"{image_name}-family",
            cpu=cpu,
            memory_limit_mib=memory_limit_mib,
            task_role=ecs_task_role,
        )

        # task definition parameters
        aws_ssm.CfnParameter(
            self,
            id="task-definition-arn",
            type="String",
            value=task_definition.task_definition_arn,
            name="/mwaa/rosbag/task-definition-arn"
        )

        repo = ecr.Repository.from_repository_name(
            self, id=id, repository_name=ecr_repository_name
        )

        img = ecs.EcrImage.from_ecr_repository(repository=repo, tag="latest")

        logs = ecs.LogDriver.aws_logs(
            stream_prefix="ecs",
            log_group=aws_logs.LogGroup(self, f"{image_name}-log-group2"),
        )

        container_name = f"{image_name}-container"

        container_def = task_definition.add_container(
            container_name,
            image=img,
            memory_limit_mib=memory_limit_mib,
            environment={"topics_to_extract": "/tf"},
            logging=logs,
        )

        # Define an ECS cluster hosted within the requested VPC
        cluster = ecs.Cluster(
            self,
            "cluster",
            cluster_name=f"{image_name}-cluster",
            container_insights=True,
            vpc=vpc,
        )

        # fargate cluster paramater
        aws_ssm.CfnParameter(
            self,
            id="rosbag-fargate-cluster",
            type="String",
            value=cluster.cluster_arn,
            name="/mwaa/rosbag/fargate-cluster-arn"
        )


        ## setup the rekogition labelling pipeline..
        # create a DynamoDB table to hold the results
        rek_labels_db = dynamodb.Table(
            self,
            "RekResultsTable2",
            partition_key=dynamodb.Attribute(
                name="timestamp", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="camera", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
        )

        # dynamo db table parameter
        aws_ssm.CfnParameter(
            self,
            id="dynamo-rek-results",
            type="String",
            value=rek_labels_db.table_name,
            name="/mwaa/rosbag/dynamo-rek-results"
        )

        # create a DynamoDB table to monitor/debug the pipeline
        rek_monitor_db = dynamodb.Table(
            self,
            "RekMonitor",
            partition_key=dynamodb.Attribute(
                name="mp4_file", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
        )

        # MWAA execution role
        mwaa_exec_role = aws_iam.Role(
            self,
            "mwaa_exec_role",
            assumed_by=aws_iam.ServicePrincipal("airflow.amazonaws.com"),
            managed_policies=[
                # TODO narrow down
                aws_iam.ManagedPolicy.from_aws_managed_policy_name("job-function/DataScientist"),
                aws_iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess"),
                aws_iam.ManagedPolicy.from_aws_managed_policy_name("AmazonSQSFullAccess"),
                aws_iam.ManagedPolicy.from_aws_managed_policy_name("CloudWatchFullAccess"),
            ],
        )

        mwaa_exec_role.assume_role_policy.add_statements(
            aws_iam.PolicyStatement(
                actions=["sts:AssumeRole"],
                effect=aws_iam.Effect.ALLOW,
                principals=[aws_iam.ServicePrincipal("airflow-env.amazonaws.com")]
            )
        )

        # TODO narrow down
        mwaa_exec_role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=[
                    "logs:*",
                ],
                effect=aws_iam.Effect.ALLOW,
                resources=["*"],
            )
        )

        mwaa_exec_role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=[
                    "ecs:*",
                ],
                effect=aws_iam.Effect.ALLOW,
                resources=["*"],
            )
        )

        mwaa_exec_role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=[
                    "airflow:*",
                ],
                effect=aws_iam.Effect.ALLOW,
                resources=["*"],
            )
        )

        mwaa_exec_role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=[
                    "iam:PassRole",
                ],
                effect=aws_iam.Effect.ALLOW,
                resources=["*"],
            )
        )

        mwaa_exec_role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=[
                    "rekognition:*",
                ],
                effect=aws_iam.Effect.ALLOW,
                resources=["*"],
            )
        )

        mwaa_exec_role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=[
                    "kms:Decrypt",
                    "kms:DescribeKey",
                    "kms:GenerateDataKey*",
                    "kms:Encrypt"
                ],
                effect=aws_iam.Effect.ALLOW,
                not_resources=[f"arn:aws:kmw:*:{self.account}:key/*"],
                conditions={
                    "StringLike": {
                        "kms:ViaService": [
                            f"sqs.{self.region}.amazonaws.com"
                        ]
                    }
                }
            )
        )

        # task_definition defautl container parameter
        aws_ssm.CfnParameter(
            self,
            id="task-definition-default-container-name",
            type="String",
            value=task_definition.default_container.container_name,
            name="/mwaa/rosbag/task-definition-default-container-name"
        )

        # MWAA environment
        mwaa_subnet_ids = list(map(lambda x: x.subnet_id, vpc.private_subnets))
        mwaa_environment = core.CfnResource(
            self,
            id="mwaa-environment",
            type="AWS::MWAA::Environment",
            properties={
                "Name": "mwaa-environment",
                "NetworkConfiguration": {
                    "SubnetIds": mwaa_subnet_ids,
                    "SecurityGroupIds": [vpc.vpc_default_security_group]
                },
                "LoggingConfiguration": {
                    "DagProcessingLogs": {
                        "Enabled": "true",
                        "LogLevel": "INFO"
                    },
                    "SchedulerLogs": {
                        "Enabled": "true",
                        "LogLevel": "INFO"
                    },
                    "WebserverLogs": {
                        "Enabled": "true",
                        "LogLevel": "INFO"
                    },
                    "WorkerLogs": {
                        "Enabled": "true",
                        "LogLevel": "INFO"
                    },
                    "TaskLogs": {
                        "Enabled": "true",
                        "LogLevel": "INFO"
                    }
                },
                "SourceBucketArn": dag_bucket.bucket_arn,
                "DagS3Path": "dags",
                "PluginsS3Path": "plugins/plugins.zip",
                "RequirementsS3Path": "requirements/requirements.txt",
                "ExecutionRoleArn": mwaa_exec_role.role_arn,
                "WebserverAccessMode": "PUBLIC_ONLY",
                "MaxWorkers": 25,
                "EnvironmentClass": "mw1.large"
            }
        )