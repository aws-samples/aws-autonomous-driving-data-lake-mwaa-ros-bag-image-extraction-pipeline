# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

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

            S3 Buckets
                - "src" bucket will hold incoming data
                - "dest" bucket will be the destination for saving processed data
                - "dags" bucket will contain MWAA dags

            MWAA environment
                - managed Apache Airflow environment for workflow orchestration

            ECS Fargate Cluster
                - Using Fargate, this cluster will not cost any money when no tasks are running

            ECS Fargate Task

            ECS Task Role - used by the docker container
                - Read access to the "-in" bucket and write access to the "-out" bucket

            VPC
                - Task will be run in this VPC's private subnets
                - MWAA will use this VPC

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
        plugins_deployment = aws_s3_deployment.BucketDeployment(
            self,
            id="airflow-dag-plugins",
            destination_bucket=dag_bucket,
            sources=[aws_s3_deployment.Source.asset(path.join(dirname(abspath(__file__)), '../plugins'))],
            destination_key_prefix="plugins"
        )

        requirements_deployment = aws_s3_deployment.BucketDeployment(
            self,
            id="airflow-dag-requirements",
            destination_bucket=dag_bucket,
            sources=[aws_s3_deployment.Source.asset(path.join(dirname(abspath(__file__)), '../requirements'))],
            destination_key_prefix="requirements"
        )

        dags_deployment = aws_s3_deployment.BucketDeployment(
            self,
            id="airflow-dag-dags",
            destination_bucket=dag_bucket,
            sources=[aws_s3_deployment.Source.asset(path.join(dirname(abspath(__file__)), '../dags'))],
            destination_key_prefix="dags"
        )

        # Create VPC and Fargate Cluster
        vpc = ec2.Vpc(
            self,
            id="mwaa-vpc",
            cidr="10.192.0.0/16",
            max_azs=2,
            nat_gateways=1,
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="public", cidr_mask=24,
                    reserved=False, subnet_type=ec2.SubnetType.PUBLIC),
                ec2.SubnetConfiguration(
                    name="private", cidr_mask=24,
                    reserved=False, subnet_type=ec2.SubnetType.PRIVATE)
            ],
            enable_dns_hostnames=True,
            enable_dns_support=True
        )

        # Add VPC endpoints
        vpc.add_interface_endpoint("airflow-api", service=ec2.InterfaceVpcEndpointAwsService(name="airflow.api"))
        vpc.add_interface_endpoint("airflow-env", service=ec2.InterfaceVpcEndpointAwsService(name="airflow.env"))
        vpc.add_interface_endpoint("airflow-ops", service=ec2.InterfaceVpcEndpointAwsService(name="airflow.ops"))
        vpc.add_interface_endpoint("ecr", service=ec2.InterfaceVpcEndpointAwsService.ECR)
        vpc.add_interface_endpoint("ecr_docker", service=ec2.InterfaceVpcEndpointAwsService.ECR_DOCKER)
        vpc.add_interface_endpoint("ec2_messages", service=ec2.InterfaceVpcEndpointAwsService.EC2_MESSAGES)
        vpc.add_interface_endpoint("cloudwatch", service=ec2.InterfaceVpcEndpointAwsService.CLOUDWATCH)
        vpc.add_interface_endpoint("cloudwatch_logs", service=ec2.InterfaceVpcEndpointAwsService.CLOUDWATCH_LOGS)
        vpc.add_interface_endpoint("sqs", service=ec2.InterfaceVpcEndpointAwsService.SQS)
        vpc.add_interface_endpoint("sns", service=ec2.InterfaceVpcEndpointAwsService.SNS)
        vpc.add_gateway_endpoint("s3", service=ec2.GatewayVpcEndpointAwsService('s3'))

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
            aws_iam.PolicyStatement(
                actions=[
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                    "logs:DescribeLogStreams",
                ],
                resources=["arn:aws:logs:*:*:*"],
            )
        )

        ecs_task_role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=[
                    "s3:GetObject",
                    "s3:GetObjectAcl",
                    "s3:ListBucket",
                ],
                resources=[
                    f"{src_bucket.bucket_arn}/*",
                    f"{src_bucket.bucket_arn}",
                    f"{dest_bucket.bucket_arn}/*",
                    f"{dest_bucket.bucket_arn}"
                ]
            )
        )

        ecs_task_role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=[
                    "s3:PutObject",
                    "s3:PutObjectAcl"
                ],
                resources=[
                    f"{dest_bucket.bucket_arn}/*"
                ]
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

        # task_definition default container parameter
        aws_ssm.CfnParameter(
            self,
            id="task-definition-default-container-name",
            type="String",
            value=task_definition.default_container.container_name,
            name="/mwaa/rosbag/task-definition-default-container-name"
        )

        # MWAA environment
        # Create MWAA IAM Policies and Roles
        mwaa_policy_document = aws_iam.PolicyDocument(
            statements=[
                aws_iam.PolicyStatement(
                    actions=["airflow:PublishMetrics"],
                    effect=aws_iam.Effect.ALLOW,
                    resources=[f"arn:aws:airflow:{self.region}:{self.account}:environment/mwaa-environment"],
                ),
                aws_iam.PolicyStatement(
                    actions=[
                        "s3:PutObject",
                        "s3:PutObjectAcl",
                        "s3:GetObject",
                        "s3:GetObjectAcl",
                        "s3:ListBucket",
                        "s3:GetObjectTagging",
                        "s3:PutObjectTagging"
                    ],
                    effect=aws_iam.Effect.ALLOW,
                    resources=[
                        f"{dag_bucket.bucket_arn}/*",
                        f"{dag_bucket.bucket_arn}",
                        f"{src_bucket.bucket_arn}/*",
                        f"{src_bucket.bucket_arn}",
                        f"{dest_bucket.bucket_arn}/*",
                        f"{dest_bucket.bucket_arn}"
                    ],
                ),
                aws_iam.PolicyStatement(
                    actions=[
                        "kms:Decrypt",
                        "kms:Encrypt",
                        "kms:ReEncrypt*",
                        "kms:DescribeKey",
                        "kms:GenerateDataKey",
                    ],
                    resources=["*"],
                ),
                aws_iam.PolicyStatement(
                    actions=[
                        "logs:CreateLogStream",
                        "logs:CreateLogGroup",
                        "logs:PutLogEvents",
                        "logs:GetLogEvents",
                        "logs:GetLogRecord",
                        "logs:GetLogGroupFields",
                        "logs:GetQueryResults"
                    ],
                    effect=aws_iam.Effect.ALLOW,
                    resources=[
                        f"arn:aws:logs:{self.region}:{self.account}:log-group:airflow-mwaa-environment-*"],
                ),
                aws_iam.PolicyStatement(
                    actions=[
                        "logs:DescribeLogGroups"
                    ],
                    effect=aws_iam.Effect.ALLOW,
                    resources=["*"],
                ),
                aws_iam.PolicyStatement(
                    actions=[
                        "sqs:ChangeMessageVisibility",
                        "sqs:DeleteMessage",
                        "sqs:GetQueueAttributes",
                        "sqs:GetQueueUrl",
                        "sqs:ReceiveMessage",
                        "sqs:SendMessage"
                    ],
                    effect=aws_iam.Effect.ALLOW,
                    resources=[f"arn:aws:sqs:{self.region}:*:airflow-celery-*"],
                ),
                aws_iam.PolicyStatement(
                    actions=[
                        "dynamodb:PutItem",
                        "dynamodb:UpdateItem"
                    ],
                    effect=aws_iam.Effect.ALLOW,
                    resources=[
                        f"{rek_monitor_db.table_arn}",
                        f"{rek_labels_db.table_arn}"
                    ],
                ),
                aws_iam.PolicyStatement(
                    actions=[
                        "ssm:GetParameter"
                    ],
                    effect=aws_iam.Effect.ALLOW,
                    resources=["*"],
                ),
                aws_iam.PolicyStatement(
                    actions=[
                        "rekognition:DetectLabels"
                    ],
                    effect=aws_iam.Effect.ALLOW,
                    resources=["*"],
                ),
                aws_iam.PolicyStatement(
                    actions=[
                        "ecs:RunTask",
                        "ecs:DescribeTasks",
                        #"airflow:*",
                    ],
                    effect=aws_iam.Effect.ALLOW,
                    resources=["*"],
                ),
                aws_iam.PolicyStatement(
                    actions=[
                        "iam:PassRole"
                    ],
                    effect=aws_iam.Effect.ALLOW,
                    resources=[
                        f"{task_definition.obtain_execution_role().role_arn}",
                        f"{ecs_task_role.role_arn}"
                    ],
                )
            ]
        )

        mwaa_service_role = aws_iam.Role(
            self,
            "mwaa-service-role",
            assumed_by=aws_iam.CompositePrincipal(
                aws_iam.ServicePrincipal("airflow.amazonaws.com"),
                aws_iam.ServicePrincipal("airflow-env.amazonaws.com"),
            ),
            inline_policies={"CDKmwaaPolicyDocument": mwaa_policy_document},
            path="/service-role/"
        )

        mwaa_service_role.assume_role_policy.add_statements(
            aws_iam.PolicyStatement(
                actions=["sts:AssumeRole"],
                effect=aws_iam.Effect.ALLOW,
                principals=[aws_iam.ServicePrincipal("airflow-env.amazonaws.com")]
            )
        )

        mwaa_logging_conf = aws_mwaa.CfnEnvironment.LoggingConfigurationProperty(
            task_logs=aws_mwaa.CfnEnvironment.ModuleLoggingConfigurationProperty(enabled=True, log_level="INFO"),
            worker_logs=aws_mwaa.CfnEnvironment.ModuleLoggingConfigurationProperty(enabled=True, log_level="INFO"),
            scheduler_logs=aws_mwaa.CfnEnvironment.ModuleLoggingConfigurationProperty(enabled=True, log_level="INFO"),
            dag_processing_logs=aws_mwaa.CfnEnvironment.ModuleLoggingConfigurationProperty(enabled=True, log_level="INFO"),
            webserver_logs=aws_mwaa.CfnEnvironment.ModuleLoggingConfigurationProperty(enabled=True, log_level="INFO")
        )

        mwaa_security_group = ec2.SecurityGroup(
            self,
            id="mwaa-sg",
            vpc=vpc,
            security_group_name="mwaa-sg"
        )
        mwaa_security_group.connections.allow_internally(ec2.Port.all_traffic(), "MWAA")

        mwaa_subnet_ids = list(map(lambda x: x.subnet_id, vpc.private_subnets))
        mwaa_network_configuration = aws_mwaa.CfnEnvironment.NetworkConfigurationProperty(
            security_group_ids=[mwaa_security_group.security_group_id],
            subnet_ids=mwaa_subnet_ids,
        )

        mwaa_environment = aws_mwaa.CfnEnvironment(
            self,
            id="mwaa-environment",
            dag_s3_path="dags",
            environment_class="mw1.small",
            execution_role_arn=mwaa_service_role.role_arn,
            logging_configuration=mwaa_logging_conf,
            name="mwaa-environment",
            network_configuration=mwaa_network_configuration,
            max_workers=25,
            plugins_s3_path="plugins/plugins.zip",
            requirements_s3_path="requirements/requirements.txt",
            source_bucket_arn=dag_bucket.bucket_arn,
            webserver_access_mode="PUBLIC_ONLY"
        )
        mwaa_environment.node.add_dependency(plugins_deployment)
        mwaa_environment.node.add_dependency(requirements_deployment)
        mwaa_environment.node.add_dependency(dags_deployment)