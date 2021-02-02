from aws_cdk import (
    aws_ec2 as ec2,
    aws_s3,
    aws_s3_notifications as s3n,
    aws_ecs as ecs,
    aws_ecr as ecr,
    aws_efs as efs,
    aws_events,
    aws_events_targets as targets,
    aws_iam,
    aws_sns,
    aws_sns_subscriptions as sns_subs,
    aws_sqs,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_lambda,
    aws_lambda_event_sources as les,
    aws_dynamodb as dynamodb,
    custom_resources as cr,
    core,
    aws_logs,
)

import boto3
import os
import json

from os import path
from os.path import dirname, abspath

# TODO read profile from env
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
        environment_vars: dict,
        memory_limit_mib: int,
        cpu: int,
        timeout_minutes: int,
        s3_filters: list,
        input_bucket_name: str,
        output_bucket_name: str,
        **kwargs,
    ) -> None:
        """
        Creates the following infrastructure:

            2 S3 Buckets
                - "src" bucket will be monitored for incoming data, and each incoming file will trigger an ECS Task
                - "dest" bucket will be the destination for saving processed data from the ECS Task
                # TODO hschoen add S3 bucket for DAGS
                - "dags" bucket will contain MWAA dags

                - These bucket names are automatically passed as environment variables to your docker container
                    In your docker container, access these bucket names via:

                    import os
                    src_bucket = os.environ["s3_source"]
                    dest_bucket = os.environ["s3_destination"]

            # TODO hschoen
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

            Step Function to execute the ECSRunFargateTask command

            Lambda Function to proces an S3 batch job.
                - This is used for development so we can repetedly run .bag files though the pipeline

            SQS queue of bag files waiting to be extracted by a Fargate task. This is written to by the
            S£ batch lambda and is also subscribed to S3 PUT events in the source bucket

            Lamda Function to pull .bag file details from the SQS queue and start the StepFunction

            Trigger to take .png files in the destination bucket and put them into an SQS queue of Rekogition labelling tasks

            Lambda Function to pull png details from the SQS queue and call Rekognition synchronously to label the images in the video. JSON 
            containing the labels is written back to S3

            SQS queue, subscribed to the S3 .json PUT events

            Lambda Function to pull the label data from Rekognition using the job IDs. This gets the labels for each frames,
                calculates which .png files corresponds to a particular frame and saves one .json file of


        :param scope:
        :param id:
        :param image_name:
        :param image_dir:
        :param build_args:
        :param memory_limit_mib: RAM to allocate per task
        :param cpu: CPUs to allocate per task
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

        dest_bucket = aws_s3.Bucket(
            self,
            id="dest-bucket",
            removal_policy=core.RemovalPolicy.DESTROY,
            encryption=aws_s3.BucketEncryption.KMS_MANAGED,
            block_public_access=aws_s3.BlockPublicAccess.BLOCK_ALL
        )

        dag_bucket = aws_s3.Bucket(
            self,
            id="dag-bucket",
            removal_policy=core.RemovalPolicy.DESTROY,
            encryption=aws_s3.BucketEncryption.KMS_MANAGED,
            block_public_access=aws_s3.BlockPublicAccess.BLOCK_ALL
        )

        # Create VPC and Fargate Cluster
        # NOTE: Limit AZs to avoid reaching resource quotas
        vpc = ec2.Vpc(self, f"MyVpc", max_azs=2)

        private_subnets = ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE)

        # EFS
        fs = efs.FileSystem(
            self,
            "efs",
            vpc=vpc,
            removal_policy=core.RemovalPolicy.DESTROY,
            throughput_mode=efs.ThroughputMode.BURSTING,
            performance_mode=efs.PerformanceMode.MAX_IO,
        )

        access_point = fs.add_access_point(
            "AccessPoint",
            path="/lambda",
            create_acl=efs.Acl(owner_uid="1001", owner_gid="1001", permissions="750"),
            posix_user=efs.PosixUser(uid="1001", gid="1001"),
        )

        # ECS Task Role
        arn_str = "arn:aws:s3:::"

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

        ecs_task_role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=["*"], resources=[access_point.access_point_arn]
            )
        )

        ecs_task_role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=[
                    "elasticfilesystem:ClientMount",
                    "elasticfilesystem:ClientWrite",
                    "elasticfilesystem:DescribeMountTargets",
                ],
                resources=["*"],
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

        run_task = tasks.EcsRunTask(
            self,
            "fargatetask",
            assign_public_ip=False,
            subnets=private_subnets,
            cluster=cluster,
            launch_target=tasks.EcsFargateLaunchTarget(
                platform_version=ecs.FargatePlatformVersion.VERSION1_4
            ),
            task_definition=task_definition,
            container_overrides=[
                tasks.ContainerOverride(
                    container_definition=task_definition.default_container,
                    environment=[
                        tasks.TaskEnvironmentVariable(
                            name=k, value=sfn.JsonPath.string_at(v)
                        )
                        for k, v in environment_vars.items()
                    ],
                )
            ],
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            input_path=sfn.JsonPath.entire_payload,
            output_path=sfn.JsonPath.entire_payload,
            timeout=core.Duration.minutes(timeout_minutes),
        )
        run_task.add_retry(
            backoff_rate=1, interval=core.Duration.seconds(60), max_attempts=1920
        )

        state_logs = aws_logs.LogGroup(self, "stateLogs")
        state_machine = sfn.StateMachine(
            self,
            "RunTaskStateMachine",
            definition=run_task,
            timeout=core.Duration.minutes(timeout_minutes),
            logs=sfn.LogOptions(destination=state_logs),
        )

        state_machine.grant_task_response(ecs_task_role)

        input_bag_queue = aws_sqs.Queue(
            self, "inputBagQueue", visibility_timeout=core.Duration.minutes(5)
        )
        # send .png object created events to our SQS input queue
        src_bucket.add_event_notification(
            aws_s3.EventType.OBJECT_CREATED,
            s3n.SqsDestination(input_bag_queue),
            aws_s3.NotificationKeyFilter(suffix="bag"),
        )

        # Create the SQS queue for input/results jobs and SNS for job completion notifications
        dlq = aws_sqs.Queue(self, "dlq")
        rek_job_queue = aws_sqs.Queue(
            self, "rekJobQueue", visibility_timeout=core.Duration.minutes(2)
        )
        rek_results_queue = aws_sqs.Queue(
            self, "rekResultQueue", visibility_timeout=core.Duration.minutes(5)
        )

        ## TESTING - this lamda is for development and allows us to push a bunch of .bag files through
        # without having to copy them into teh src bucket. Create a manifest and then use that with 
        # an S3 batch job to run this.
        s3_batch_lambda = aws_lambda.Function(
            self,
            "S3Batchprocessor",
            code=aws_lambda.Code.from_asset("./infrastructure/S3Batch"),
            environment={
                "bag_queue_url": input_bag_queue.queue_url,
                "job_queue_url": rek_job_queue.queue_url,
            },
            memory_size=3008,
            timeout=core.Duration.minutes(5),
            vpc=vpc,
            retry_attempts=0,
            handler="s3batch.lambda_handler",
            runtime=aws_lambda.Runtime("python3.7", supports_inline_code=True),
            security_groups=fs.connections.security_groups,
        )

        input_bag_queue.grant_send_messages(s3_batch_lambda.role)
        rek_job_queue.grant_send_messages(s3_batch_lambda.role)
        s3_batch_lambda.add_to_role_policy(
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
        s3_batch_lambda.add_to_role_policy(
            aws_iam.PolicyStatement(
                actions=["s3:List*", "s3:Get*", "s3:PutObject"], resources=["*"]
            )
        )

        bag_queue_lambda = aws_lambda.Function(
            self,
            "BagQueueProcessor",
            code=aws_lambda.Code.from_asset("./infrastructure/bag-queue-proc"),
            environment={
                "bag_queue_url": input_bag_queue.queue_url,
                "state_machine_arn": state_machine.state_machine_arn,
                "dest_bucket": dest_bucket.bucket_name,
            },
            memory_size=3008,
            timeout=core.Duration.minutes(5),
            vpc=vpc,
            retry_attempts=0,
            handler="bag-queue-proc.lambda_handler",
            runtime=aws_lambda.Runtime("python3.7", supports_inline_code=True),
            security_groups=fs.connections.security_groups,
        )
        # SQS queue of .bag files to be processed
        bag_queue_lambda.add_event_source(les.SqsEventSource(input_bag_queue))
        input_bag_queue.grant_consume_messages(s3_batch_lambda.role)
        bag_queue_lambda.add_to_role_policy(
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
        bag_queue_lambda.add_to_role_policy(
            aws_iam.PolicyStatement(
                actions=["s3:List*", "s3:Get*", "s3:PutObject"], resources=["*"]
            )
        )
        bag_queue_lambda.add_to_role_policy(
            aws_iam.PolicyStatement(
                actions=["states:StartExecution"],
                resources=[state_machine.state_machine_arn],
            )
        )

        ## setup the rekogition labelling pipeline..

        # create a DynamoDB table to hold the rseults
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

        # create a DynamoDB table to monitor/debug the pipeline
        rek_monitor_db = dynamodb.Table(
            self,
            "RekMonitor",
            partition_key=dynamodb.Attribute(
                name="mp4_file", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
        )

        # send .png object created events to our SQS input queue
        dest_bucket.add_event_notification(
            aws_s3.EventType.OBJECT_CREATED,
            s3n.SqsDestination(rek_job_queue),
            aws_s3.NotificationKeyFilter(suffix="png"),
        )

        # Lambda to call Rekogition DetectLabels API syncronously 
        process_rek_sync_lambda = aws_lambda.Function(
            self,
            "RekSyncProcessor",
            code=aws_lambda.Code.from_asset("./infrastructure/process-queue-sync"),
            environment={
                "job_queue_url": rek_job_queue.queue_url,
                "results_table": rek_labels_db.table_name,
                "monitor_table": rek_monitor_db.table_name,
                "frame_duration": "67",
            },
            memory_size=3008,
            # reserved_concurrent_executions=20,
            timeout=core.Duration.minutes(2),
            vpc=vpc,
            retry_attempts=0,
            handler="process-queue-sync.lambda_handler",
            runtime=aws_lambda.Runtime("python3.7", supports_inline_code=True),
            security_groups=fs.connections.security_groups,
        )

        process_rek_sync_lambda.add_event_source(les.SqsEventSource(rek_job_queue))
        rek_labels_db.grant_read_write_data(process_rek_sync_lambda.role)
        rek_monitor_db.grant_read_write_data(process_rek_sync_lambda.role)
        process_rek_sync_lambda.add_to_role_policy(
            aws_iam.PolicyStatement(
                actions=[
                    "rekognition:DetectLabels",
                ],
                resources=["*"],
            )
        )
        process_rek_sync_lambda.add_to_role_policy(
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
        process_rek_sync_lambda.add_to_role_policy(
            aws_iam.PolicyStatement(
                actions=["s3:List*", "s3:Get*", "s3:PutObject"], resources=["*"]
            )
        )

        # Lambda to anonymize the images which contain a VRU
        anon_labelling_imgs = aws_s3.Bucket(
            self,
            id="anon-labelling-imgs",
            removal_policy=core.RemovalPolicy.DESTROY,
            encryption=aws_s3.BucketEncryption.KMS_MANAGED,
        )

        pillow_layer = aws_lambda.LayerVersion(
            self,
            "PillowLayer",
            code=aws_lambda.Code.from_asset(path.join(dirname(abspath(__file__)), 'pillow-layer')),
            compatible_runtimes=[aws_lambda.Runtime("python3.6")],
        )

        select_labelling_imgs = aws_lambda.Function(
            self,
            "SelectLabellingImgs",
            code=aws_lambda.Code.from_asset("./infrastructure/select-labelling-imgs"),
            environment={
                "image_bucket": anon_labelling_imgs.bucket_name,
            },
            memory_size=3008,
            timeout=core.Duration.minutes(10),
            vpc=vpc,
            retry_attempts=0,
            handler="select-labelling-imgs.lambda_handler",
            runtime=aws_lambda.Runtime("python3.6"),
            security_groups=fs.connections.security_groups,
        )

        select_labelling_imgs.add_layers(pillow_layer)

        dest_bucket.add_event_notification(
            aws_s3.EventType.OBJECT_CREATED,
            s3n.LambdaDestination(select_labelling_imgs),
            aws_s3.NotificationKeyFilter(suffix="json"),
        )

        select_labelling_imgs.add_to_role_policy(
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
        select_labelling_imgs.add_to_role_policy(
            aws_iam.PolicyStatement(
                actions=["s3:List*", "s3:Get*", "s3:PutObject"], resources=["*"]
            )
        )
        select_labelling_imgs.add_to_role_policy(
            aws_iam.PolicyStatement(
                actions=["rekognition:DetectText", "rekognition:DetectFaces"],
                resources=["*"],
            )
        )
