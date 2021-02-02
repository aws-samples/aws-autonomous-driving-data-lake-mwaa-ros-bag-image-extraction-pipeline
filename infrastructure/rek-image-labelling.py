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
    aws_sqs,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_lambda,
    aws_lambda_event_sources,
    custom_resources as cr,
    core,
    aws_logs,
)

from lambda_function import lambda_code
import boto3
import os

account = boto3.client("sts").get_caller_identity().get("Account")
region = boto3.session.Session().region_name


class ImageLabelling(core.Stack):
    def __init__(
        self,
        scope: core.Construct,
        id: str,
        image_name: str,
        sqs_writer_role: aws_iam.Role,
        **kwargs
    ) -> None:

        super().__init__(scope, id, **kwargs)
