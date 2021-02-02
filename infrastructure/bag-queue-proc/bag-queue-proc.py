import boto3
import json
import time
import re
import tarfile
import os
import logging
from botocore.exceptions import ClientError
import shutil


def trigger_bag_processing(bucket, dest_bucket, prefix):
    state_machine_arn = os.environ["state_machine_arn"]
    s3_object = dict(
        [("bucket", bucket), ("key", prefix), ("dest_bucket", dest_bucket)]
    )
    now = str(int(time.time()))
    name = prefix + "-sf-" + now
    name = re.sub("\W+", "", name)
    print(s3_object)
    client = boto3.client("stepfunctions")
    try:
        response = client.start_execution(
            stateMachineArn=state_machine_arn, name=name, input=json.dumps(s3_object)
        )
    except client.exceptions.InvalidName as e:
        logging.warning(e)


def lambda_handler(event, context):

    sqs = boto3.client("sqs")
    queue = os.environ["bag_queue_url"]
    dest_bucket = os.environ["dest_bucket"]

    print(event)
    if "Records" in event:
        messages = event["Records"]
        for m in messages:
            print(m)
            body = json.loads(m["body"])
            if 'Records' in body:
                for r in body['Records']:
                    bucket = r["s3"]["bucket"]["name"]
                    prefix = r["s3"]["object"]["key"]
                    print(prefix)
                    if prefix.endswith(".bag"):
                        trigger_bag_processing(bucket, dest_bucket, prefix)
            else:                
                bucket = body["s3BucketArn"].replace("arn:aws:s3:::", "")
                prefix = body["s3Key"]
                print(prefix)
                if prefix.endswith(".bag"):
                    trigger_bag_processing(bucket, dest_bucket, prefix)

    return {"status": 200}


if __name__ == "__main__":
    event_sqs_put = {'Records': [{'messageId': 'd90d1753-e27b-4ec4-9b65-7ce4886e187d', 'receiptHandle': 'AQEBjots6fnzI9CHWOjZ51Ii4EMb2CNXlA6HH6//M7LeRHehkaZCMIRyAOm58YJ83RhUEdE/+X15fJRdi4kI8EIzEwMXAKCC17GVNwKa6tcTU1pjQ7lVF8+2ni592FWImLYsJjlzZNbNZXTPjB/ism5YHo/u0+5bLBBCtF8U4GRLkcs8zomA7tcZuT1RnVisjniAa0iGljCjLx/hzowBf3DX5ETkuyGevONo8zeBepUet3xB39zya45P4KRf7HsmqWVYeQa4+y2Dpi6aABqs2dfnBfNnseH3LgsWfjt30m3K8fyWX+D3NT4B2T4l4btssH5BdMQB4l3IRyJ94NsEdAihw/0MdemCbPldgfI6zsShmtpF+WJ9sa/nHfJTPoMV8zn0e0ivmMyyQr0KiqddrNLO1Wd3eI5XyvuGWa/eTGOsQx0GAP+hmWMIloFgCuvV62CZ', 'body': '{"Records":[{"eventVersion":"2.1","eventSource":"aws:s3","awsRegion":"eu-west-1","eventTime":"2020-11-23T14:27:30.963Z","eventName":"ObjectCreated:CompleteMultipartUpload","userIdentity":{"principalId":"AWS:AROA5CGYXMF2RGDTIQ5FO:davesmn-Isengard"},"requestParameters":{"sourceIPAddress":"88.104.17.245"},"responseElements":{"x-amz-request-id":"13DE08EA5893AD03","x-amz-id-2":"KH7DhCMxqboKvcoUxFLut6RADrrs5pgI2B3/PKOXjMAfqnG7wl5Q4jMY9fZD3vPNvAba62lhglNrRJ+QC9268Z6I6ovQXqeU"},"s3":{"s3SchemaVersion":"1.0","configurationId":"M2FiOGRlNTEtNjQzZC00ZGU0LWFkZWYtYmRiZmQ0M2E4MmYx","bucket":{"name":"my-rosbag-stack-srcbucketa467747d-6hpd0b1ohil1","ownerIdentity":{"principalId":"A1OHAXFWJQKA9R"},"arn":"arn:aws:s3:::my-rosbag-stack-srcbucketa467747d-6hpd0b1ohil1"},"object":{"key":"2020-10-05-11-10-58_0.bag","size":2811855506,"eTag":"49e0e6b2f53839d7f4c6c780040f99c3-168","sequencer":"005FBBC6CACE029534"}}}]}', 'attributes': {'ApproximateReceiveCount': '3', 'SentTimestamp': '1606141652776', 'SenderId': 'AIDAJQOC3SADRY5PEMBNW', 'ApproximateFirstReceiveTimestamp': '1606141652780'}, 'messageAttributes': {}, 'md5OfBody': 'ab1a4d9e18ccdb8e7600c26e60a9bbcc', 'eventSource': 'aws:sqs', 'eventSourceARN': 'arn:aws:sqs:eu-west-1:898102681973:my-rosbag-stack-inputBagQueueC9E70386-1W8YJ8U32CPYG', 'awsRegion': 'eu-west-1'}]}
    
    event_sqs = {
        "Records": [
            {
                "messageId": "1420b27d-53a1-4fa3-a5f3-a7c2cff66bde",
                "receiptHandle": "AQEBMRY5o5zlOtxUlq7gFeyUXyxPc8nWIwjPYdQZ1YYyFhv7kALe54otIobZ/Uu/kacMqTyLXNEkWvYUIm05FbVb4UwAR86S6yq2WPsYNzaPXst/qX91wTvxScQK5UcAw5LEMWZEsfj1ymfmBS+d2LUnpGmi1vIITEspRm/bQbiXDWXGh7Z9gvTLb5JSzfWR6Nke/hMTF6jKoF3kpKpJxB9Mes28fn6DatP8EonHNXpCX8+edaGx3lxpalChn5Sh0K4ICoWGYHBXbB8XV3AjXsTBoCR0fTADbUn1TtaRk/jQtwrSbk40IKGjgr1MVDq4gxejFmpUDetlETwiPTCkR+AU2efkhDkRDExCe4Au/k+2HBvQeCppb+v+cYaLFJJa7WI2EGCvTZa9QoPZgOpa1TSvpecBWqzWFfMPpdm1kmBJUwgscZoMunYRGCpka1JAqtqH",
                "body": '{"s3BucketArn": "arn:aws:s3:::bosch-data-bucket", "s3Key": "20201007/20201007_103222/2020-10-07-10-34-23_19.bag"}',
                "attributes": {
                    "ApproximateReceiveCount": "1",
                    "SentTimestamp": "1603981892647",
                    "SenderId": "AROA5CGYXMF2RGDTIQ5FO:davesmn-Isengard",
                    "ApproximateFirstReceiveTimestamp": "1603981892648",
                },
                "messageAttributes": {},
                "md5OfBody": "4dcba5c1f38f6e1748ccfe557068f576",
                "eventSource": "aws:sqs",
                "eventSourceARN": "arn:aws:sqs:eu-west-1:898102681973:my-rosbag-stack-inputBagQueueC9E70386-1W8YJ8U32CPYG",
                "awsRegion": "eu-west-1",
            }
        ]
    }
    event_bridge = {
        "version": "0",
        "id": "5f573b4e-da0c-7e96-85d6-31d02dde1aca",
        "detail-type": "Step Functions Execution Status Change",
        "source": "aws.states",
        "account": "898102681973",
        "time": "2020-10-29T16:17:33Z",
        "region": "eu-west-1",
        "resources": [
            "arn:aws:states:eu-west-1:898102681973:execution:RunTaskStateMachine2AB9F9D4-hMqR3ykDFzlQ:2020100720201007_10322220201007103423_19bagsf1603987013"
        ],
        "detail": {
            "executionArn": "arn:aws:states:eu-west-1:898102681973:execution:RunTaskStateMachine2AB9F9D4-hMqR3ykDFzlQ:2020100720201007_10322220201007103423_19bagsf1603987013",
            "stateMachineArn": "arn:aws:states:eu-west-1:898102681973:stateMachine:RunTaskStateMachine2AB9F9D4-hMqR3ykDFzlQ",
            "name": "2020100720201007_10322220201007103423_19bagsf1603987013",
            "status": "SUCCEEDED",
            "startDate": 1603987013445,
            "stopDate": 1603988253365,
            "input": '{"bucket": "bosch-data-bucket", "key": "20201007/20201007_103222/2020-10-07-10-34-23_19.bag", "dest_bucket": "my-rosbag-stack-destbucket3708473c-hevpuy70d45q"}',
            "inputDetails": {"included": True},
            "output": '{"Attachments":[{"Details":[{"Name":"subnetId","Value":"subnet-09d7fff377005b461"},{"Name":"networkInterfaceId","Value":"eni-03b1785a080704e73"},{"Name":"macAddress","Value":"0a:fc:12:d7:01:c3"},{"Name":"privateIPv4Address","Value":"10.0.133.29"}],"Id":"49fa84b6-5c16-44cc-9da2-4a39b06a990a","Status":"DELETED","Type":"eni"}],"Attributes":[],"AvailabilityZone":"eu-west-1a","ClusterArn":"arn:aws:ecs:eu-west-1:898102681973:cluster/my-ros-image-cluster","Connectivity":"CONNECTED","ConnectivityAt":1603987018016,"Containers":[{"ContainerArn":"arn:aws:ecs:eu-west-1:898102681973:container/5a1e31cb-89c2-4456-9b80-b2e579118781","Cpu":"0","ExitCode":0,"GpuIds":[],"Image":"898102681973.dkr.ecr.eu-west-1.amazonaws.com/rosbag-repository:latest","ImageDigest":"sha256:1efa5ef75370278ab2526a9647dd642815ac3eadc5e6e7f111b8801f4c27e850","LastStatus":"STOPPED","Memory":"12288","Name":"my-ros-image-container","NetworkBindings":[],"NetworkInterfaces":[{"AttachmentId":"49fa84b6-5c16-44cc-9da2-4a39b06a990a","PrivateIpv4Address":"10.0.133.29"}],"RuntimeId":"677f29de5e5c4f669878cc64fe81bca5-4097625808","TaskArn":"arn:aws:ecs:eu-west-1:898102681973:task/my-ros-image-cluster/677f29de5e5c4f669878cc64fe81bca5"}],"Cpu":"4096","CreatedAt":1603987014253,"DesiredStatus":"STOPPED","ExecutionStoppedAt":1603988239000,"Group":"family:my-ros-image-family","InferenceAccelerators":[],"LastStatus":"STOPPED","LaunchType":"FARGATE","Memory":"12288","Overrides":{"ContainerOverrides":[{"Command":[],"Environment":[{"Name":"s3_source_prefix","Value":"20201007/20201007_103222/2020-10-07-10-34-23_19.bag"},{"Name":"s3_source","Value":"bosch-data-bucket"},{"Name":"s3_destination","Value":"my-rosbag-stack-destbucket3708473c-hevpuy70d45q"}],"EnvironmentFiles":[],"Name":"my-ros-image-container","ResourceRequirements":[]}],"InferenceAcceleratorOverrides":[]},"PlatformVersion":"1.4.0","PullStartedAt":1603987032901,"PullStoppedAt":1603987120901,"StartedAt":1603987124901,"StartedBy":"AWS Step Functions","StopCode":"EssentialContainerExited","StoppedAt":1603988251936,"StoppedReason":"Essential container in task exited","StoppingAt":1603988239071,"Tags":[],"TaskArn":"arn:aws:ecs:eu-west-1:898102681973:task/my-ros-image-cluster/677f29de5e5c4f669878cc64fe81bca5","TaskDefinitionArn":"arn:aws:ecs:eu-west-1:898102681973:task-definition/my-ros-image-family:10","Version":5}',
            "outputDetails": {"included": True},
        },
    }
    os.environ[
        "state_machine_arn"
    ] = "arn:aws:states:eu-west-1:898102681973:stateMachine:RunTaskStateMachine2AB9F9D4-hMqR3ykDFzlQ"
    os.environ["dest_bucket"] = "my-rosbag-stack-destbucket3708473c-hevpuy70d45q"
    os.environ[
        "bag_queue_url"
    ] = "https://sqs.eu-west-1.amazonaws.com/898102681973/my-rosbag-stack-inputBagQueueC9E70386-1W8YJ8U32CPYG"
    lambda_handler(event_sqs_put, "")
