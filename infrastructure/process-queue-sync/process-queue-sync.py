import os
import boto3
import json
import logging
from botocore.exceptions import ClientError
import shutil
import io
import time
from datetime import datetime, timedelta
import re

s3 = boto3.client(
    "s3",
)
dynamo = boto3.client("dynamodb")


def export_json_file(file_labels, bucket, key):

    frame_duration = int(os.environ["frame_duration"])
    
    # This function assumes that the mp4 we processed contains one or more frames, the time of the start
    # of the mp4 can be calculated from the filename index * frame duration
    # Assumption: the S3 prefixes are of the form:
    #

    path_elems = key.split("/")
    file_elems = path_elems[-1].split(".")

    file = f"{file_elems[0]}.json"
    f = io.BytesIO(json.dumps(file_labels).encode())
    upload_path = "/".join(path_elems[0:-1])
    upload_key = f"{upload_path}/{file}"
    logging.info(f"Uploading {bucket}/{upload_key}")
    s3.upload_fileobj(f, bucket, upload_key)

    # calculate the absolute timestamp of this frame based on the prefix name and
    # the file index * the duration per frame
    camera = re.match("[A-Za-z]*", file_elems[0]).group(0)
    file_offset = int(re.search("[0-9]{4}", file_elems[0]).group(0))
    print(f"file offset:{file_offset}")

    # Extract the base time for the .bag file from the S3 prefix
    bt_elems = path_elems[-2].split("_")
    bt_elems = bt_elems[0].split("-")
    frame_time = datetime(*[int(x) for x in bt_elems[0:6]])
    print(f"frame time: {frame_time}")

    # now adjust for the file offset in the file name (e.g. front<offset>.png) as well as
    # the timestamp relative to the the start of the mp4 file
    td = timedelta(milliseconds=(file_offset * frame_duration))
    print(f"td:{td}")
    frame_time = frame_time + td
    print(f"ft_iso: {frame_time.isoformat()}")


    s3_key = "/".join([bucket, key]).replace("mp4", "png")
    table = os.environ["results_table"]

    db_key = {"timestamp": {"S": frame_time.isoformat()}, "camera": {"S": camera}}

    item = {
        "timestamp": {"S": frame_time.isoformat()},
        "camera": {"S": camera},
        "s3_loc": {"S": s3_key},
        #        "json" : {"S" : json.dumps(file_labels)} # handy for development but not necessary as the json is also in S3
    }

    # Put the entry into the table
    dynamo.put_item(TableName=table, Item=item)

    # Upated the table with each detection
    ped_cnt = 0
    bike_cnt = 0
    motorbike_cnt = 0
    for l in file_labels:

        # Keep a count of the number of people/bikes/motorbikes in the image
        name = l["Name"].replace(" ", "_")
        # Ignore items where there's no bounding box
        if "Instances" in l:
            count = len(l["Instances"])
            if count == 0:
                continue
            if name == "Person":
                print(name)
                ped_cnt = ped_cnt + count
            elif name == "Bicycle":
                print(name)
                bike_cnt = bike_cnt + count
            elif name == "Motorcycle":
                print(name)
                motorbike_cnt = motorbike_cnt + count

        update_expression = f"SET {name} = :conf"
        condition_expression = f"attribute_not_exists({name}) OR {name} < :conf"

        #
        try:

            dynamo.update_item(
                TableName=table,
                Key=db_key,
                UpdateExpression=update_expression,
                ConditionExpression=condition_expression,
                ExpressionAttributeValues={":conf": {"N": f'{l["Confidence"]}'}},
            )
        except ClientError as e:
            logging.warning(e)

    try:

        dynamo.update_item(
            TableName=table,
            Key=db_key,
            UpdateExpression=f"SET Ped_Count = :peds, Bike_Count  = :bikes, Motorbike_Count = :motorbikes ",
            ExpressionAttributeValues={
                ":peds": {"N": f"{ped_cnt}"},
                ":bikes": {"N": f"{bike_cnt}"},
                ":motorbikes": {"N": f"{motorbike_cnt}"},
            },
        )
    except ClientError as e:
        logging.warning(e)
    file_labels = []


def process_labels(bucket, key, labels):

    export_json_file(labels, bucket, key)


def lambda_handler(event, context):

    print(event)

    sqs = boto3.client("sqs")
    rek = boto3.client("rekognition")
    ddb = boto3.client("dynamodb")
    s3 = boto3.client("s3")

    messages = event["Records"]

    label_count = 0
    sleep_time = 0
    monitor_table = os.environ["monitor_table"]

    for m in messages:

        body = json.loads(m["body"])
        print(f"Body: {body}")
        if "Records" not in body:
            continue
        bucket = body["Records"][0]["s3"]["bucket"]["name"]
        key = body["Records"][0]["s3"]["object"]["key"]
        s3_loc = "/".join([bucket, key])
        receipt_handle = m["receiptHandle"]
        png_key = key.replace("mp4", "png")

        json_key = key.replace("mp4", "json")
        response = rek.detect_labels(
            Image={"S3Object": {"Bucket": bucket, "Name": png_key}}
        )
        print(response)

        process_labels(bucket, key, response["Labels"])
        ddb.update_item(
            TableName=monitor_table,
            Key={"mp4_file": {"S": s3_loc}},
            UpdateExpression="SET #s = :sts, #e = :now",
            ExpressionAttributeNames={"#s": "Status", "#e": "End"},
            ExpressionAttributeValues={
                ":sts": {"S": "Complete"},
                ":now": {"S": datetime.now().isoformat()},
            },
        )

        sqs.delete_message(
            QueueUrl=os.environ["job_queue_url"], ReceiptHandle=receipt_handle
        )

    return {"status": 200}
        



## for testing...
if __name__ == "__main__":

    event = {
        "Records": [
            {
                "messageId": "17abcf72-d5bf-4624-9df4-ddc5c15c8e1e",
                "receiptHandle": "AQEBkFFUd6jugUQTeCLZIhl39enwF5IkjNJpQWa8KXV2xvZ+C2gWcsnpbMv0OO9ZGtNNIkAQzUqRnPFfTnBk3zA4th+3F+tHwCaWXuWimwYZZ6SQhckxCmpj68UwJmtU/Li0OoNUEyyus+LklZmuQVp2H2ZNgAplxf4cKFlTV1qqMxZdPXn3KF6cJe/fwyDucZq4QXf0MCMQcEBwjQSqCB2nTeCvMdKAi2Jxfckolv8KNwN/DXkPFrSVUNFKeZNV3ytx5/0RcgmpLbfquQ864FSIUXqtKqAmoxELVlbFNfYerHTs1re/lFZVqcOHTFvJkN3aGACwJPPw5aFCFlqsRhH9dkHm+ZX6atHUUl3PbIuOghwtl5mPBpqAPB0XcZeMj1gcglZ9pq4ujNm0UhW5PkKUbSW2/Qm2B4AYU36Wa9xUTJjPkJaAMvpG+oLnQpM/Oqmv",
                "body": '{"Records":[{"eventVersion":"2.1","eventSource":"aws:s3","awsRegion":"eu-west-1","eventTime":"2020-11-11T14:56:58.372Z","eventName":"ObjectCreated:Put","userIdentity":{"principalId":"AWS:AROA5CGYXMF22WA56TEAW:346e01e85bd84c8c88b1139c1d2c6379"},"requestParameters":{"sourceIPAddress":"54.228.164.251"},"responseElements":{"x-amz-request-id":"73F46EA7C23360A3","x-amz-id-2":"0bDQ8c/R3BW3T2mMF4Wg91d7uSuFhbSWKPASmkZduueWEIfV8Gl9BcZmcr4x3bCpmjiViDO5+K7pnZmw/0O7bQQA+nGJ9R7m"},"s3":{"s3SchemaVersion":"1.0","configurationId":"ODhjMGEyYzYtOGYyMC00NzM4LWI5MDktOTM1NmI5NDZjMjgy","bucket":{"name":"my-rosbag-stack-destbucket3708473c-hevpuy70d45q","ownerIdentity":{"principalId":"A1OHAXFWJQKA9R"},"arn":"arn:aws:s3:::my-rosbag-stack-destbucket3708473c-hevpuy70d45q"},"object":{"key":"20201007/20201007_151042/2020-10-07-15-10-42_137/right0111.mp4","size":324301,"eTag":"08750a98a96d063af5f5652b50626851","sequencer":"005FABFBBA849DB250"}}}]}',
                "attributes": {
                    "ApproximateReceiveCount": "5",
                    "SentTimestamp": "1605106619386",
                    "SenderId": "AIDAJQOC3SADRY5PEMBNW",
                    "ApproximateFirstReceiveTimestamp": "1605106619386",
                },
                "messageAttributes": {},
                "md5OfBody": "2bf8bccff2dd18be69776629050c8f64",
                "eventSource": "aws:sqs",
                "eventSourceARN": "arn:aws:sqs:eu-west-1:898102681973:my-rosbag-stack-rekJobQueueFDF53CB9-14W1ZZJ2SAAR4",
                "awsRegion": "eu-west-1",
            }
        ]
    }

    os.environ[
        "job_queue_url"
    ] = "https://sqs.eu-west-1.amazonaws.com/898102681973/my-rosbag-stack-rekJobQueueFDF53CB9-14W1ZZJ2SAAR4"
    os.environ[
        "rek_complete_topic"
    ] = "arn:aws:sns:eu-west-1:898102681973:my-rosbag-stack-RekCompleteTopic5564433B-1I2L0WMVP7N9X"
    os.environ[
        "rek_sns_role"
    ] = "arn:aws:iam::898102681973:role/my-rosbag-stack-rekSNSPublishRoleDA9BC943-1TCSS4BMK6H68"
    os.environ["monitor_table"] = "my-rosbag-stack-RekMonitorA9472196-KZKYF14HYCRF"
    os.environ[
        "results_table"
    ] = "my-rosbag-stack-RekResultsTableC0A5D930-1EVEL6LLFLYWP"

    lambda_handler(event, "")
