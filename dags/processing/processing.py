METADATA_PROCESSING_KEY="processing.status"
METADATA_PROCESSING_VALUE_FAILURE="failure"
METADATA_PROCESSING_VALUE_COMPLETE="complete"
METADATA_PROCESSING_VALUE_IN_PROGRESS="in progress"
import boto3
def add_s3_tag(tag_key, tag_value, bucket, key):
    import boto3

    print(f"Setting key {tag_key} to value {tag_value}.")
    s3_client = boto3.client('s3')
    response = s3_client.put_object_tagging(
        Bucket=bucket,
        Key=key,
        Tagging={
            'TagSet': [
                {
                    'Key': tag_key,
                    'Value': tag_value
                }
            ]
        }
    )

    print(f"Metadata setting response {response}")


def determine_workload(**kwargs):
    """
    Determines if there is work to be done. It checks the xcom data for entries from the upstream sensor.
    If no such data can be found, it returns the name of the 'no_work' branch, otherwise it'll return the
    'tag_manifest_file' branch's name

    :param kwargs:
    :return: Name of the next task to execute, depending on the metadata in xcom
    """

    print(f"Check xcom for metadata.")

    key = kwargs['ti'].xcom_pull(task_ids=f"bag_file_sensor", key=f"filename_s3_key")
    bucket = kwargs['ti'].xcom_pull(task_ids=f"bag_file_sensor", key=f"filename_s3_bucket")

    if not (key and bucket):
        print(f"No unprocessed bag file could be found. Stopping DAG execution")
        return 'no_work'

    print(f"Found unprocessed bag files. Continuing DAG execution.")
    return 'tag_bag_file_in_process'


def tag_bag(**kwargs):
    """
    Adds metadata to bag file.

    :param kwargs:
    :return: Name of the next task to execute, depending on the metadata in xcom
    """
    print(f"Pulling metadata info from xcom.")
    key = kwargs['ti'].xcom_pull(task_ids=f"bag_file_sensor", key=f"filename_s3_key")
    bucket = kwargs['ti'].xcom_pull(task_ids=f"bag_file_sensor", key=f"filename_s3_bucket")
    print(f"Pulled key {key} in bucket {bucket} from xcom.")
    value = kwargs['tag_value']

    add_s3_tag(METADATA_PROCESSING_KEY, value, bucket, key)

    return f"Successfully tagged {key} with tags {METADATA_PROCESSING_KEY}:{value}."


def run_fargate_task(**kwargs):
    """
    Runs the image extraction Fargate task.

    :param kwargs:
    :return:
    """
    import boto3

    key = kwargs['ti'].xcom_pull(task_ids=f"bag_file_sensor", key=f"filename_s3_key")
    bucket = kwargs['ti'].xcom_pull(task_ids=f"bag_file_sensor", key=f"filename_s3_bucket")

    # TODO where to pull this config, e.g. SecretsManager?
    dest_bucket = kwargs['bucket_dest']
    private_subnets = kwargs['private_subnets'].split(",")
    fargate_cluster = kwargs['fargate_cluster']
    fargate_task_arn = kwargs['fargate_task_arn']
    fargate_task_name = kwargs['fargate_task_name']

    client = boto3.client('ecs')
    response = client.run_task(
        cluster=fargate_cluster,
        launchType='FARGATE',
        taskDefinition=fargate_task_arn,
        count=1,
        platformVersion='LATEST',
        overrides={
            'containerOverrides': [
                {
                    'name': fargate_task_name,
                    'environment': [
                        {
                            'name': 's3_source',
                            'value': bucket,
                        },
                        {
                            'name': 's3_source_prefix',
                            'value': key,
                        },
                        {
                            'name': 's3_destination',
                            'value': dest_bucket,
                        },
                        {
                            'name': 'topics_to_extract',
                            'value': '/tf',
                        }
                    ]
                }
            ]
        },
        networkConfiguration={
            'awsvpcConfiguration': {
                'subnets': private_subnets,
                'assignPublicIp': 'DISABLED'
            }
        }
    )

    # push task arn to xcom
    task_arn = response["tasks"][0]["taskArn"]
    print(f"Pushing task arn {task_arn} to key 'extract_task_arn'.")
    kwargs['ti'].xcom_push(key="extract_task_arn", value=task_arn)

    # wait for task to be in 'running' state
    # will wait max 10mins for task to enter 'running' state before failing
    print(f"Waiting for task {task_arn} to enter 'running' state.")
    waiter = client.get_waiter('tasks_running')
    waiter.wait(cluster=fargate_cluster, tasks=[task_arn])


def wait_for_extraction(**kwargs):
    """
    Waits for image extraction to finish and returns next task to execute

    :param kwargs:
    :return:
    """

    # retrieve task id
    task_arn = kwargs['ti'].xcom_pull(task_ids=f"extract_png", key=f"extract_task_arn")
    fargate_cluster = kwargs['fargate_cluster']

    # poll task id until stopped
    import boto3
    client = boto3.client('ecs')
    response = client.describe_tasks(cluster=fargate_cluster, tasks=[task_arn])

    import time
    polls = round(60 * 30 / 10)
    while (response["tasks"][0]["lastStatus"] != "STOPPED") and polls > 0:
        polls -= 1
        time.sleep(10)
        response = client.describe_tasks(cluster=fargate_cluster, tasks=[task_arn])
        print(f"Checking Fargate task status.\nLast queried status is {response['tasks'][0]['lastStatus']}.")

    # final check
    response = client.describe_tasks(cluster=fargate_cluster, tasks=[task_arn])
    last_status = response["tasks"][0]["lastStatus"]

    if last_status == "STOPPED":
        return "extraction_success"

    return "extraction_failed"


#
#   TODO: split responsibilities
#
def process_labels(file_labels, bucket, key, table_target):
    import boto3
    import json
    import logging
    from botocore.exceptions import ClientError
    import io
    from datetime import datetime, timedelta
    import re

    s3 = boto3.client("s3")
    dynamo = boto3.client("dynamodb")

    # TODO: pull from secrets manager
    frame_duration = 67

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


    db_key = {"timestamp": {"S": frame_time.isoformat()}, "camera": {"S": camera}}

    item = {
        "timestamp": {"S": frame_time.isoformat()},
        "camera": {"S": camera},
        "s3_loc": {"S": key},
    }

    # Put the entry into the table
    dynamo.put_item(TableName=table_target, Item=item)

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
                TableName=table_target,
                Key=db_key,
                UpdateExpression=update_expression,
                ConditionExpression=condition_expression,
                ExpressionAttributeValues={":conf": {"N": f'{l["Confidence"]}'}},
            )
        except ClientError as e:
            logging.warning(e)

    try:
        dynamo.update_item(
            TableName=table_target,
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


def list_objects(bucket, prefix):
    import boto3
    s3_client = boto3.client("s3")

    print(f"Will scan for files in s3://{bucket}/{prefix}")
    paginator = s3_client.get_paginator('list_objects_v2')
    iterator = paginator.paginate(
        Bucket=bucket,
        Prefix=prefix,
        PaginationConfig={
            "PageSize": 100
        }
    )

    return iterator


def label_images(**kwargs):
    """
    Sends images to Rekognition for labeling

    :param kwargs:
    :return:
    """
    import boto3

    # list PNG files in S3
    prefix = kwargs['ti'].xcom_pull(task_ids=f"bag_file_sensor", key=f"filename_s3_key")[:-4] + "/"
    bucket = kwargs['bucket_dest']
    table = kwargs['table_dest']

    iterator = list_objects(bucket, prefix)

    # send to Rekognition for labeling
    rek_client = boto3.client("rekognition")
    for page in iterator:
        for object in page["Contents"]:
            key = object["Key"]
            if key.endswith(".png"):
                response = rek_client.detect_labels(Image={"S3Object": {"Bucket": bucket, "Name": key}})
                process_labels(response["Labels"], bucket, key, table)


def save_to_s3(image, bucket, key):
    import boto3

    s3 = boto3.client("s3")
    s3.upload_fileobj(
        image,
        bucket,
        f"bounding_boxes/{key}"
    )


def draw_bounding_box(bucket, json_key):
    """
    Given a Rekognition json file all detected bounding boxes will be written into the related image

    :param bucket:
    :param key:
    :return:
    """

    import boto3
    import json
    import io
    from PIL import Image, ImageDraw

    s3 = boto3.resource("s3")

    # load json from S3
    json_object = s3.Object(bucket, json_key)
    json_content = json.loads(json_object.get()['Body'].read().decode('utf-8'))

    # load matching image key
    png_key = json_key.replace(".json", ".png")
    stream = io.BytesIO(s3.Object(bucket, png_key).get()['Body'].read())
    image = Image.open(stream)

    imgWidth, imgHeight = image.size
    draw = ImageDraw.Draw(image)

    for item in json_content:
        for inst in item["Instances"]:
            box = inst['BoundingBox']
            left = imgWidth * box['Left']
            top = imgHeight * box['Top']
            width = imgWidth * box['Width']
            height = imgHeight * box['Height']

            points = (
                (left, top),
                (left + width, top),
                (left + width, top + height),
                (left, top + height),
                (left, top)

            )
            draw.line(points, fill='#00d400', width=2)
            in_mem_image = io.BytesIO()
            image.save(in_mem_image, format=image.format)
            in_mem_image.seek(0)

            save_to_s3(in_mem_image, bucket, png_key)




def draw_bounding_boxes(**kwargs):
    """
    Draws images with bounding boxes of the objects Rekognition found

    :param kwargs:
    :return:
    """
    # list PNG files in S3
    prefix = kwargs['ti'].xcom_pull(task_ids=f"bag_file_sensor", key=f"filename_s3_key")[:-4] + "/"
    bucket = kwargs['bucket_dest']

    iterator = list_objects(bucket, prefix)
    for page in iterator:
        for object in page["Contents"]:
            key = object["Key"]
            if key.endswith(".json"):
                draw_bounding_box(bucket, key)

def get_parameter(parameter):
    ssm = boto3.client('ssm')
    parameter = ssm.get_parameter(Name='/mwaa/rosbag/bag-src')
    return(parameter['Parameter']['Value'])

