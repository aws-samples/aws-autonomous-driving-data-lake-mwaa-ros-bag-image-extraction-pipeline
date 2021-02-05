METADATA_PROCESSING_KEY="processing.status"
METADATA_PROCESSING_VALUE_FAILURE="failure"
METADATA_PROCESSING_VALUE_COMPLETE="complete"
METADATA_PROCESSING_VALUE_IN_PROGRESS="in progress"

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
    return 'tag_bag_file'


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

    add_s3_tag(METADATA_PROCESSING_KEY, METADATA_PROCESSING_VALUE_IN_PROGRESS, bucket, key)

    #xcom_push for the filename used by check cancel
    name = key.rsplit('/', 1)
    kwargs['ti'].xcom_push(key="file_name", value=name[1])

    return f"Successfully tagged {key} with tags {METADATA_PROCESSING_KEY}:{METADATA_PROCESSING_VALUE_IN_PROGRESS}."


def run_fargate_task(**kwargs):
    """
    Runs the image extraction Fargate task.

    :param kwargs:
    :return:
    """
    import boto3
    import os

    key = kwargs['ti'].xcom_pull(task_ids=f"bag_file_sensor", key=f"filename_s3_key")
    bucket = kwargs['ti'].xcom_pull(task_ids=f"bag_file_sensor", key=f"filename_s3_bucket")

    # TODO where to pull this config, e.g. SecretsManager?
    dest_bucket = kwargs['bucket_dest']
    private_subnets = kwargs['private_subnets']
    fargate_cluster = kwargs['fargate_cluster']
    fargate_task = kwargs['fargate_task']

    client = boto3.client('ecs')
    client.run_task(
        cluster=fargate_cluster,
        launchType='FARGATE',
        taskDefinition=fargate_task,
        count=1,
        platformVersion='LATEST',
        overrides={
            'containerOverrides': [
                {
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
                'subnets': [private_subnets],
                'assignPublicIp': 'DISABLED'
            }
        }
    )

