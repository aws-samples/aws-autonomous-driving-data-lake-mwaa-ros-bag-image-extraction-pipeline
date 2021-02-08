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
