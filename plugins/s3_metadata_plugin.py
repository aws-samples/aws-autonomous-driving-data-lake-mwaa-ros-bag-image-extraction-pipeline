from airflow.plugins_manager import AirflowPlugin
from sensors.s3_metadata_sensor import *


class S3MetadataPlugin(AirflowPlugin):
    name = 's3_metadata_plugin'

    sensors = [S3MetadataSensor]