#!/usr/bin/env python3

from aws_cdk import core
from infrastructure.ecs_stack import RosbagProcessor
import os
import json
import shutil

#zip plugin
shutil.make_archive('plugins/plugins', 'zip', 'plugins/')

# Load config
project_dir = os.path.dirname(os.path.abspath(__file__))

config_file = os.path.join(project_dir, "config.json")

with open(config_file) as json_file:
    config = json.load(json_file)

print(config)

image_name = config["image-name"]
stack_id = config["stack-id"]
ecr_repository_name = config["ecr-repository-name"]
cpu = config["cpu"]
memory_limit_mib = config["memory-limit-mib"]

app = core.App()

RosbagProcessor(
    app,
    stack_id,
    image_name=image_name,
    ecr_repository_name=ecr_repository_name,
    cpu=cpu,
    memory_limit_mib=memory_limit_mib
)

app.synth()
