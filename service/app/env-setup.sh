#!/bin/bash

source /opt/ros/melodic/setup.bash
echo $ROS_PACKAGE_PATH
export PYTHONPATH=$PYTHONPATH:$ROS_PACKAGE_PATH
