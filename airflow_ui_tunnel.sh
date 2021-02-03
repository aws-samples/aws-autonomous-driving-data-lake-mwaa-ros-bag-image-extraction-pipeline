#!/bin/bash
cd "$(dirname "$0")"

if test "$#" -ne 2; then
    echo "Specify:"
    echo "\t 1) AWS CLI profile name"
    echo "\t 2) AWS region"
    exit
fi

PROFILE=$1
REGION=$2

bastion_instance_id=`aws --profile $PROFILE --region $REGION ec2 describe-instances --filters 'Name=tag:airflow_function,Values=bastion' 'Name=instance-state-code,Values=16' | jq -r '.Reservations[0].Instances[0].InstanceId'`
echo "Bastion Instance ID: ${bastion_instance_id}"

bastion_az=`aws --profile $PROFILE --region $REGION ec2 describe-instances --filters 'Name=tag:airflow_function,Values=bastion' 'Name=instance-state-code,Values=16' | jq -r '.Reservations[0].Instances[0].Placement.AvailabilityZone'`
echo "Bastion AZ: ${bastion_az}"

# TODO: retrieve Airflow UI ip -- HOW?

aws --profile $PROFILE --region $REGION ec2-instance-connect send-ssh-public-key --instance-id $bastion_instance_id --availability-zone $bastion_az --instance-os-user ec2-user --ssh-public-key file://$HOME/.ssh/id_rsa.pub
ssh -i /$HOME/.ssh/id_rsa -L 8888:$webserver_ip:8080 ec2-user@$bastion_instance_id
