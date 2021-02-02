#!/bin/bash

profile=$1
cmd=$2
build=$3
region=$4

# TODO include profile switch here
export aws_account_id=$(aws --profile "$profile" sts get-caller-identity --query Account --output text)

REPO_NAME=vsi-rosbag-repository # Should match the ecr repository name given in config.json
IMAGE_NAME=my-vsi-ros-image          # Should match the image name given in config.json

pip install -r requirements.txt --use-deprecated=legacy-resolver | grep -v 'already satisfied'
cdk bootstrap aws://$aws_account_id/$region

if [ $build = true ] ;
then
    export repo_url=$aws_account_id.dkr.ecr.$region.amazonaws.com/$REPO_NAME
    docker build ./service -t $IMAGE_NAME:latest
    last_image_id=$(docker images | awk '{print $3}' | awk 'NR==2')
    docker tag $last_image_id $repo_url
    echo docker push $repo_url
    aws ecr describe-repositories --repository-names $REPO_NAME --region $region || aws ecr create-repository --repository-name $REPO_NAME --region $region
    docker push $repo_url
else
  echo Skipping build
fi

cdk $cmd --profile $profile --region $region

