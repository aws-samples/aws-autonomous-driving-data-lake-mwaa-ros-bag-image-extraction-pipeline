## Introduction
# Rosbag image processing using Amazon Managed Workflows for Apache Airflow (MWAA)
This solution describes a workflow that processes ROS bag files on Amazon S3 (https://aws.amazon.com/s3/), extracts individual PNGs from a video stream using AWS Fargate (https://aws.amazon.com/fargate) on Amazon Elastic Container Service (https://aws.amazon.com/ecs) (Amazon ECS), uses Amazon Rekognition (https://aws.amazon.com/rekognition/) to detect objects within the exported images and draws bounding boxes for the detected objects within the images. For more information about this solution, including more information about this solution please refer to this blog post.
https://aws.amazon.com/blogs/architecture/field-notes-deploying-autonomous-driving-and-adas-workloads-at-scale-with-amazon-managed-workflows-for-apache-airflow/
At the end of this deployment and testing, you will have an original image and image with detected objects as shown below.

**Image extracted from ROS bag file:**

![output_extracted](./outputs/left0193_original.png)

**Image after highlighting bounding boxes of detected objects:**

![output_annotated](./outputs/left0193_labeled.png)

### Recording reuse

The Industry Kit Program team reports on the impact that industry kits have for the AWS field and our customers. Don't forget to record reuse for every customer you show this to.

In order to do so, click the "Record project reuse" button on this kit’s BuilderSpace page and enter the SFDC opportunity ID or paste the link to your SFDC opportunity.

![reuse](/assets/reuse.png)

## Support

If you notice a defect, or need support with deployment or demonstrating the kit, create an Issue here: [https://gitlab.aws.dev/industry-kits/automotive/autonomous-vehicle-datalake/-/issues]

## Prerequisities

* Prepare an AWS account (https://aws.amazon.com/premiumsupport/knowledge-center/create-and-activate-aws-account/) to be used for the installation
* AWS CLI (https://aws.amazon.com/cli/) installed and configured to point to AWS account (https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html)
* Use a named profile (https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-profiles.html) to access your AWS account
* Install the AWS Cloud Development Kit (https://aws.amazon.com/cdk/)
* Our example uses Python version 3.9 or newer
* Docker (https://www.docker.com/) installed and running
* Download the sample Rosbag file from the assets folder.

## Architecture

![](/assets/Architecture-for-Deploying-Autonomous-Driving-ADAS-workloads-at-scale-3.jpg)

## Deployment

- Make a ECR repo, with the desired name such as "vsi-rosbag-repository-3"

- Make sure the repo name that you have created, matches the repo name in the deploy.sh file. Also the repo name (vsi-rosbag-repository-3) and the image name (my-vsi-ros-image-3) in the deploy.sh file and config.json respectively needs to match.

- In a terminal, start the deployment by typing
./deploy.sh <named-profile> deploy true <region>
Replace <named-profile> with the named profile which you configured for your AWS CLI to point to your target AWS account.
Replace <region> with the region that is associated with your named profile.
As an example: If your named profile for the AWS CLI is called rosbag-processing and your associated region is us-east-1 the resulting command would be:
./deploy.sh rosbag-processing deploy true us-east-1

- After confirming the deployment CDK will take several minutes to synthesise the CloudFormation templates, build the Docker image and deploy the solution to the target AWS account.

- Alternatively, if you have a cloud9 environment, and using the default, please use the command ./deploy1.sh rosbag-processing deploy true us-east-1

![](/assets/pic1.jpg)

## Testing and inspecting results
****Step 1:** To start the pipeline**, the ROS bag file needs to uploaded to the s3 bucket from the assets folder. 
Copy the ROS bag file to S3.You can download the ROS bag file here. After downloading you need to copy the file to the input bucket that was created during the CDK deployment in the previous step.
Open the AWS management console and navigate to the service S3. You should find a list of your buckets including the buckets for input data, output data and DAG definitions that were created during the CDK deployment.

![](/assets/pic.jpg)

Select the bucket with prefix rosbag-processing-stack-srcbucket and copy the ROS bag file into the bucket.
Now that the ROS bag file is in S3, we need to enable the image processing workflow in MWAA.

****Step 2:** Manage the ROS bag processing DAG(Directed Acyclic Graphs) from the Airflow UI**
In order to access the Airflow web server, open the AWS management console and navigate to the MWAA service.
From the overview, select the MWAA environment that was created and select Open Airflow UI as shown in the following screenshot:

![](/assets/pic2.jpg)

The Airflow UI and the ROS bag image processing DAG should appear as in the following screenshot:

We can now enable the DAG by flipping the switch (see picture below) from “Off” to “On” position. 

![](/assets/pic3.jpg)

Amazon MWAA will now schedule the launch for the workflow. By selecting the DAG’s name (rosbag_processing) and selecting the Graph View, you can review the workflow’s individual tasks:

![](/assets/pic4.jpg)

After enabling the image processing DAG by flipping the switch to “on” in the previous step, Airflow will detect the newly uploaded ROS bag file in S3 and start processing it. You can monitor the progress from the DAG’s tree view , as shown in the following screenshot:

![](/assets/pic5.jpg)

**Step 2a:** (Optional)
Re-running the image processing pipeline
Our solution uses S3 metadata to keep track of the processing status of each ROS bag file in the input bucket in S3. If you want to re-process a ROS bag file:

open the AWS management console,
navigate to S3, click on the ROS bag file you want to re-process,
remove the file’s metadata tag processing.status from the properties tab:

![](/assets/pic6.jpg)

With the metadata tagprocessing.status removed, Amazon MWAA picks up the file the next time the image processing workflow is launched. You can manually start the workflow by choosing Trigger DAG from the Airflow web server UI.

![](/assets/pic7.jpg)

**Step 3: Inspect the results**
In the AWS management console navigate to S3, and select the output bucket with prefix rosbag-processing-stack-destbucket. The bucket holds both the extracted images from the ROS bag’s topic containing the video data and the images with highlighted bounding boxes for the objects detected by Amazon Rekognition.

![](/assets/pic8.jpg)


Navigate to the folder 20201005 and select an image,
Navigate to the folder bounding_boxes
Select the corresponding image. You should receive an output similar to the following two examples(see image below):
Left: Image extracted from ROS bag camera feed topic. Right: Same image with highlighted objects that Amazon Rekognition detected.
Left: Image extracted from ROS bag camera feed topic. Right: Same image with highlighted objects that Amazon Rekognition detected.

![](/assets/pic9.jpg)

Amazon CloudWatch for monitoring and creating operational dashboard
Amazon MWAA comes with CloudWatch metrics to monitor your MWAA environment. You can create dashboards for quick and intuitive operational insights. To do so:

Open the AWS management console and navigate to the service CloudWatch.
Select the menu “Dashboards” from the navigation pane and click on “Create dashboard”.
After providing a name for your dashboard and selecting a widget type, you can choose from a variety of metrics that MWAA provides.  This is shown in the following picture.
MWAA comes with a range of pre-defined metrics that allow you to monitor failed tasks, the health of your managed Airflow environment and helps you right-size your environment.

![](/assets/pic10.jpg)

## Clean Up
In order to delete the deployed solution from your AWS account, you must:

**1. Delete all data in your S3 buckets**

In the management console navigate to the service S3. You should see three buckets containing rosbag-processing-stack that were created during the deployment.
Select each bucket and delete its contents. Note: Do not delete the bucket itself.

**2. Delete the solution’s CloudFormation stack**

You can delete the deployed solution from your AWS account either from the management console or the command line interface.

Using the management console:

Open the management console and navigate to the CloudFormation service. Make sure to select the same region you deployed your solution to (for exmaple,. us-east-1).
Select stacks from the side menu. The solution’s CloudFormation stack is shown in the following screenshot:

![](/assets/pic10.jpg)

Select the rosbag-processing-stack and select Delete. CloudFormation will now destroy all resources related to the ROS bag image processing pipeline.

Using the Command Line Interface (CLI):

Open a shell and navigate to the directory from which you started the deployment, then type the following:

cdk destroy --profile <named-profile>
Bash
Replace <named-profile> with the named profile which you configured for your AWS CLI to point to your target AWS account.

As an example: if your named profile for the AWS CLI is called rosbag-processing and your associated Region is us-east-1 the resulting command would be:

cdk destroy --profile rosbag-processing
Bash
After confirming that you want to destroy the CloudFormation stack, the solution’s resources will be deleted from your AWS account.

## **Conclusion**

This readme illustrated how to deploy and leverage workflows for running Autonomous Driving & ADAS workloads at scale. This solution was designed using fully managed AWS services. The solution was deployed on AWS using AWS CDK templates. Using MWAA we were able to set up a centralized, transparent, intuitive workflow orchestration across multiple systems. Even though the pipeline involved several steps in multiple AWS services, MWAA helped us understand and visualize the workflow, its individual tasks and dependencies.

Usually, distributed systems are hard to debug in case of errors or unexpected outcomes. However, building the ROS bag processing pipelines on MWAA allowed us to inspect the log files involved centrally from the Airflow UI, for debugging or auditing. Furthermore, with MWAA the ROS bag processing workflow is part of the code base and can be tested, refactored, versioned just like any other software artifact. By providing the CDK templates, we were able to architect a fully automated solution which you can adapt to your use case.

We hope you found this interesting and helpful and invite your comments on the solution!

### Deployment Video (optional)

### Deploymeny FAQ (optional)
