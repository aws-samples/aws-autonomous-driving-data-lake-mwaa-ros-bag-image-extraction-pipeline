from engine import parse_bag
import os
import subprocess


if __name__ == "__main__":
    args = {
        "s3_bag_file_bucket": os.environ["s3_source"],
        "s3_output_bucket": os.environ["s3_destination"],
        "s3_bag_file_prefix": os.environ["s3_source_prefix"],
        "topics_to_extract": os.environ["topics_to_extract"],
    }
    print(args)
    subprocess.Popen(["/opt/ros/melodic/bin/roslaunch export.launch"], shell=True)
    print("spawned roslaunch")

    parse_bag(args)
