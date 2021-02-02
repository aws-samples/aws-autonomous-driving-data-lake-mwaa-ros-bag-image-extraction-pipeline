import boto3
import json
import time
from datetime import datetime
import logging
import os


# This is for testing the pipeline - it allows us to inject messages at various point using S3 batch

# Send .bag files to the bag file ingest queue
def proc_bag(event):
    bag_queue = os.environ["bag_queue_url"]
    sqs = boto3.client("sqs")

    print(event)
    m = {}
    m["Id"] = f"{time.process_time()}".replace(".", "-")
    entries = []
    for e in event["tasks"]:
        m_body = {}
        m_body["s3BucketArn"] = e["s3BucketArn"]
        m_body["s3Key"] = e["s3Key"]
        m["MessageBody"] = json.dumps(m_body)
        entries.append(m)

    response = sqs.send_message_batch(QueueUrl=bag_queue, Entries=entries)
    return response


# send mp4 files to the rejkogition processing queue
def proc_mp4(event):
    job_queue = os.environ["job_queue_url"]
    sqs = boto3.client("sqs")

    print(event)
    m = {}
    m["Id"] = f"{time.process_time()}".replace(".", "-")
    entries = []
    for e in event["tasks"]:
        m_body = {}
        bucket = e["s3BucketArn"].replace("arn:aws:s3:::", "")
        m_body["s3"] = {"bucket": {"name": bucket}, "object": {"key": e["s3Key"]}}
        m["MessageBody"] = json.dumps({"Records": [m_body]})
        entries.append(m)

    print(entries)
    response = sqs.send_message_batch(QueueUrl=job_queue, Entries=entries)
    print(response)
    return response


def lambda_handler(event, context):

    print(event)

    if "mp4" in event["tasks"][0]["s3Key"]:

        response = proc_mp4(event)
    else:
        response = proc_bag(event)

    return {"status": 200}


if __name__ == "__main__":
    os.environ[
        "bag_queue"
    ] = "https://sqs.eu-west-1.amazonaws.com/898102681973/my-rosbag-stack-inputBagQueueC9E70386-1W8YJ8U32CPYG"
    os.environ[
        "job_queue_url"
    ] = "https://sqs.eu-west-1.amazonaws.com/898102681973/my-rosbag-stack-rekJobQueueFDF53CB9-14W1ZZJ2SAAR4"

    event = {
        "invocationSchemaVersion": "1.0",
        "invocationId": "AAAAAAAAAAGAS5yzNx3A9Gi2IigLAaPHIFY+Qqb4bWdmkGWOOGNjIhVz0YlrFeXhJtiXayhUNoRi5AMXwh35IbMCPuYxBeF9p/4uIPnchGN1B5guRxdfR3MqsYjvHgp9DWXapmwYQTI+ZHH9Kc5XzIxPgBx5kvl+l7w1ExP0aIr3d921QIca2B9lnjRxrgBAJIXBS16+/sub6/E2xbvkD7aUwrDpewrUMMUzGuWEWJ1ejZShmEvIDAKp5zi9OvrXFRXqv2RU9oRnVpKAD6JruBRskoJGqiG8DnJRyZjpPG0ubo/jCt09pBEZoUaG3mVD0H2kMT9ApP4V+Fc3AphkjosK/6kPCmkDAyxyPXWHgPj3gKcALmLJ/VgVGUYWlIPJ9Pkd7Ab4ZZgrdmFTWgKETeJVZJxWcssGppzNBl2xnz5DxDYXh904iTeplv49jLv0T74SDspPJfHCbVgmyEMQp6vnvTYMx+VimMTM8t3HYcT95SG15fO7XpGOZNE94n6086HYN5s2JNAFd5yFT/m8HpIt89euZoGbgLVZscf4QiudUK+iAQBnyQg/FVmKmpA3i5jf3gTMFtsU0+zMYuqLYwtxwQyt0L/W5Q6dtNXzOUMv+MUHJAbdlGItByN957064DoIcxs5+QgYpD+459LhtkJh2KnvsRrERhS8JNuIZ92VhUZ0NMc9xM1O91sgJH8tukX3fjtGT/egmXJKtNaRq9+czkLDlyBlphERBOo1e4gv64dZ4MFW7jpuJlWTTqwdHESxj+qt3xuDlK/wL3wWZGnBPZE3Zs04ATkw9PPj1k9Bz90QqtW526VsDyI66JXrDCSKhDAd5VZjpLWYwRp+PwoxlLBIwtfCWGKBoiH2iO4dQcx7J5agCyDVLLBY1WwKRaXcTEswetwHF6JrPBMXN/Nocr3dBPChm4JK8sMKVUzqWS6Sc1i8xuSrXjMOcevK+E08uQlVJoeT2i0mFKKIqY7DRQzCksBncWdO2+J2FFYgwk2Lr5aZMVrlQTpSNd2HUqzcTkAQVIYS482/vI0gfxbl/q7FzMmLpSbGCbbk8OSRK8C+Xn/hAAEqxDln536cfF6DF73ILbosNkPQNljN+xX0nGUrEV7xgx+Oc/Lpykq2fbmIy99NVnviO0OovDtYm0nksRHSq30gm4qLvuO5Zpc/pGaPpJuMuOy2io4hw1rJYMQnTwFnXvALa0VvWbb2fntLd/uGWHYAn9woDaknTDBgV10a+DDbSxfgs4KN5azpMFRxl8jppONiSvrQ9UFh5kiDsjV4+dOzjVuZdw6asWRb7OShBd8Vv7FRapw5E8OQ41yJ2yovn4qgUm+4Jce69vD9E5B3rgkdXJG129h7LRlNJm1IVTKjKMG70wZJ4Nw9LmtN4Ajc38/mNMKnfDN4r4BbUT2xxLJeL+bauSFDXkkazWsFlypM7zpmCMeG/HYnZfk54tv2CrPJXq9dpe07qqbkDmsBJuQVY8WHzvAqrcSm8sHmdIfJi9/yWj3TjLe1+B597Asl+p/K719RHWeJeJX8wNicjF0Kp83/+sGfxNuxPGJlzb39jsMn1EZ0kyJsVEjcQYfbvY+3CjLCvTXEGD/0PCw6EvE1CIfiWmDIDcBxqCwkC/uuXqx/5URnqSktAmpgWMG/mnFHuBsyPAWrS5H4labU4ldtG97SinHpm4JMdRHUL47j9EH7ZF4EN4mtU0MRrU4Mb2C/XIM8bHv/XD8e5Rp5ix1u7eafgb2gnbclmPhTfUryMNHLr3NBo2DsGZMPBnjf1rfkfcL1aXzq9jt6L1S82qJI3BWPxoyrZ9m8irlZAIIn30r1KnjBNn+mVRQwUnNPvIcCV64jWBV0mr4lov3WO/E=",
        "job": {"id": "d9bed646-7b17-4b2e-a7e4-9744888ca9f0"},
        "tasks": [
            {
                "taskId": "AAAAAAAAAAEqCGpRv/6wzQ/b85B5GXqM9JeONdiXWZqxLNKF/1Mupob/CPZFHjQUvB5/FhvnL4qDxSpXbBwjYoCzV7WdNHAlGrIVFhX7quD7lWjnWoz2ncYz7KnLifHdUsC6p5mmaEjKitOS8DOptogkIKVdDLHDh4S+RPmsAa9luvFQWlCuISNDgdxW+P2GhQSvJQVqSBJVOvoSwHNqZWTmdxVH8IpX7HoPIxiv5kgLlbQmCoXiAt6POgXqWoO3AfVo8N8qaIG5/fP9Aox1fVPSEd6qZfMbI1b8bZBL+CBL+xph+Bz55IoIwoAP4MfGqwWNU/nsmVhJg1Px/LoklFmRG2lGTjUeGUTugUjPnag9xYUQ/V6rD33f4mNcFF7+SsBjGSvwGX780LuUljOEp6i7kUZr/WugG+m3JEsgXTepz6PAjkfCuuyls4khoIQl3M5OW6VanwNGLhDt5ido5pjbKsvf4D03kdjT+ujC/IlQmfagnPzDTHyg9eXM2+BKHoW1Kg1LgCJavW7BZcDuh8NafBogs8sgGLiy2oLYvUkB8ZWd0DATbuYonud1I5qX+oajGb1jCQKV6UAacxlPDlgQDkwCzMHCYuzHNYbb9UmLkTbICFSoKI9wKCXtg2PLUa1FFUAE8a52cF5Xcqy/9rWBO5jxAtP/HUzn/lH8WokVI6dWrjLboV3onedHiYFjA625DfyDRTY+MgVI3xlq+nH9XdWV2jvAKBgYWCGKrfS2vbBCtxBOCWI2MRJiY3d6A6LBmmOtbz9X/65qcoQLEz4x+0n8+oGy16gvo+c14hsWiFBhTHvANSwgBtJi+61pNFsTbPo6gGsiaev1r+8rRWrX7GSuktstdSpicP7BJj9hp7lDjM+Nn4P2ams36rYsENsaHe2DeFc5wCh9vv3ZHZ27BW921peNCuiXkHR9NUeHBHh87HRjnijjNF+Z3jl49Nds2IBsAynyy0qTqBcvo6CIbNUvQ480p7aULUlQ9QsSL+7uKpycmYLRxg==",
                "s3BucketArn": "arn:aws:s3:::bosch-data-bucket",
                "s3Key": "2020100720201007_103222/2020-10-07-10-34-23_19.bag",
                "s3VersionId": None,
            }
        ],
    }
    event_mp4 = {
        "invocationSchemaVersion": "1.0",
        "invocationId": "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",
        "job": {"id": "04fda4d4-77d7-4dba-97fb-baf95189c1a0"},
        "tasks": [
            {
                "taskId": "AAAAAAAAAAHLDW/Pbpa1aGbFbHBVtU5O1qfRX/MwjjYEaMmYkHhwh/GOMuMnInPhNTMOOwm8s8l8Pxdp021fpyugcE3flOYe8SfKHHchw3pQucDsDTS3OqZJsI94+Hu3oITfmaU/Acj1/QbdMf7Y9cGTJqDzlJtIyOWEi6Pi4fltaqWzCISlaTsWnDMFB6NBaG9LPw+asL3PLaRQUJpWFCNicjmLabpP4b0+9dsinrzMjcTa6CBYT20FleVP3sEi7C5a0ffvj1bugEj7Opbubt4GrkJ8j+8ADD6t+Z3k2NG2XjcqX8Ykt1ZuSwsTxGjmNT14ziiQFQLI3JukZLqvAKJD2hr6rbfG4vu2yR2ymRPfUws9TxkzicCd9uIGqVneQ7WRJ0X6Bb8+MKV8fsdt/UxrRN8B42NWBamjlzCUj+1/K/kj/ERqjZZArmqms9T+76F472lhuYILv9dD1KZARCjEmeaOi816Loge+uOvy68kLWD6O6F28qh3SY758qCLQlI2TszqXJMsCIMWBYPMWVm4SlcwxL8xyHxDqoriKFyBaTKK0rrSDTJ0kSVWfhiGFMeH7JC01zKyefj24QZe7Bjr+/FEC35p0wYiJIggf9U/qo2oJ0mK8SdjWnK0qt8xDoi8XquQ70A5oR9HZjedcvdpAZKpdm62GJqh0DW99rqiPrRC7lFnNzg9J94CU+Unx7r29xHtZi8NfJ+WRAamhjmpA7PxQMCQUiozJKxTAFsKVOEM1yPIYWQ61tbA6RB8Z0/zwkbRK11O+dNkx9A29OAljZTNtmyGllKV7Zg94Gwi4Q8RqQNYdD4iNGz+jADuAKmpxKSSxXwJed4IZDY6wWI86xd4yfBAQiTNewgfb5HByQRd9wULFXpbtHle/rHA74vG9cPi2OS2ysQU8I8MqYNmEY+4JnMIekZbaLivF1gWIcD9vlnPfeoe/zgEwJlhG39qClSsK1euRWbIua0sXGEgMu363NgHdjIetudsH2GMhIE1S8e/Nl/WkpssyztxZ/i8iTOWRp5hlfoF3707aubMe0Uy75K640Kqx98g0BHJt1oT7g7/BfKnI9XU9w6AAmX1VlNjvTaf4Q0T2DMw",
                "s3BucketArn": "arn:aws:s3:::my-rosbag-stack-destbucket3708473c-hevpuy70d45q",
                "s3Key": "20201005/20201005_111058/2020-10-05-11-10-58_0/front0016.mp4",
                "s3VersionId": None,
            }
        ],
    }
    
    event_s3put = {'Records': [{'messageId': '6c7c82d4-70b5-4257-a386-800c94e2b730', 'receiptHandle': 'AQEBFZQaqL65hvbbE4iRZVbF+tkZaz0Msw+wmR6TKP3qMp+9L8AsSKPGG9dwYvVHEtdpV+OWLiEFyR5hUcIKDXZH1BtIg/8RX5DXFugkW2QETrt0QhxvCF7ddmtfyWRTiagW7461xFsVoJa0KXCK9lUnOMpAXpzccIEF5YnK6tZrq88xcBZ6AMmb+9PsqgohZlWlqhQAE0mw+9g3kWIx7UBA/vpBilO+n5XoQ8DwoCOkW0qlZvXBmTHNSIlkjlR6yhD2uavrJ0xHV9aKLp4LrIkpt2BbI1EU6x9huzQFF+4NTjZEsqsqOJdbybrHIgZopffAhANvomhwGpZSuIv76hGp2IlNgQB4DECZ9RVz/x5z8ft5Qy4VHPqOfmELIEyTIM58Lz08eWy/En50HKPDkAKynrjzB91SlQtwNnP2gv88rYNyccNmUb44E+V37WIXpKGm', 'body': '{"Records":[{"eventVersion":"2.1","eventSource":"aws:s3","awsRegion":"eu-west-1","eventTime":"2020-11-23T12:18:51.915Z","eventName":"ObjectCreated:CompleteMultipartUpload","userIdentity":{"principalId":"AWS:AROA5CGYXMF2RGDTIQ5FO:davesmn-Isengard"},"requestParameters":{"sourceIPAddress":"88.104.17.245"},"responseElements":{"x-amz-request-id":"CN3X0K6MFT8S8T0T","x-amz-id-2":"EjNEgiLZUrGzmiClDtsjM/8ehzz39lpIDsgSaz9tVOO5OCwGAFZ9qwplO7ZrXytPwFZxFwCJo5euvRoTsFGqNDffmjCLotqY"},"s3":{"s3SchemaVersion":"1.0","configurationId":"M2FiOGRlNTEtNjQzZC00ZGU0LWFkZWYtYmRiZmQ0M2E4MmYx","bucket":{"name":"my-rosbag-stack-srcbucketa467747d-6hpd0b1ohil1","ownerIdentity":{"principalId":"A1OHAXFWJQKA9R"},"arn":"arn:aws:s3:::my-rosbag-stack-srcbucketa467747d-6hpd0b1ohil1"},"object":{"key":"2020-10-05-11-10-58_0.bag","size":2811855506,"eTag":"2739785b3c4c0b2447696aaf402feea1-168","sequencer":"005FBBA8A2A45AB1FF"}}}]}', 'attributes': {'ApproximateReceiveCount': '21', 'SentTimestamp': '1606133936532', 'SenderId': 'AIDAJQOC3SADRY5PEMBNW', 'ApproximateFirstReceiveTimestamp': '1606133936532'}, 'messageAttributes': {}, 'md5OfBody': '91a2feef56d5fd6bbcd7e411f2661118', 'eventSource': 'aws:sqs', 'eventSourceARN': 'arn:aws:sqs:eu-west-1:898102681973:my-rosbag-stack-inputBagQueueC9E70386-1W8YJ8U32CPYG', 'awsRegion': 'eu-west-1'}]}

    lambda_handler(event_mp4, "")
