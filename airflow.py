import boto3

client = boto3.client('mwaa')
response = client.create_web_login_token(
    Name='mwaa-environment'
)

print(response["WebServerHostname"])