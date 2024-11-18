import boto3
import urllib.request

# AWS S3 configuration
s3_client = boto3.client('s3')
bucket_name = 'e2e-shop-bucket'
folder = 'raw/'

def lambda_handler(event, context):
    repo_file_url = 'https://github.com/EAlmazanG/e2e-shop-pipedash/blob/main/data/main_product_descriptions.csv'
    file_name = 'product_descriptions.csv'

    try:
        # Download the file using urllib
        response = urllib.request.urlopen(repo_file_url)
        data = response.read()

        # Upload to S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=f"{folder}{file_name}",
            Body=data,
            ContentType='text/csv'
        )
        return {
            'statusCode': 200,
            'body': f"File {file_name} successfully uploaded to {bucket_name}/{folder}"
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': f"An error occurred: {str(e)}"
        }