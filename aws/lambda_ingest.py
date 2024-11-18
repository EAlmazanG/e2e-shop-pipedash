import boto3
import urllib.request

# AWS S3 configuration
s3_client = boto3.client('s3')
bucket_name = 'e2e-shop-bucket'
folder = 'raw/'

def lambda_handler(event, context):
    # URLs and file names for the two files
    files = [
        {
            "url": "https://github.com/EAlmazanG/e2e-shop-pipedash/blob/main/data/main_product_descriptions.csv",
            "file_name": "main_product_descriptions.csv"
        },
        {
            "url": "https://github.com/EAlmazanG/e2e-shop-pipedash/blob/main/data/retail.csv",
            "file_name": "retail.csv"
        }
    ]

    results = []

    try:
        for file in files:
            # Download the file using urllib
            response = urllib.request.urlopen(file["url"])
            data = response.read()

            # Upload to S3
            s3_client.put_object(
                Bucket=bucket_name,
                Key=f"{folder}{file['file_name']}",
                Body=data,
                ContentType='text/csv'
            )

            results.append(f"File {file['file_name']} successfully uploaded to {bucket_name}/{folder}")

        return {
            'statusCode': 200,
            'body': results
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': f"An error occurred: {str(e)}"
        }
