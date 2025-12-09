import json
import os
import logging
from io import StringIO
from pathlib import Path
from typing import List, Dict, Any
from dotenv import load_dotenv

import boto3
from botocore.exceptions import ClientError

logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s | %(name)s | %(asctime)s] - [%(filename)s | %(module)s | %(funcName)s | L%(lineno)d] : %(message)s"
)
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent.parent
load_dotenv(BASE_DIR / ".env")


class BucketClient:
    def __init__(
            self, 
            bucket_name: str, 
            aws_access_key_id: str,
            aws_secret_access_key: str,
            endpoint_url: str = None,
            region_name: str = "us-east-1"
        ):
        self.bucket_name = bucket_name
        self.client = boto3.client(
            "s3", 
            endpoint_url=endpoint_url,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )

    def read_s3(self, prefix: str, filename: str) -> List[Dict[str, Any]]:
        """Read JSONL file from S3 and return as list of records."""
        key = f"{prefix.strip('/')}/{filename}"
        try:
            response = self.client.get_object(Bucket=self.bucket_name, Key=key)
            body = response["Body"].read().decode("utf-8")
            records = [json.loads(line) for line in body.splitlines() if line.strip()]
            logger.info(f"Read {len(records)} records from s3://{self.bucket_name}/{key}")
            return records
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                logger.warning(f"Object {key} not found in {self.bucket_name}, returning empty list")
                return []
            else:
                logger.error(f"Failed to read from {key}: {e}")
                raise

    def write_s3(self, prefix: str, filename: str, records: List[Dict[str, Any]]) -> bool:
        """Write a list of JSON records to S3 (overwrite)."""
        key = f"{prefix.strip('/')}/{filename}"
        try:
            buffer = StringIO()
            for record in records:
                buffer.write(json.dumps(record) + "\n")

            self.client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=buffer.getvalue().encode("utf-8"),
                ContentType="application/json"
            )
            logger.info(f"Wrote {len(records)} records to s3://{self.bucket_name}/{key}")
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                logger.warning(f"Object {key} not found in {self.bucket_name}, returning empty list")
                return False
            else:
                logger.error(f"Failed to read from {key}: {e}")
                raise

    def append_s3(self, prefix: str, filename: str, new_records: List[Dict[str, Any]]) -> None:
        """Append new records into an existing JSONL object in S3."""
        existing_records = self.read_s3(prefix, filename)
        all_records = existing_records + new_records
        self.write_s3(prefix, filename, all_records)
        logger.info(f"Appended {len(new_records)} records to s3://{self.bucket_name}/{prefix}/{filename}, total now {len(all_records)}")

if __name__ == "__main__":
    # Example usage
    datalake = BucketClient(
        bucket_name="datalake",
        aws_access_key_id=os.getenv("MINIO_ROOT_USER"),
        aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD"),
        endpoint_url=os.getenv("MINIO_ENDPOINT_LOCALHOST"),
        region_name=os.getenv("AWS_REGION_NAME", "us-east-1")
    )

    print("Datalake client initialized.")

    datasets = BucketClient(
        bucket_name="dataset-files",
        aws_access_key_id=os.getenv("MINIO_ROOT_USER"),
        aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD"),
        endpoint_url=os.getenv("MINIO_ENDPOINT_LOCALHOST"),
        region_name=os.getenv("AWS_REGION_NAME", "us-east-1")
    )

    print("Datasets client initialized.")
    test_prefix = "/dummy-datasets/"
    test_filename = "sales_data_2.jsonl"

    records = [
        {"id": 0, "ProductCategoryName": "Bikes", "SalesAmount": 3578.27, "OrderDate": "2010-12-29"},
        {"id": 1, "ProductCategoryName": "Bikes", "SalesAmount": 3399.99, "OrderDate": "2010-12-29"},
    ]

    datasets.write_s3(test_prefix, test_filename, records)
    read_records = datasets.read_s3(test_prefix, test_filename)
    print(f"Read records: {read_records}")

    new_records = [
        {"id": 2, "ProductCategoryName": "Clothing", "SalesAmount": 49.99, "OrderDate": "2010-12-30"},
        {"id": 3, "ProductCategoryName": "Accessories", "SalesAmount": 19.99, "OrderDate": "2010-12-30"},
    ]

    datasets.append_s3(test_prefix, test_filename, new_records)
    updated_records = datasets.read_s3(test_prefix, test_filename)
    print(f"Updated records: {updated_records}")
