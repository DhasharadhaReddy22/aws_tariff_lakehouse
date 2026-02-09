import json
import os
from pathlib import Path
from io import BytesIO
from typing import List, Dict, Any

import boto3
from botocore.exceptions import ClientError

from .logger import get_logger
from .config import config

logger = get_logger(__name__, caller_file_path=__file__)

class BucketClient:
    def __init__(self, bucket_name: str, region_name: str|None = None):
        """
        Args:
            bucket_name (str): Target S3 bucket
            region_name (str, optional): AWS region (optional; boto3 default chain applies)
        """
        self._bucket_name = bucket_name
        self.s3 = boto3.client("s3", region_name=region_name)

    @property
    def bucket_name(self) -> str:
        return self._bucket_name

    def exists(self, key: str) -> bool:
        """
        Checks whether an object exists.

        Returns:
            True if object exists, False otherwise.
        """
        try:
            self.s3.head_object(Bucket=self.bucket_name, Key=key)
            return True
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code")
            if error_code in ("404", "NoSuchKey"):
                return False

            logger.error(f"Key present, but error checking existence for s3://{self.bucket_name}/{key} | {e}")
            return False
        
    def put_json(self, key: str, payload: Dict[str, Any]) -> None:
        """
        Writes a single JSON object (can overwrite).
        """
        try:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")

            self.s3.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=body,
                ContentType="application/json"
            )

            logger.info(f"Wrote JSON to s3://{self.bucket_name}/{key}")

        except ClientError as e:
            logger.error(f"Failed to write JSON to {key} - {e}")
            raise

    def put_jsonl(self, key: str, records: List[Dict[str, Any]]) -> None:
        """
        Writes JSON Lines (can overwrite).
        Each list element is written as one JSON line.
        """
        try:
            buffer = BytesIO()

            for record in records:
                buffer.write(json.dumps(record, ensure_ascii=False).encode("utf-8"))
                buffer.write(b"\n")

            self.s3.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=buffer.getvalue(),
                ContentType="application/json",
            )

            logger.info(f"Wrote {len(records)} JSONL records to s3://{self.bucket_name}/{key}")

        except ClientError as e:
            logger.error(f"Failed to write JSONL to {key} - {e}")
            raise
    
    def put_text(self, key: str, text: str) -> None:
        """
        Writes plain text (UTF-8) (can overwrite).
        """
        try:
            self.s3.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=text.encode("utf-8"),
                ContentType="text/plain"
            )

            logger.info(f"Wrote text to s3://{self.bucket_name}/{key}")

        except ClientError as e:
            logger.error(f"Failed to write text to {key} - {e}")
            raise

    def put_bytes(self, key: str, data: bytes, content_type: str) -> None:
        """
        Writes raw bytes (images, PDFs, etc.) (can overwrite).
        The content_type must be a valid MIME type tells s3 what kind of data is being written.
        Valid examples: "image/jpeg", "image/svg+xml", "application/pdf", etc.
        """
        try:
            self.s3.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=data,
                ContentType=content_type
            )

            logger.info(f"Wrote bytes of {content_type} to s3://{self.bucket_name}/{key}")

        except ClientError as e:
            logger.error(f"Failed to write bytes of {content_type} to {key} - {e}")
            raise

    def get_json(self, key: str) -> Dict[str, Any]:
        """
        Reads and parses a JSON object.

        Raises:
            Exception if object does not exist or read fails.
        """
        try:
            key = key.lstrip("/")
            response = self.s3.get_object(Bucket=self.bucket_name, Key=key)
            body = response["Body"].read().decode("utf-8")
            return json.loads(body)

        except ClientError as e:
            logger.error(f"Failed to read JSON from {key} - {e}")
            raise

    def get_jsonl(self, key: str) -> List[Dict[str, Any]]:
        """
        Reads and parses JSON Lines.
        """
        try:
            key = key.lstrip("/")
            response = self.s3.get_object(Bucket=self.bucket_name, Key=key)
            body = response["Body"].read().decode("utf-8")
            return [
                json.loads(line)
                for line in body.splitlines()
                if line.strip()
            ]

        except ClientError as e:
            logger.error(f"Failed to read JSONL from {key} - {e}")
            raise

    def get_bytes(self, key: str) -> bytes:
        """
        Reads raw bytes. Used for images, PDFs, etc.
        """
        try:
            key = key.lstrip("/")
            response = self.s3.get_object(Bucket=self.bucket_name, Key=key)
            return response["Body"].read()

        except ClientError as e:
            logger.error(f"Failed to read bytes from {key} - {e}")
            raise

    def download_bytes(self, key: str, local_path: Path) -> None:
        """
        Downloads an object from S3 and writes it to a local file.

        Intended for local debugging or explicit Lambda /tmp usage.
        """
        try:
            key = key.lstrip("/")
            data = self.get_bytes(key)
            object_name = Path(key).name


            # Directory path (no file suffix)
            if local_path.suffix == "":
                local_path.mkdir(parents=True, exist_ok=True)
                target_file = local_path / object_name
            else:
                # File path
                local_path.parent.mkdir(parents=True, exist_ok=True)
                target_file = local_path

            target_file.write_bytes(data)

            logger.info(f"Downloaded s3://{self.bucket_name}/{key} -> {target_file}")

        except Exception as e:
            logger.error(f"Failed to download s3://{self.bucket_name}/{key} | {e}")
            raise

bucket_client = BucketClient(
    bucket_name=config.get("AWS_S3_LAKEHOUSE_BUCKET"),
    region_name=config.get("AWS_REGION_NAME", default="us-east-1")
)

if __name__ == "__main__":
    # Example usage
    datasets = BucketClient(
        bucket_name="dummy-lakehouse",
        region_name="us-east-1"
    )

    print("Datasets client initialized.")
    test_key = "dummy-datasets/sales_data_2.jsonl"

    records = [
        {"id": 0, "ProductCategoryName": "Bikes", "SalesAmount": 3578.27, "OrderDate": "2010-12-29"},
        {"id": 1, "ProductCategoryName": "Bikes", "SalesAmount": 3399.99, "OrderDate": "2010-12-29"},
    ]

    datasets.put_jsonl(records=records, key=test_key)
    read_records = datasets.get_jsonl(key=test_key)
    print(f"Read records: {read_records}")

    new_records = [
        {"id": 2, "ProductCategoryName": "Clothing", "SalesAmount": 49.99, "OrderDate": "2010-12-30"},
        {"id": 3, "ProductCategoryName": "Accessories", "SalesAmount": 19.99, "OrderDate": "2010-12-30"},
    ]

    datasets.put_jsonl(records=new_records, key=test_key)
    updated_records = datasets.get_jsonl(key=test_key)
    print(f"Updated records: {updated_records}")

    test_img_key = "/dummy-datasets/elt_chalkboard_medium.png"
    datasets.download_bytes(key=test_img_key, local_path=Path("../../../logs/images/"))

    test_pdf_key = "dummy-datasets/DEC_Bills _ Billing and Cost Management _ Global.pdf"
    datasets.download_bytes(key=test_pdf_key, local_path=Path("../../../logs/pdfs/"))