import os
import logging
from airflow.models import Connection
from airflow.utils.session import provide_session

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


@provide_session # injects airflow DB session for creating connections and other DB operations
def create_aws_conn(
    conn_id: str = "aws_default",
    region: str | None = None,
    session=None
):
    """
    Create or recreate an AWS Airflow connection that will use the boto3 credential provider chain, i.e.,
    it will look for in the following order:
        1. the credentials as env variables
        2. searches for the IAM role attached to the EC2 instance / ECS task / Lambda function
        3. the AWS credentials file (~/.aws/credentials)
    """

    conn_id = conn_id.replace(" ", "_")
    region = region or os.getenv("AWS_REGION", "us-east-1")

    conn = Connection( # creates a connection uri for the AWS connection
        conn_id=conn_id,
        conn_type="aws",
        extra={
            "region_name": region
        }
    )

    existing = session.query(Connection).filter(Connection.conn_id == conn_id).one_or_none()
    if existing:
        logger.info(f"Deleting existing AWS connection: {conn_id}")
        session.delete(existing)
        session.commit()

    session.add(conn)
    session.commit()
    logger.info(f"AWS connection '{conn_id}' created using region '{region}' and credentials or IAM role.")

if __name__ == "__main__":
    create_aws_conn()