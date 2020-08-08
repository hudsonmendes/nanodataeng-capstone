import os
import io
import boto3
import requests

from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

class S3DownloadAndUploadOperator(BaseOperator):
    """
    Downloads a file from a HTTP source and uploads it into S3.
    This provides a S3 path for operations that require the file to
    be there, such as Redshift COPY operations.
    """

    @apply_defaults
    def __init__(
            self,
            http_source_url: str,
            s3_destination_bucket: str,
            s3_destination_path: str,
            *args, **kwargs) -> None:
        """
        Initializes the Operator.

        Parameters
        ----------
        http_source_url       {str} the HTTP source that will be downloaded
        s3_destination_bucket {str} the S3 bucket to where the file will be uploaded
        s3_destination_path   {str} the S3 key under which the file will be uploaded 
        """
        super(S3DownloadAndUploadOperator, self).__init__(*args, **kwargs)
        self.s3 = boto3.client('s3')
        self.http_source_url       = http_source_url
        self.s3_destination_bucket = s3_destination_bucket
        self.s3_destination_path   = s3_destination_path

    def execute(self, context):
        """
        Executes the download/upload sequence

        Parameters
        ----------
        context {object} airflow context
        """
        S3DownloadAndUploadOperator.stage_file_to_s3(
            s3=self.s3,
            source_url=self.http_source_url,
            destination_bucket=self.s3_destination_bucket,
            destination_path=self.s3_destination_path)

    @staticmethod
    def stage_file_to_s3(
            s3,
            source_url,
            destination_bucket,
            destination_path):
        """
        Downloads file, loads it into an in memory buffer
        and finally uploads that memory buffer to S3.

        Parameters
        ----------
        source_url         {str} the HTTP source that will be downloaded
        destination_bucket {str} the S3 bucket to where the file will be uploaded
        destination_path   {str} the S3 key under which the file will be uploaded 
        """
        s3 = boto3.client('s3')
        file_name = os.path.basename(source_url)
        req = requests.get(source_url, stream=True)
        buffer = io.BytesIO()
        for chunk in req.iter_content(chunk_size=1024):
            if chunk:
                buffer.write(chunk)
        
        buffer.seek(0)
        s3.upload_fileobj(
            Fileobj=buffer,
            Bucket=destination_bucket,
            Key=destination_path)
        print(f'[ok] {file_name}')