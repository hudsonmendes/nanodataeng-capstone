import io
import os
import boto3
import zipfile
from urllib.request import urlopen

from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

class S3UnzipAndUploadOperator(BaseOperator):
    """
    Unzip a S3 file and upload that into another S3 folder,
    without unpacking the ZIP file locally.
    """

    @apply_defaults
    def __init__(
            self,
            s3_source_url: str,
            s3_destination_bucket: str,
            s3_destination_folder: str,
            *args, **kwargs) -> None:
        """
        Creates the Operator.

        Parameters
        ----------
        s3_source_url         {str} The S3 url to the ZIP file that will be unpacked.
        s3_destination_bucket {str} The S3 bucket to where the files will be unpacked.
        s3_destination_folder {str} The S3 folder to where the files will be unpacked.
        """
        super(S3UnzipAndUploadOperator, self).__init__(*args, **kwargs)
        self.s3 = boto3.client('s3')
        self.s3_source_url         = s3_source_url
        self.s3_destination_bucket = s3_destination_bucket
        self.s3_destination_folder = s3_destination_folder

    def execute(self, context):
        """
        Performs the download (into memory), unpack and upload.

        Parameters
        ----------
        context {object} Airflow context
        """
        S3UnzipAndUploadOperator.stage_zip_files_to_s3(
            s3=self.s3,
            source_url=self.s3_source_url,
            destination_bucket=self.s3_destination_bucket,
            destination_folder=self.s3_destination_folder)

    @staticmethod
    def stage_zip_files_to_s3(
            s3,
            source_url,
            destination_bucket,
            destination_folder):
        """
        Performs the download (into memory), unpack and upload.

        Parameters
        ----------
        source_url         {str} The S3 url to the ZIP file that will be unpacked.
        destination_bucket {str} The S3 bucket to where the files will be unpacked.
        destination_folder {str} The S3 folder to where the files will be unpacked.
        """
        print(f'Downloading "{source_url}", please wait...')
        with urlopen(source_url) as res:
            buffer = io.BytesIO(res.read())
            file_zip = zipfile.ZipFile(buffer)
            print('Download completed.')

            print(f'Uploading each file in "{source_url}" to s3://{destination_bucket}/{destination_folder}')
            for inner_file_name in file_zip.namelist():
                inner_file_buffer = file_zip.read(inner_file_name) 
                s3.put_object(
                    Bucket=destination_bucket,
                    Key=os.path.join(destination_folder, inner_file_name),
                    Body=inner_file_buffer)
                print(f'[ok] {inner_file_name}')