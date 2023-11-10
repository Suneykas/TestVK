import pytest
from conftest import api_checker, delete_object, upload_object, delete_bucket
import boto3
from urls import CloudStorage


@pytest.mark.api_bucket
class TestsApiBucket:
    def test_object(self):
        test_name = 'Test Object'
        bucket = upload_object()[0]
        key = upload_object()[1]
        session = boto3.session.Session()
        s3_client = session.client(
            service_name='s3',
            endpoint_url=CloudStorage.host
        )
        response = s3_client.get_object(Bucket=bucket, Key=key)
        api_checker(200, response, test_name)

    def test_list_objects(self):
        test_name = 'Test ListObjects'
        bucket, expected_key = upload_object()
        session = boto3.session.Session()
        s3_client = session.client(
            service_name='s3',
            endpoint_url=CloudStorage.host
        )
        response = s3_client.list_objects(Bucket=bucket)
        for key in s3_client.list_objects(Bucket=bucket)['Contents']:
            key = key['Key']
        api_checker(200, response, test_name, expected_key, key)
        delete_object(bucket, key)
        delete_bucket(bucket)
