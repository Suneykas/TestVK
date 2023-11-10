import pytest
from conftest import api_checker, test_delete_object, test_upload_object, test_delete_bucket
import boto3
from urls import CloudStorage


@pytest.mark.api_bucket
class TestsApiBucket:
    def test_object(self):
        test_name = 'Test Object'
        bucket = test_upload_object()[0]
        key = test_upload_object()[1]
        session = boto3.session.Session()
        s3_client = session.client(
            service_name='s3',
            endpoint_url=CloudStorage.host
        )
        response = s3_client.get_object(Bucket=bucket, Key=key)
        api_checker(200, response, test_name)

    def test_list_objects(self):
        test_name = 'Test ListObjects'
        bucket, expected_key = test_upload_object()
        session = boto3.session.Session()
        s3_client = session.client(
            service_name='s3',
            endpoint_url=CloudStorage.host
        )
        response = s3_client.list_objects(Bucket=bucket)
        for key in s3_client.list_objects(Bucket=bucket)['Contents']:
            key = key['Key']
        api_checker(200, response, test_name, expected_key, key)
        test_delete_object(bucket, key)
        test_delete_bucket(bucket)
