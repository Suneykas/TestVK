import pytest
import boto3
from urls import CloudStorage
import random


random_str = str('suneev_test-' + str(random.randint(0, 999999)))


def api_checker(true_sc, response, test_name=None, key=None):
    if response['ResponseMetadata']['HTTPStatusCode'] != true_sc:
        print('\n', test_name, '- NOT Passed. WRONG STATUS CODE. '
                               '\nСurrent Status code is', response['ResponseMetadata']['HTTPStatusCode'],
              '\nСurrent Response is\n', response)
        assert response['ResponseMetadata']['HTTPStatusCode'] == true_sc
    elif key is not None and key != key:
        print('\n', test_name, '- NOT Passed. WRONG KEY. Сurrent Key', key)
        assert key == key
    else:
        print('\n', test_name, '- Passed.\nStatus code is', response['ResponseMetadata']['HTTPStatusCode'],
              '\nResponse is\n', response)


def pytest_addoption(parser):
    parser.addoption('--host_name', action='store', default=CloudStorage.host,
                     help="Choose host_name: CloudStorage or another")


@pytest.fixture()
def api_session(request):
    endpoint_url = request.config.getoption("host_name")
    if endpoint_url == CloudStorage.host:
        print('Start Testing')
    else:
        raise pytest.UsageError("--host_name should be CloudStorage or another")
    yield


def create_bucket():
    test_name = 'create_bucket'
    bucket = random_str
    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url=CloudStorage.host
    )
    response = s3_client.create_bucket(
        Bucket=bucket,
        CreateBucketConfiguration={
            'LocationConstraint': 'ru-msk'
        }
    )
    api_checker(200, response, test_name)
    print('\nBucket', bucket, '- successfully created')
    return bucket


def upload_object(bucket=None, key=None):
    test_name = 'upload_object'
    bucket = create_bucket()
    key = random_str
    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url=CloudStorage.host
    )
    response = s3_client.put_object(Body='TEST_TEXT_TEST_TEXT', Bucket=bucket, Key=key)
    api_checker(200, response, test_name)
    print('\nObject', key, '-successfully created in Bucket', bucket)
    return bucket, key


def delete_object(bucket, key):
    test_name = 'delete_object'
    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url=CloudStorage.host
    )
    response = s3_client.delete_object(Bucket=bucket, Key=key, )
    api_checker(204, response, test_name)


def delete_bucket(bucket):
    test_name = 'delete_bucket'
    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url=CloudStorage.host
    )
    response = s3_client.delete_bucket(Bucket=bucket)
    api_checker(204, response, test_name)
