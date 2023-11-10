import pytest
import boto3
from urls import CloudStorage
import random


random_str = str('test-' + str(random.randint(0, 999999)))


def api_checker(true_sc, response, test_name=None, expected_key=None, key=None):
    if response['ResponseMetadata']['HTTPStatusCode'] != true_sc:
        print('\n', test_name, '- NOT Passed. WRONG STATUS CODE. '
                               '\nСurrent Status code is', response['ResponseMetadata']['HTTPStatusCode'],
              '\nСurrent Response is\n', response)
        assert response['ResponseMetadata']['HTTPStatusCode'] == true_sc
    elif expected_key is not None and key != expected_key:
        print('\n', test_name, '- NOT Passed. WRONG KEY. Сurrent Key', key)
        assert key == expected_key
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
        session = boto3.session.Session()
        s3_client = session.client(
            service_name='s3',
            endpoint_url=CloudStorage.host
        )
    else:
        raise pytest.UsageError("--host_name should be CloudStorage or another")
    yield api_session
    print('Testing completed')


def test_create_bucket(api_session):
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


def test_upload_object():
    test_name = 'upload_object'
    bucket = test_create_bucket(api_session)
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


def test_delete_object(bucket, key):
    test_name = 'delete_object'
    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url=CloudStorage.host
    )
    response = s3_client.delete_object(Bucket=bucket, Key=key, )
    api_checker(204, response, test_name)


def test_delete_bucket(bucket):
    test_name = 'delete_bucket'
    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url=CloudStorage.host
    )
    response = s3_client.delete_bucket(Bucket=bucket)
    api_checker(204, response, test_name)
