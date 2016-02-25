from __future__ import unicode_literals

import datetime

from botocore.exceptions import ClientError
import boto3
from freezegun import freeze_time
import sure  # noqa

from moto import mock_kinesis


def create_s3_stream(client, stream_name):
    return client.create_delivery_stream(
        DeliveryStreamName=stream_name,
        S3DestinationConfiguration={
            'RoleARN': 'arn:aws:iam::123456789012:role/firehose_delivery_role',
            'BucketARN': 'arn:aws:s3:::firhose_bucket',
            'Prefix': 'data/',
            'BufferingHints': {
                'SizeInMBs': 123,
                'IntervalInSeconds': 123
            },
            'CompressionFormat': 'UNCOMPRESSED',
            'EncryptionConfiguration': {
                'NoEncryptionConfig': 'NoEncryption',
                'KMSEncryptionConfig': {
                    'AWSKMSKeyARN': 'arn:aws:kms:us-east-1:123456789012:alias/example-alias'
                }
            }
        }
    )


def create_redshift_stream(client, stream_name):
    return client.create_delivery_stream(
        DeliveryStreamName=stream_name,
        RedshiftDestinationConfiguration={
            'RoleARN': 'arn:aws:iam::123456789012:role/firehose_delivery_role',
            'ClusterJDBCURL': 'jdbc:redshift://host.amazonaws.com:5439/database',
            'CopyCommand': {
                'DataTableName': 'outputTable',
                'CopyOptions': "CSV DELIMITER ',' NULL '\\0'"
            },
            'Username': 'username',
            'Password': 'password',
            'S3Configuration': {
                'RoleARN': 'arn:aws:iam::123456789012:role/firehose_delivery_role',
                'BucketARN': 'arn:aws:s3:::kinesis-test',
                'Prefix': 'myFolder/',
                'BufferingHints': {
                    'SizeInMBs': 123,
                    'IntervalInSeconds': 124
                },
                'CompressionFormat': 'UNCOMPRESSED',
            }
        }
    )

create_stream = create_redshift_stream


@mock_kinesis
@freeze_time("2015-03-01")
def test_create_stream():
    client = boto3.client('firehose', region_name='us-east-1')

    response = create_stream(client, 'stream1')
    stream_arn = response['DeliveryStreamARN']
    print "="*120
    response = client.describe_delivery_stream(DeliveryStreamName='stream1')
    print "Test code"
    stream_description = response['DeliveryStreamDescription']

    # Sure and Freezegun don't play nicely together
    created = stream_description.pop('CreateTimestamp')
    last_updated = stream_description.pop('LastUpdateTimestamp')
    from dateutil.tz import tzlocal
    assert created == datetime.datetime(2015, 3, 1, tzinfo=tzlocal())
    assert last_updated == datetime.datetime(2015, 3, 1, tzinfo=tzlocal())
    print
    print stream_description
    print "="*120
    stream_description.should.equal({
        'DeliveryStreamName': 'stream1',
        'DeliveryStreamARN': stream_arn,
        'DeliveryStreamStatus': 'ACTIVE',
        'VersionId': 'string',
        'Destinations': [
            {
                'DestinationId': 'string',
                'RedshiftDestinationDescription': {
                    'RoleARN': 'arn:aws:iam::123456789012:role/firehose_delivery_role',
                    'ClusterJDBCURL': 'jdbc:redshift://host.amazonaws.com:5439/database',
                    'CopyCommand': {
                        'DataTableName': 'outputTable',
                        'CopyOptions': "CSV DELIMITER ',' NULL '\\0'"
                    },
                    'Username': 'username',
                    'S3Configuration': {
                        'RoleARN': 'arn:aws:iam::123456789012:role/firehose_delivery_role',
                        'BucketARN': 'arn:aws:s3:::kinesis-test',
                        'Prefix': 'myFolder/',
                        'BufferingHints': {
                            'SizeInMBs': 123,
                            },
                        'CompressionFormat': 'UNCOMPRESSED',
                    },
                }
            }
        ],
        "HasMoreDestinations": False,
    })


@mock_kinesis
@freeze_time("2015-03-01")
def test_deescribe_non_existant_stream():
    client = boto3.client('firehose', region_name='us-east-1')

    client.describe_delivery_stream.when.called_with(DeliveryStreamName='not-a-stream').should.throw(ClientError)


@mock_kinesis
@freeze_time("2015-03-01")
def test_list_and_delete_stream():
    client = boto3.client('firehose', region_name='us-east-1')

    create_stream(client, 'stream1')
    create_stream(client, 'stream2')

    set(client.list_delivery_streams()['DeliveryStreamNames']).should.equal(set(['stream1', 'stream2']))

    client.delete_delivery_stream(DeliveryStreamName='stream1')

    set(client.list_delivery_streams()['DeliveryStreamNames']).should.equal(set(['stream2']))


@mock_kinesis
def test_create_s3_stream():
    client = boto3.client('firehose', region_name='us-east-1')

    response = create_s3_stream(client, 'stream1')
    stream_arn = response['DeliveryStreamARN']

    response = client.describe_delivery_stream(DeliveryStreamName='stream1')
    stream_description = response['DeliveryStreamDescription']

    # Sure and Freezegun don't play nicely together
    created = stream_description.pop('CreateTimestamp')
    last_updated = stream_description.pop('LastUpdateTimestamp')

    stream_description.should.equal({
        'DeliveryStreamName': 'stream1',
        'DeliveryStreamARN': stream_arn,
        'DeliveryStreamStatus': 'ACTIVE',
        'VersionId': 'string',
        'Destinations': [
            {
                'DestinationId': 'string',
                'S3DestinationDescription': {
                    'RoleARN': 'arn:aws:iam::123456789012:role/firehose_delivery_role',
                    'BucketARN': 'arn:aws:s3:::firhose_bucket',
                    'Prefix': 'data/',
                    'BufferingHints': {
                        'SizeInMBs': 123,
                        'IntervalInSeconds': 123
                        },
                    'CompressionFormat': 'UNCOMPRESSED',
                    'EncryptionConfiguration': {
                        'NoEncryptionConfig': 'NoEncryption',
                        'KMSEncryptionConfig': {
                            'AWSKMSKeyARN': 'arn:aws:kms:us-east-1:123456789012:alias/example-alias'
                            }
                        }
                    }
            }
        ],
        "HasMoreDestinations": False,
    })



@mock_kinesis
def test_put_record():
    client = boto3.client('firehose', region_name='us-east-1')

    create_stream(client, 'stream1')
    client.put_record(
        DeliveryStreamName='stream1',
        Record={
            'Data': 'some data'
        }
    )


@mock_kinesis
def test_put_record_batch():
    client = boto3.client('firehose', region_name='us-east-1')

    create_stream(client, 'stream1')
    client.put_record_batch(
        DeliveryStreamName='stream1',
        Records=[
            {
                'Data': 'some data1'
            },
            {
                'Data': 'some data2'
            },
        ]
    )
