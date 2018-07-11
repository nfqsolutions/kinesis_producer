import logging
import time
from multiprocessing.pool import ThreadPool

import boto3
import botocore
from botocore.exceptions import *

log = logging.getLogger(__name__)


def get_connection(aws_region):
    session = boto3.session.Session()
    connection = session.client('kinesis', region_name=aws_region)
    return connection


def get_firehose_connection(aws_region):
    session = boto3.session.Session()
    connection = session.client('firehose', region_name=aws_region)
    return connection


def call_and_retry(boto_function, max_retries, **kwargs):
    """Retry Logic for generic boto client calls.

    This code follows the exponetial backoff pattern suggested by
    http://docs.aws.amazon.com/general/latest/gr/api-retries.html
    """
    retries = 0
    while True:
        if retries:
            log.warning('Retrying (%i) %s', retries, boto_function)

        try:
            return boto_function(**kwargs)
        except Exception as exc:
            if retries >= max_retries:
                raise exc
            error_code = exc.response.get("Error", {}).get("Code")
            log.warning('Error code: {}'.format(error_code))
            time.sleep(2 ** retries * .1)
            retries += 1


class Client(object):
    """Synchronous Kinesis client."""

    def __init__(self, config):
        self.stream = config['stream_name']
        self.max_retries = config['kinesis_max_retries']
        self.connection = get_connection(config['aws_region'])

    def put_record(self, record):
        """Send records to Kinesis API.

        Records is a list of tuple like (data, partition_key).
        """
        data, partition_key = record

        log.debug('Sending record: %s', data[:100])
        try:
            call_and_retry(self.connection.put_record, self.max_retries,
                           StreamName=self.stream, Data=data,
                           PartitionKey=partition_key)
        except:
            log.exception('Failed to send records to Kinesis')

    def close(self):
        log.debug('Closing client')

    def join(self):
        log.debug('Joining client')


class FirehoseClient(object):
    """Synchronous Firehose client."""

    def __init__(self, config):
        self.stream = config['stream_name']
        self.max_retries = config['kinesis_max_retries']
        self.connection = get_firehose_connection(config['aws_region'])

    def put_record(self, record):
        """Send records to Firehose API.

        Records is a list of tuple like (data, partition_key).
        """
        data, pk = record

        log.debug('Sending record: %s', data[:100])
        try:
            call_and_retry(self.connection.put_record, self.max_retries,
                           DeliveryStreamName=self.stream,
                           Record={'Data': data})
        except Exception as e:
            log.info('Exception type: {}'.format(type(e)))
            log.exception('Failed to send records to Firehose. Retrying')
            call_and_retry(self.connection.put_record, self.max_retries,
                           DeliveryStreamName=self.stream,
                           Record={'Data': data})

    def close(self):
        log.debug('Closing client')

    def join(self):
        log.debug('Joining client')


class ThreadPoolClient(Client):
    """Thread pool based asynchronous Kinesis client."""

    def __init__(self, config):
        super(ThreadPoolClient, self).__init__(config)
        self.pool = ThreadPool(processes=config['kinesis_concurrency'])

    def put_record(self, records):
        task_func = super(ThreadPoolClient, self).put_record
        self.pool.apply_async(task_func, args=[records])

    def close(self):
        super(ThreadPoolClient, self).close()
        self.pool.close()

    def join(self):
        super(ThreadPoolClient, self).join()
        self.pool.join()


class FirehoseThreadPoolClient(FirehoseClient):
    """Thread pool based asynchronous Firehose client."""

    def __init__(self, config):
        super(FirehoseThreadPoolClient, self).__init__(config)
        self.pool = ThreadPool(processes=config['kinesis_concurrency'])

    def put_record(self, records):
        task_func = super(FirehoseThreadPoolClient, self).put_record
        self.pool.apply_async(task_func, args=[records])

    def close(self):
        super(FirehoseThreadPoolClient, self).close()
        self.pool.close()

    def join(self):
        super(FirehoseThreadPoolClient, self).join()
        self.pool.join()
