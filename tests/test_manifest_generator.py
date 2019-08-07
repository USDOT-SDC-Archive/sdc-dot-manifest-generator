import pytest
import os
import shutil
import boto3
import tempfile
from os import path
from moto import mock_dynamodb, mock_s3
from common.logger_utility import LoggerUtility
from unittest import mock
from lambdas import manifest_generator_lambda_handler
from s3transfer.processpool import ProcessPoolDownloader


def Any(cls):
    class Any(cls):
        def __eq__(self, other):
            return True
    return Any()


def test_run_in_parallel():

    class MockFuture:
        exception = ZeroDivisionError

    class MockThreadPoolExecutor:
        def __init__(self, *args, **kwargs):
            return

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            return False

        def submit(self, job):
            return MockFuture()

    job_generator = ["job_1", "job_2", "job_3"]

    manifest_generator_lambda_handler.ThreadPoolExecutor = MockThreadPoolExecutor

    with pytest.raises(ZeroDivisionError):
        manifest_generator_lambda_handler.__run_in_parallel(job_generator, max_workers=3)


def test_get_size():

    os.stat = mock.MagicMock()
    manifest_generator_lambda_handler.getSize("filename")
    os.stat.assert_called_with("filename")


def test_update_batch_status():

    batch_id = "batch_id"
    status = "status"
    is_historical = "is_historical"

    LoggerUtility.logInfo = mock.MagicMock()

    manifest_generator_lambda_handler.update_batch_status(batch_id, status, is_historical)

    LoggerUtility.logInfo.assert_called_with(
        "Place holder to push the batch id and status to ES - {} - {} - {} ".format(batch_id,status,is_historical))


def test_delete_dir(monkeypatch):

    LoggerUtility.logError = mock.MagicMock()

    def mock_rmtree(*args, **kwargs):
        raise OSError(1, "strerror", "filename")

    monkeypatch.setattr(shutil, "rmtree", mock_rmtree)

    with pytest.raises(OSError):
        manifest_generator_lambda_handler.delete_dir("dir")
    LoggerUtility.logError.assert_called_with("Error: filename - strerror.")


@mock_s3
def test_process_manifest_files():
    os.environ['DDB_CURATED_RECORDS_TABLE_ARN'] = "part1/part2"
    os.environ['DDB_CURATED_RECORDS_INDEX_NAME'] = "DDB_CURATED_RECORDS_INDEX_NAME"
    os.environ['DDB_MANIFEST_INDEX_NAME'] = "DDB_MANIFEST_INDEX_NAME"
    os.environ['DDB_MANIFEST_TABLE_ARN'] = "part1/part2"
    os.environ['CURATED_BUCKET_NAME'] = "bucket"

    response = {
            "Count": 2,
            "Items": [{
                    "ManifestId": "ManifestId1",
                    "S3Key": "URL_bucket/URL1",
                    "TotalNumCuratedRecords": 50,
                    "State": "New Hampshire"
                },
                {
                    "ManifestId": "ManifestId2",
                    "S3Key": "URL_bucket/URL2",
                    "TotalNumCuratedRecords": 50,
                    "State": "Colorado"
                },
                {
                    "ManifestId": "ManifestId1",
                    "S3Key": "URL_bucket/URL3",
                    "TotalNumCuratedRecords": 50,
                    "State": "New Hampshire"
                }
            ]
        }

    class MockDynamodb:
        class Table:
            def __init__(self, *args, **kwargs):
                pass

            def query(self, IndexName, KeyConditionExpression, FilterExpression):
                return response

    class MockS3Resource:
        def __init__(self, *args, **kwargs):
            pass

        class Bucket:
            def __init__(self, *args, **kwargs):
                pass

            def download_file(self, key, filename):
                pass

    manifest_generator_lambda_handler.s3Resource = MockS3Resource()
    manifest_generator_lambda_handler.dynamodb = MockDynamodb()
    manifest_generator_lambda_handler.dynamodb.Table.put_item = mock.MagicMock()
    boto3.client = mock.MagicMock()
    manifest_generator_lambda_handler.delete_dir = mock.MagicMock()

    manifest_generator_lambda_handler.__process_manifest_files("batch_id", "table_name", is_historical=True)

    manifest_generator_lambda_handler.dynamodb.Table.put_item.assert_called_with(
        Item={
            "ManifestId": "ManifestId1",
            "BatchId": "batch_id",
            "TableName": "table_name",
            "ManifestS3Key": Any(str),
            "CombinedS3Key": Any(str),
            "CombinedFileSize": Any(int),
            "IsHistorical": True,
            "FileStatus": "open",
            "TotalCuratedRecordsCount": 150,
            "TotalCuratedRecordsByState": {'New Hampshire': 100, 'Colorado': 50}
        }
    )


def test_generate_manifest_files_no_batch_id():

    event = {
        'is_historical': 'true',
        'queueUrl': 'queueUrl',
        'receiptHandle': 'receiptHandle'
    }
    data = manifest_generator_lambda_handler.generate_manifest_files(event, context=None)

    event2 = event.copy()
    event2["batch_id"] = ""

    assert data == event2


def test_generate_manifest_files_batch_id():

    with pytest.raises(KeyError):
        event = {
            'batch_id': 'batch_id',
            'is_historical': 'true',
            'queueUrl': 'queueUrl',
            'receiptHandle': 'receiptHandle'
        }
        manifest_generator_lambda_handler.generate_manifest_files(event, context=None)

