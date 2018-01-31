from common.logger_utility import *
import boto3
from boto3.dynamodb.conditions import Attr, Key
import functools
from concurrent.futures import ThreadPoolExecutor
import json
import uuid


def __get_ready_for_processing_batches(batch_table_name):
    try:
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(batch_table_name)
        response = table.scan(
            FilterExpression=Attr('ReadyForProcessing').eq('true')
        )
        LoggerUtility.logInfo("Response from scan - {}".format(response))
        if response['Count'] == 0:
            batch_id = ""
        else:
            for item in response['Items']:
                batch_id = item['BatchId']

        LoggerUtility.logInfo("Current batch id - {}".format(batch_id))
    except Exception as e:
        LoggerUtility.logError("Unable to fetch batch id from batch table - {}".format(batch_table_name))
        raise e
    return batch_id


def __run_in_parallel(job_generator, max_workers):
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(job) for job in job_generator]
    for future in futures:
        exception = future.exception()
        if exception is not None:
            raise exception


def __initiate_manifest_process(batch_id):

    yield from (
        functools.partial(__process_manifest_files, batch_id, "alert"),
        functools.partial(__process_manifest_files, batch_id, "jam"),
        functools.partial(__process_manifest_files, batch_id, "jam_point_sequence"),
        functools.partial(__process_manifest_files, batch_id, "irregularity"),
        functools.partial(__process_manifest_files, batch_id, "irregularity_point_sequence"),
        functools.partial(__process_manifest_files, batch_id, "irregularity_alert"),
        functools.partial(__process_manifest_files, batch_id, "irregularity_jam"),
        functools.partial(__process_manifest_files, batch_id, "corridor_reading"),
        functools.partial(__process_manifest_files, batch_id, "corridor_point_sequence"),
        functools.partial(__process_manifest_files, batch_id, "corridor_alert"),
        functools.partial(__process_manifest_files, batch_id, "corridor_jam")
    )


def __process_manifest_files(batch_id, table_name):
    try:
        LoggerUtility.logInfo("Batch id - {} and table_name - {}".format(batch_id, table_name))
        curated_records_table_name = os.environ['DDB_CURATED_RECORDS_TABLE_ARN'].split('/')[1]
        curated_records_index_name = os.environ['DDB_CURATED_RECORDS_INDEX_NAME']
        manifest_files_table_name = os.environ['DDB_MANIFEST_TABLE_ARN'].split('/')[1]
        curated_bucket_name = os.environ['CURATED_BUCKET_NAME']
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(curated_records_table_name)
        response = table.query(
            IndexName=curated_records_index_name,
            KeyConditionExpression=Key('BatchId').eq(batch_id) & Key('DataTableName').eq(table_name)
        )

        if response['Count'] > 0:
            records = response['Items']
            while response.get('LastEvaluatedKey'):
                LoggerUtility.logInfo("More records present, so querying the index to get additional records "
                                      "for table - {}".format(table_name))
                response = table.query(
                    IndexName=curated_records_index_name,
                    KeyConditionExpression=Key('BatchId').eq(batch_id) & Key('DataTableName').eq(table_name),
                    ExclusiveStartKey=response['LastEvaluatedKey']
                )
                records.extend(response['Items'])
            LoggerUtility.logInfo("Completed fetching all records from index for table - {} "
                                  "with count - {}".format(table_name, len(records)))
            entries_list = []
            for record in records:
                entries = dict()
                entries['url'] = record['S3Key']
                entries['mandatory'] = bool("true")
                entries_list.append(entries)

            if len(entries_list) > 0:
                json_data = json.dumps(entries_list)
                modified_json = json.dumps('{"entries":' + json_data + '}')
                manifest_json = json.JSONDecoder().decode(modified_json)
                output_to_file = json.loads(manifest_json)
                manifest_file_name = "/tmp/" + str(uuid.uuid4()) + ".manifest"
                with open(manifest_file_name, 'w') as manifest_file:
                    json.dump(output_to_file, manifest_file)

                manifest_s3_key = "manifest/" + batch_id + "/" + table_name + "/" + os.path.basename(manifest_file_name)
                s3 = boto3.client('s3')

                s3.upload_file(manifest_file_name, curated_bucket_name, manifest_s3_key)
                LoggerUtility.logInfo(
                    "Successfully uploaded manifest file to s3 for batch id - {} and table name - {}".format(batch_id,
                                                                                                             table_name))

                table = dynamodb.Table(manifest_files_table_name)
                response = table.put_item(
                    Item={
                        "ManifestId": str(uuid.uuid4()),
                        "BatchId": batch_id,
                        "TableName": table_name,
                        "ManifestS3Key": manifest_s3_key,
                        "FileStatus": "open"
                    }
                )
                LoggerUtility.logInfo("Response from put item - {}".format(response))
                LoggerUtility.logInfo("Successfully created an item in dyanmodb table - {} for batch id - {} "
                                      "and table name - {}".format(manifest_files_table_name, batch_id, table_name))
        else:
            LoggerUtility.logInfo("No records to process for table - {}. Exiting the process".format(table_name))

    except Exception as e:
        LoggerUtility.logError("Failed to upload manifest file for batch id - {} "
                               "and table name - {} with exception - {}".format(batch_id, table_name, e))



def delete_batch_id(batch_id, batch_table_name):
    try:
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(batch_table_name)
        response = table.delete_item(
            Key={
                'BatchId': batch_id
            },
            ConditionExpression=Attr('IsCurrent').eq('false') & Attr('ReadyForProcessing').eq('true')
        )
        LoggerUtility.logInfo("Successfully deleted batch id - {}, response - {}".format(batch_id, response))

    except Exception as e:
        LoggerUtility.logError("Unable to delete batch id - {} from batch table - {}".format(batch_id, batch_table_name))
        raise e


def generate_manifest_files(event, context):
    LoggerUtility.setLevel()
    LoggerUtility.logInfo("Initiating manifest process")
    batch_table_name = os.environ['DDB_BATCH_TABLE_ARN'].split('/')[1]

    batch_id = __get_ready_for_processing_batches(batch_table_name)
    if batch_id != "":
        __run_in_parallel(__initiate_manifest_process(batch_id), max_workers=15)
        delete_batch_id(batch_id, batch_table_name)

    LoggerUtility.logInfo("Completed manifest process")
    return batch_id

