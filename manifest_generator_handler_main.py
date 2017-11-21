from lambdas.manifest_generator_lambda_handler import *


def lambda_handler(event, context):
    # manifest_generator_handle_event = GenerateManifests()
    batch_id = generate_manifest_files(event, context)
    persist_curated_records_function_name = os.environ['PERSIST_TO_REDSHIFT_LAMBDA_FUNCTION_NAME']
    client = boto3.client('lambda')
    LoggerUtility.logInfo("Invoking Persist curated records lambda function")
    payload = dict()
    payload['batch_id'] = batch_id
    response = client.invoke(FunctionName=persist_curated_records_function_name,
                             InvocationType='Event',
                             Payload=json.dumps(payload))
    LoggerUtility.logInfo("Response from invoked lambda function - {}".format(response))
