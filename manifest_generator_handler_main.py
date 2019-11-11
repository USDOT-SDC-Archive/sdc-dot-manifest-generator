from lambdas.manifest_generator_lambda_handler import *


def lambda_handler(event, context):
    data = generate_manifest_files(event)
    LoggerUtility.log_info("Manifest files generated for batchId : " + data['batch_id'])
    return data
