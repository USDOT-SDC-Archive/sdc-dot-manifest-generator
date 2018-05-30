from lambdas.manifest_generator_lambda_handler import *


def lambda_handler(event, context):
    # manifest_generator_handle_event = GenerateManifests()
    batch_id = generate_manifest_files(event, context)
    LoggerUtility.logInfo("Manifest files generated for batchId : "+batch_id)
