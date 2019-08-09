from lambdas.manifest_generator_lambda_handler import *


def lambda_handler(event, context):
    # manifest_generator_handle_event = GenerateManifests()
    data = generate_manifest_files(event, context)
    LoggerUtility.logInfo("Manifest files generated for batchId : "+ data['batch_id'])
    return data

#lambda_handler({'table_type':'alert','is_historical':'true', 'batch_id' :'1531486938' },{})
#lambda_handler({'table_type':'irregularity','is_historical':'true'},{})
#lambda_handler({'table_type':'irregularity','is_historical':'true'},{})