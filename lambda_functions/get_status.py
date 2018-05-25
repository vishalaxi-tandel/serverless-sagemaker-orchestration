import boto3
import os

sagemaker = boto3.client('sagemaker')


def lambda_handler(event, context):
    stage = event['stage']
    if stage == 'Training':
        name = event['name']
        training_details = describe_training_job(name)
        status = training_details['TrainingJobStatus']
        if status == 'Completed':
            s3_output_path = training_details['OutputDataConfig']['S3OutputPath']
            model_data_url = os.path.join(s3_output_path, name, 'output/model.tar.gz')
            event['message'] = 'Training job "{}" complete. Model data uploaded to "{}"'.format(name, model_data_url)
            event['model_data_url'] = model_data_url
        elif status == 'Failed':
            failure_reason = training_details['FailureReason']
            event['message'] = 'Training job failed. {}'.format(failure_reason)
    elif stage == 'Deployment':
        name = event['endpoint']
        endpoint_details = describe_endpoint(name)
        status = endpoint_details['EndpointStatus']
        if status == 'InService':
            event['message'] = 'Deployment completed for endpoint "{}".'.format(name)
        elif status == 'Failed':
            failure_reason = endpoint_details['FailureReason']
            event['message'] = 'Deployment failed for endpoint "{}". {}'.format(name, failure_reason)
        elif status == 'RollingBack':
            event['message'] = 'Deployment failed for endpoint "{}", rolling back to previously deployed version.'.format(name)
    event['status'] = status
    return event


def describe_training_job(name):
    """ Describe SageMaker training job identified by input name.
    Args:
        name (string): Name of SageMaker training job to describe.
    Returns:
        (dict)
        Dictionary containing metadata and details about the status of the training job.
    """
    try:
        response = sagemaker.describe_training_job(
            TrainingJobName=name
        )
    except Exception as e:
        print(e)
        print('Unable to describe training job.')
        raise(e)
    return response
    
def describe_endpoint(name):
    """ Describe SageMaker endpoint identified by input name.
    Args:
        name (string): Name of SageMaker endpoint to describe.
    Returns:
        (dict)
        Dictionary containing metadata and details about the status of the endpoint.
    """
    try:
        response = sagemaker.describe_endpoint(
            EndpointName=name
        )
    except Exception as e:
        print(e)
        print('Unable to describe endpoint.')
        raise(e)
    return response