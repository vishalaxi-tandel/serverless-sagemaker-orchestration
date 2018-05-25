import boto3
import os

# ARN of IAM role Amazon SageMaker can assume to access model artifacts and docker image for deployment
EXECUTION_ROLE = os.environ['EXECUTION_ROLE']

# Instance type to deploy trained model to
INSTANCE_TYPE = os.environ['INSTANCE_TYPE'] 

sagemaker = boto3.client('sagemaker')

def lambda_handler(event, context):
    name = event['name']
    endpoint = event['endpoint']
    model_data_url = event['model_data_url']
    container = event['container']
    print('Creating model resource from training artifact...')
    create_model(name, container, model_data_url)
    print('Creating endpoint configuration...')
    create_endpoint_config(name)
    print('Checking if model endpoint already exists...')
    if check_endpoint_exists(endpoint):
        print('Existing endpoint found for model. Updating existing model endpoint...')
        update_endpoint(endpoint, name)
    else:
        print('There is no existing endpoint for this model. Creating new model endpoint...')
        create_endpoint(endpoint, name)
    event['stage'] = 'Deployment'
    event['status'] = 'Creating'
    event['message'] = 'Started deploying model "{}" to endpoint "{}"'.format(name, endpoint)
    return event

def create_model(name, container, model_data_url):
    """ Create SageMaker model.
    Args:
        name (string): Name to label model with
        container (string): Registry path of the Docker image that contains the model algorithm
        model_data_url (string): URL of the model artifacts created during training to download to container
    Returns:
        (None)
    """
    try:
        sagemaker.create_model(
            ModelName=name,
            PrimaryContainer={
                'Image': container,
                'ModelDataUrl': model_data_url
            },
            ExecutionRoleArn=EXECUTION_ROLE
        )
    except Exception as e:
        print(e)
        print('Unable to create model.')
        raise(e)
    
def create_endpoint_config(name):
    """ Create SageMaker endpoint configuration. 
    Args:
        name (string): Name to label endpoint configuration with.
    Returns:
        (None)
    """
    try:
        sagemaker.create_endpoint_config(
            EndpointConfigName=name,
            ProductionVariants=[
                {
                    'VariantName': 'prod',
                    'ModelName': name,
                    'InitialInstanceCount': 1,
                    'InstanceType': INSTANCE_TYPE
                }
            ]
        )
    except Exception as e:
        print(e)
        print('Unable to create endpoint configuration.')
        raise(e)

def check_endpoint_exists(endpoint_name):
    """ Check if SageMaker endpoint for model already exists.
    Args:
        endpoint_name (string): Name of endpoint to check if exists.
    Returns:
        (boolean)
        True if endpoint already exists.
        False otherwise.
    """
    try:
        sagemaker.describe_endpoint(
            EndpointName=endpoint_name
        )
        return True
    except Exception as e:
        return False

    
def create_endpoint(endpoint_name, config_name):
    """ Create SageMaker endpoint with input endpoint configuration.
    Args:
        endpoint_name (string): Name of endpoint to create.
        config_name (string): Name of endpoint configuration to create endpoint with.
    Returns:
        (None)
    """
    try:
        sagemaker.create_endpoint(
            EndpointName=endpoint_name,
            EndpointConfigName=config_name
        )
    except Exception as e:
        print(e)
        print('Unable to create endpoint.')
        raise(e)

def update_endpoint(endpoint_name, config_name):
    """ Update SageMaker endpoint to input endpoint configuration. 
    Args:
        endpoint_name (string): Name of endpoint to update.
        config_name (string): Name of endpoint configuration to update endpoint with.
    Returns:
        (None)
    """
    try:
        sagemaker.update_endpoint(
            EndpointName=endpoint_name,
            EndpointConfigName=config_name
        )
    except Exception as e:
        print(e)
        print('Unable to update endpoint.')
        raise(e)

