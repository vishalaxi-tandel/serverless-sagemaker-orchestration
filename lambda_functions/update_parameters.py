import boto3
import os

ssm = boto3.client('ssm')

def lambda_handler(event, context):
    endpoint_name = event['name']
    latest_data_upload = event['latest_data_upload']
    last_train_param = event['last_train_param']
    put_parameter(last_train_param, latest_data_upload)
    event['status'] = 'ParamsUpdated'
    return event
    
def put_parameter(name, value):
    """ Update Paramater Store parameter identified by input key with input value.
    Args:
        name (string): Name of parameter.
        value (string): Value to update parameter with.
    Returns:
        (None)
    """
    try:
        ssm.put_parameter(
            Name=name,
            Value=value,
            Type='String',
            Overwrite=True
        )
    except Exception as e:
        print(e)
        print('Unable to update parameters.')
        raise(e)