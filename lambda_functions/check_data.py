import datetime
import boto3
import json
import os

# Prefix to give to training jobs, models, and endpoint configs, and endpoint name
MODEL_PREFIX = os.environ['MODEL_PREFIX'] 

# Name of Parameter Store key corresponding to value of last successful training job date
LAST_TRAIN_PARAM = '/models/{}/train/latest'.format(MODEL_PREFIX) 

# Time interval to look for training data files to run training on
INTERVAL = int(os.environ['INTERVAL'])

# Name of bucket containing training data and to output model artifacts to
BUCKET = os.environ['BUCKET'] # Name of bucket

# Prefix of training data files in bucket
TRAIN_SET_PREFIX = os.path.join('data', MODEL_PREFIX, 'train')

# Path to location of training data files
TRAIN_SET_PATH = os.path.join('s3://', BUCKET, TRAIN_SET_PREFIX, '')

# Training manifest object key
TRAIN_MANIFEST_KEY = os.path.join(TRAIN_SET_PREFIX, 'manifest')

# Full URI of training data manifest file
TRAIN_MANIFEST_URI = os.path.join('s3://', BUCKET, TRAIN_MANIFEST_KEY)

# Path to output model artifacts to
OUTPUT_PATH = os.path.join('s3://', BUCKET, 'models', MODEL_PREFIX, '')

s3 = boto3.client('s3')
ssm = boto3.client('ssm')

def lambda_handler(event, context):
    time = event['time']
    print('Generating manifest...')
    dates = get_dates(INTERVAL) # Get datetime strings for the dates in the interval we want to train on
    filenames = ['{}.csv'.format(date) for date in dates] # Convert datetime strings to filenames
    extant_objects = check_objects_exist(filenames) # Check to see if objects matching the filenames exist in S3
    latest_data_upload = get_latest_date(extant_objects) # Find the date of the last data file uploaded
    last_train_date = ssm.get_parameter(Name=LAST_TRAIN_PARAM)['Parameter']['Value'] # Retrieve the date of the last successful training job from Parameter Store
    if (latest_data_upload is None) or (latest_data_upload <= last_train_date):
        print('No new data uploaded since last training run.')
        print('Skipping training until next scheduled training run.')
        return {
            'no_new_data': True
        }
    body = make_manifest(extant_objects)
    print('Uploading manifest to S3...')
    put_manifest(body)
    return {
        'time': time,
        'train_manifest_uri': TRAIN_MANIFEST_URI,
        's3_output_path': OUTPUT_PATH,
        'last_train_param': LAST_TRAIN_PARAM,
        'latest_data_upload': latest_data_upload,
        'endpoint': MODEL_PREFIX,
        'no_new_data': False
    }

def get_dates(interval):
    """ Creates datetime year-month-date strings for the input time interval.
    Args:
        interval (int): Time interval in days
    Returns:
        (list)
        List of datetime strings.
    """
    dates = list()
    today = datetime.date.today()
    for num_day in range(interval):
        day = today - datetime.timedelta(days=num_day)
        dates.append(day.strftime("%Y-%m-%d"))
    return dates
    
def get_latest_date(keys):
    """ Munges datetimes from a list of object keys where each key contains a datetime substring and returns latest
    Args:
        keys (list): List of object key strings
    Returns:
        (string or None)
        Latest datetime munged from datetimes in keys
        None if the input list of keys is empty or there are no valid datetimes munged from the keys
    """
    dates = [os.path.basename(key).split('.')[0] for key in keys]
    if len(dates) == 0:
        return None
    elif len(dates) == 1:
        return dates[0]
    return sorted(dates)[-1]
    
def check_objects_exist(filenames):
    """ Checks to see if the input filenames exist as objects in S3
    Args:
        filenames (list): List of filename strings to check S3 for
    Returns:
        (list)
        Filtered list containing string paths of filenames that exist as objects in S3
    """
    exists = list()
    for filename in filenames: 
        val = check_object_exists(filename)
        if val is True:
            exists.append(filename)
    return exists
    
    
def check_object_exists(filename):
    """ Checks to see if the input filename exists as object in S3
    Args:
        filename (string): Filename to check S3 for
    Returns:
        (boolean)
        True if object corresponding to filename exists in S3
        False otherwise
    """
    key = os.path.join(TRAIN_SET_PREFIX, filename)
    try:
        response = s3.head_object(
            Bucket=BUCKET,
            Key=key
        )
    except Exception as e:
        print('Unable to find object "{}" in bucket "{}".'.format(key, BUCKET))
        print('The URI for this object will not be added to the manifest.')
        return False
    return True
    
def make_manifest(keys):
    """ Creates a SageMaker S3 object json manifest from the input object keys 
    Args:
        keys (list): S3 object keys to add to manifest
    Returns:
        (bytes)
        UTF-8 encoded bytes object of stringified manifest json
    """
    payload = list()
    prefix = {'prefix': TRAIN_SET_PATH}
    payload.append(prefix)
    for key in keys:
        payload.append(key)
    return json.dumps(payload).encode()

def put_manifest(body):
    """ Upload manifest body to object in S3
    Args:
        body (bytes): UTF-8 encoded bytes object of stringified manifest json
    Returns:
        (None)
    """
    try:
        s3.put_object(
            Body=body,
            Bucket=BUCKET,
            ContentType='text/plain',
            Key=TRAIN_MANIFEST_KEY
        )
    except Exception as e:
        print(e)
        print('Unable to put manifest to s3.')
        raise(e)
