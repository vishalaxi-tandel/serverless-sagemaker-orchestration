import os
import urllib


ENABLED = os.environ['ENABLED'] # Whether to enable posting of Slack messages.
ACCESS_TOKEN = os.environ['ACCESS_TOKEN']  # Slack OAuth access token from environment variables
CHANNEL = os.environ['CHANNEL']  # Slack channel to post message to


def lambda_handler(event, context):
    message = event['message']
    if ENABLED == 'True':
        post_message(message)
    return event


def post_message(message):
    """ Posts message to Slack channel via Slack API.
    Args:
        message (string): Message to post to channel
    Returns:
        (None)
    """
    url = 'https://slack.com/api/chat.postMessage'
    data = urllib.parse.urlencode(
        (
            ("token", ACCESS_TOKEN),
            ("channel", CHANNEL),
            ("text", message)
        )
    )
    data = data.encode("ascii")
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    request = urllib.request.Request(url, data, headers)
    urllib.request.urlopen(request)