"""
Author: Juan Cruz-Benito, 2017

Adapted from https://github.com/ibm-messaging/message-hub-samples/tree/master/kafka-python-console-sample

To run this software, use the guidelines available at
https://github.com/ibm-messaging/message-hub-samples/blob/master/kafka-python-console-sample/README.md
"""

import json
import requests

# Configure the proper values (get them from IBM Bluemix service) for:
kafka_rest_url = ""
kafka_topic = ""
auth_token = ""

headers = {
    'X-Auth-Token': auth_token,
    'Content-Type': 'application/json'
}


response = requests.get(kafka_rest_url+'/topics/'+kafka_topic, headers=headers)
print(response.content)
