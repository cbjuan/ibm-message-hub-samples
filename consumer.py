"""
Author: Juan Cruz-Benito, 2017

Adapted from https://github.com/ibm-messaging/message-hub-samples/tree/master/kafka-python-console-sample

To run this software, use the guidelines available at
https://github.com/ibm-messaging/message-hub-samples/blob/master/kafka-python-console-sample/README.md
"""

import json
import os
import signal
import sys

import requests


class MessageHubRest(object):

    def __init__(self, rest_endpoint, api_key):
        self.path = '{0}/admin/topics'.format(rest_endpoint)
        self.headers = {
            'X-Auth-Token': api_key,
            'Content-Type': 'application/json'
        }

    def create_topic(self, topic_name, partitions=1, retention_hours=24):
        """
        POST /admin/topics
        """
        payload = {
            'name': topic_name,
            'partitions': partitions,
            'configs': {
                'retentionMs': retention_hours * 60 * 60 * 1000
            }
        }
        return requests.post(self.path, headers=self.headers, json=payload)

    def list_topics(self):
        """
        GET /admin/topics
        """
        return requests.get(self.path, headers=self.headers)


class ConsumerTask(object):

    def __init__(self, is_bluemix, conf, topic_name):
        try:
            from confluent_kafka import Consumer
        except:
            from confluent_kafka_prebuilt import Consumer
        self.consumer = Consumer(conf)
        self.topic_name = topic_name
        self.running = True

    def stop(self):
        self.running = False

    def run(self):
        print('The consumer has started')
        self.consumer.subscribe([self.topic_name])
        while self.running:
            msg = self.consumer.poll(1)
            if msg is not None and msg.error() is None:
                print('Message consumed: topic={0}, partition={1}, offset={2}, key={3}, value={4}'.format(
                    msg.topic(),
                    msg.partition(),
                    msg.offset(),
                    msg.key(),
                    msg.value()))
            else:
                pass
        self.consumer.unsubscribe()
        self.consumer.close()


class MessageHubSample(object):

    def __init__(self):
        # Configure the proper values (get them from IBM Bluemix service) for:
        # topic_name
        # opts['brokers']
        # opts['rest_endpoint']
        # opts['api_key']
        # opts['ca_location']

        self.topic_name = ''
        self.opts = {}
        self.run_consumer = True
        self.consumer = None
        self.bluemix = True

        # Running locally on development machine
        self.bluemix = False
        print('Running in local mode.')

        self.opts['brokers'] = ""
        self.opts['rest_endpoint'] = ""
        self.opts['api_key'] = ""
        self.opts['username'] = self.opts['api_key'][0:16]
        self.opts['password'] = self.opts['api_key'][16:48]

        # Bluemix/Ubuntu: '/etc/ssl/certs'
        # Red Hat: '/etc/pki/tls/cert.pem',
        # Mac OS X: select System root certificates from Keychain Access and export as .pem on the filesystem
        self.opts['ca_location'] = ""
        if not os.path.exists(self.opts['ca_location']):
            print('Error - Failed to access <cert_location> : {0}'.format(self.opts['ca_location']))
            sys.exit(-1)

        # print('Kafka Endpoints: {0}'.format(self.opts['brokers']))
        # print('Admin REST Endpoint: {0}'.format(self.opts['rest_endpoint']))

        if any(k not in self.opts for k in ('brokers', 'username', 'password', 'ca_location', 'rest_endpoint', 'api_key')):
            print('Error - Failed to retrieve options. Check that app is bound to a Message Hub service or that command line options are correct.')
            sys.exit(-1)

        # Use Message Hub's REST admin API to create the topic
        # with 1 partition and a retention period of 24 hours.
        rest_client = MessageHubRest(self.opts['rest_endpoint'], self.opts['api_key'])
        print('Creating the topic {0} with Admin REST API'.format(self.topic_name))
        response = rest_client.create_topic(self.topic_name, 1, 24)
        print(response.text)

        # Use Message Hub's REST admin API to list the existing topics
        print('Admin REST Listing Topics:')
        response = rest_client.list_topics()
        print(response.text)


if __name__ == "__main__":
    app = MessageHubSample()
    print('This sample app will run until interrupted.')

    driver_options = {
        'bootstrap.servers': app.opts['brokers'],
        'security.protocol': 'SASL_SSL',
        'ssl.ca.location': app.opts['ca_location'],
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': app.opts['username'],
        'sasl.password': app.opts['password'],
        'api.version.request': True
    }
    consumer_opts = {
        'client.id': 'kafka-python-console-sample-consumer',
        'group.id': 'kafka-python-console-sample-group'
    }

    # Add the common options to consumer and producer
    for key in driver_options:
        consumer_opts[key] = driver_options[key]

    consumer = ConsumerTask(None, consumer_opts, app.topic_name)
    while True:
        consumer.run()