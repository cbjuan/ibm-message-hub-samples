# ibm-message-hub-samples
Examples on how to create message consumers & producers using IBM Message Hub. Coded in Python. Runs in local machine.

This repo contains 3 files:
 * consumer.py -> This script run a consumer that listens to a Kafka topic using IBM Message Hub. It runs in forever using a ```while True``` loop.
 * producer.py -> This script sends the message typed by user in the CLI to the Kafka topic specified in the config.
 * rest-example.py -> An example of how to consume info using the Kafka REST API.

Code adapted from https://github.com/ibm-messaging/message-hub-samples/tree/master/kafka-python-console-sample

To run this software (dependencies, requirements, etc.), use the guidelines available at 
https://github.com/ibm-messaging/message-hub-samples/blob/master/kafka-python-console-sample/README.md
