#!/usr/bin/python -W ignore
########################################################## {COPYRIGHT-TOP} ###
# Licensed Materials - Property of IBM
# 5737-I32
#
# (C) Copyright IBM Corp. 2019
#
# US Government Users Restricted Rights - Use, duplication, or
# disclosure restricted by GSA ADP Schedule Contract with IBM Corp.
########################################################## {COPYRIGHT-END} ###

import json
import logging
import os
import sys
from confluent_kafka import KafkaError

ENCODING = 'utf-8'

class ApplicationMessageBase():
    """A library to allow Applications to interact with a message source (only Kafka so far).

    Deals with message retrieval, messaging producing, and handles scalabilty of
    multiple clients. Must be created from an existing application, from which it
    can take the connection details.

    This script expect configuration parameters to be specified as environment
    variables.

    LOG_LEVEL .................. Log verbosity level (ERROR, WARNING, INFO, DEBUG)
                                 - default: DEBUG
    """

    def __init__(self, application):
        """Store Kafka connections from application."""
        self.kafka_consumer = application.kafka_consumer
        self.kafka_consumer.subscribe([application.work_q_name])
        self.kafka_producer = application.kafka_producer
        self.compl_q_name = application.compl_q_name
        self.application = application

        # Instantiate logger
        loglevels = {'INFO': logging.INFO, 'DEBUG': logging.DEBUG,
                     'ERROR': logging.ERROR, 'WARNING': logging.WARNING}
        log_level = os.environ.get('LOG_LEVEL', 'DEBUG')
        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        logging.basicConfig(stream=sys.stdout,
                            format=log_format,
                            level=loglevels[log_level])
        self.logger = logging.getLogger(__name__)

    def parse_work_message(self, msg):
        """Take an application work message, and parses it into a dictionary.

        Elements:
            action_params : As passed by PE, needs to be understood by calling application
            docs: list of docs to apply the actions to
        """
        parsed_msg = {}

        try:
            parsed_msg['action_params'] = msg.get('action_params', None)
            parsed_msg['docs'] = msg.get('docs', None)
        except (AttributeError, KeyError):
            self.logger.error("Message parse error - invalid work message format")

        return parsed_msg

    def decode_msg(self, msg):
        """Decode JSON message and log errors."""
        if msg:
            if not msg.error():
                return json.loads(msg.value().decode(ENCODING))

            elif msg.error().code() != KafkaError._PARTITION_EOF:
                self.logger.error(msg.error().code())

    def read_message(self, timeout=10):
        """
        Read JSON message and log errors.

        Before returning a message, make sure the run_id is not part of the ignored list from stopped policies.
        """
        msg = None
        msg_string = self.kafka_consumer.poll(timeout=timeout)
        if msg_string:
            try:
                msg = self.decode_msg(msg_string)
            except json.decoder.JSONDecodeError:
                self.logger.error("Message decode error - invalid JSON")

        # Override the message and drop it in the case that we have an ignored run_id
        try:
            if msg['run_id'] in self.application.kafka_ignored_run_ids:
                self.logger.debug("Dropping message due to ignored run_id %s", msg['run_id'])
                msg = None
        except (TypeError, KeyError):
            pass

        return msg

    def send_reply(self, response_msg):
        """Send message on kafka completion queue."""
        self.kafka_producer.produce(self.compl_q_name, str(response_msg))
        self.kafka_producer.flush()

        self.kafka_consumer.commit()


class ApplicationReplyMessage():
    """The reply message, and functions to build it."""

    def __init__(self, msg):
        """Initialize."""
        self.reply = {}
        self.reply['mo_ver'] = msg['mo_ver']
        self.reply['run_id'] = msg['run_id']
        self.reply['policy_id'] = msg['policy_id']
        self.reply['docs'] = []

    def add_result(self, status, key, tags=None):
        """Add result to reply message queue."""
        result = {'status': status, 'fkey': key.fkey, 'path': key.path if isinstance(key.path, str) else key.path.decode(ENCODING)}
        if tags:
            if isinstance(tags, dict):
                result['tags'] = tags
            else:
                raise ValueError('Result tags must be added as a dictionary')
        self.reply['docs'].append(result)

    def __str__(self):
        """Override string method for debug printing."""
        return json.dumps(self.reply)
