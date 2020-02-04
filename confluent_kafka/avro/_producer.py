#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2020 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from confluent_kafka import Producer
from confluent_kafka.avro.cached_schema_registry_client import CachedSchemaRegistryClient
from confluent_kafka.avro.serializer import KeySerializerError, ValueSerializerError
from confluent_kafka.avro.serializer.message_serializer import AvroSerializer, MessageField, topic_key_name_strategy, \
    topic_value_name_strategy


class AvroProducer(Producer):
    """
        Kafka Producer client which does avro schema encoding to messages.
        Handles schema registration, Message serialization.

        Constructor takes below parameters.

        :param dict config: Config parameters containing url for schema registry (``schema.registry.url``)
                            and the standard Kafka client configuration (``bootstrap.servers`` et.al).
        :param Schema default_key_schema: Optional default avro schema for key
        :param Schema default_value_schema: Optional default avro schema for value
        :param CachedSchemaRegistryClient schema_registry: Optional CachedSchemaRegistryClient instance
    """
    __slots__ = ['_key_serializer', '_value_serializer', 'key_schema', 'value_schema']

    def __init__(self, config,
                 default_key_schema=None, default_value_schema=None,
                 schema_registry=None):

        sr_conf = {key.replace("schema.registry.", ""): value
                   for key, value in config.items() if key.startswith("schema.registry")}

        if sr_conf.get("basic.auth.credentials.source") == 'SASL_INHERIT':
            sr_conf['sasl.mechanisms'] = config.get('sasl.mechanisms', '')
            sr_conf['sasl.username'] = config.get('sasl.username', '')
            sr_conf['sasl.password'] = config.get('sasl.password', '')

        ap_conf = {key: value
                   for key, value in config.items() if not key.startswith("schema.registry")}

        if schema_registry is None:
            schema_registry = CachedSchemaRegistryClient(sr_conf)
        elif sr_conf.get("url", None) is not None:
            raise ValueError("Cannot pass schema_registry along with schema.registry.url config")

        self.key_schema = default_key_schema
        self.value_schema = default_value_schema

        self._key_serializer = AvroSerializer(schema_registry,
                                              schema=default_key_schema,
                                              name_strategy=sr_conf.get('key.subject.name.strategy',
                                                                        topic_key_name_strategy),
                                              field=MessageField.KEY)
        self._value_serializer = AvroSerializer(schema_registry,
                                                schema=default_value_schema,
                                                name_strategy=sr_conf.get('key.subject.name.strategy',
                                                                          topic_value_name_strategy),
                                                field=MessageField.VALUE)
        super(AvroProducer, self).__init__(ap_conf)

    def produce(self, topic, **kwargs):
        """
            Asynchronously sends message to Kafka by encoding with specified or default avro schema.

            :param str topic: topic name
            :param object value: An object to serialize
            :param Schema value_schema: Avro schema for value
            :param object key: An object to serialize
            :param Schema key_schema: Avro schema for key

            Plus any other parameters accepted by confluent_kafka.Producer.produce

            :raises SerializerError: On serialization failure
            :raises BufferError: If producer queue is full.
            :raises KafkaException: For other produce failures.
        """

        key = kwargs.pop('key', None)
        value = kwargs.pop('value', None)

        key_schema = kwargs.pop('key_schema', self.key_schema)
        value_schema = kwargs.pop('value_schema', self.value_schema)

        if value is not None:
            if value_schema is None:
                raise ValueSerializerError("Avro schema required for values")
            else:
                value = self._value_serializer.encode(topic, value_schema(value))

        if key is not None:
            if key_schema is None:
                raise KeySerializerError("Avro schema required for values")
            else:
                key = self._key_serializer.encode(topic, key_schema(key))

        super(AvroProducer, self).produce(topic, value, key, **kwargs)
