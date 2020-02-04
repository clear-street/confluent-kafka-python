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


import io
import logging
import struct
import warnings

from fastavro import schemaless_writer, schemaless_reader

from confluent_kafka.avro.serializer import SerializerError, ValueSerializerError, KeySerializerError

log = logging.getLogger(__name__)
MAGIC_BYTE = 0


class ContextStringIO(io.BytesIO):
    """
    Wrapper to allow use of StringIO via 'with' constructs.
    """

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
        return False


class MessageSerializer(object):
    """
    A helper class that can serialize and deserialize messages
    that need to be encoded or decoded using the schema registry.

    All encode_* methods return a buffer that can be sent to kafka.
    All decode_* methods accept a buffer received from kafka.
    """

    # TODO: Deprecate reader_[key|value]_schema args move into Schema object
    def __init__(self, registry_client, reader_key_schema=None, reader_value_schema=None):
        self.registry_client = registry_client
        self.reader_key_schema = reader_key_schema
        self.reader_value_schema = reader_value_schema
        warnings.warn(
            "MessageSerializer is being deprecated and will be removed in a future release."
            "Use AvroSerializer instead. ",
            category=DeprecationWarning, stacklevel=2)

    # TODO: Deprecate, rename serialize()
    def encode_record_with_schema(self, topic, schema, record, is_key=False):
        """
        Given a parsed Schema, encode a record for the given topic.  The
        record is expected to be a dictionary.
        The schema is registered with the subject of 'topic-value'

        :param str topic: Topic name
        :param Schema schema: Parsed schema
        :param object record: An object to serialize
        :param bool is_key: If the record is a key
        :returns: Encoded record with schema ID as bytes
        :rtype: bytes
        """

        subject_suffix = ('-key' if is_key else '-value')
        subject = topic + subject_suffix

        self.registry_client.register(subject, schema)

        with ContextStringIO() as outf:
            # Write the magic byte and schema ID in network byte order (big endian)
            outf.write(struct.pack('>bI', MAGIC_BYTE, schema.id))

            # write the record to the rest of the buffer
            schemaless_writer(outf, schema.schema, record)

            return outf.getvalue()

    def _deserialize(self, schema_id, data, is_key=False):
        """
        Deserializes bytes.

        :param int schema_id: Schema Registry Id
        :param BytesIO data: bytes to be decoded
        :param bool is_key: True if Message Key
        :returns: Decoded value
        :rtype: object
        """

        schema = self.registry_client.get_by_id(schema_id)
        reader_schema = self.reader_key_schema if is_key else self.reader_value_schema

        if reader_schema:
            return schemaless_reader(data, schema.schema, reader_schema.schema)
        return schemaless_reader(data, schema.schema)

    def decode_message(self, data, is_key=False):
        """
        Decode a message from kafka that has been encoded for use with
        the schema registry.

        :param str|bytes or None data: message key or value to be decoded
        :param bool is_key: True if data is Message Key
        :returns: Decoded value.
        :rtype object:
        """

        if data is None:
            return None

        if len(data) <= 5:
            raise SerializerError("message is too small to decode")

        with ContextStringIO(data) as payload:
            magic, schema_id = struct.unpack('>bI', payload.read(5))
            if magic != MAGIC_BYTE:
                raise SerializerError("message does not start with magic byte")

            return self._deserialize(schema_id, payload, is_key)


class MessageField(object):
    KEY = 0
    VALUE = 1


def topic_name_strategy(field=MessageField.VALUE):
    """


    :param int field: Either key or Value
    :returns: Filed specific topic name strategy function
    :rtype: callable(str, Schema)
    """
    if field is MessageField.VALUE:
        return topic_value_name_strategy
    return topic_key_name_strategy


def topic_key_name_strategy(topic, schema):
    """
        Returns a subject name in the form of {topic}-key

        See the Confluent docs for additional information.
        https://docs.confluent.io/current/schema-registry/serializer-formatter.html#how-the-naming-strategies-work

        :param str topic: Topic name
        :param Schema schema: Not used
        :returns: subject name
        :rtype: str
        """

    return topic + "-key"


def topic_value_name_strategy(topic, schema):
    """
    Returns a subject name in the form of {topic}-value

    See the Confluent docs for additional information.
    https://docs.confluent.io/current/schema-registry/serializer-formatter.html#how-the-naming-strategies-work

    :param str topic: Topic name
    :param Schema schema: Not used
    :returns: subject name
    :rtype: str
    """

    return topic + "-value"


def record_name_strategy(topic, schema):
    """
    Returns a subject name in the form of {schema name}

    See the Confluent docs for additional information.
    https://docs.confluent.io/current/schema-registry/serializer-formatter.html#how-the-naming-strategies-work

    :param str topic: Not used
    :param Schema schema: datum schema
    :returns: Subject name
    :rtype: str
    """
    return schema.name


def topic_record_name_strategy(topic, schema):
    """
    Returns a subject name in the form of {topic}-{schema name}

    See the Confluent docs for additional information.
    https://docs.confluent.io/current/schema-registry/serializer-formatter.html#how-the-naming-strategies-work

    :param str topic: Topic name
    :param Schema schema: datum schema
    :returns: Subject name
    :rtype: str
    """

    return topic + "-" + schema.name


class AvroSerializer(object):
    __slots__ = ['registry_client', 'namer', 'error', '_schema', '_hash']

    def __init__(self, registry_client, schema=None, name_strategy=None, field=MessageField.VALUE):
        self.registry_client = registry_client
        self.error = KeySerializerError if field == MessageField.KEY else ValueSerializerError
        self._schema = schema

        if name_strategy is None:
            name_strategy = topic_name_strategy(field)

        self.namer = name_strategy

    def __hash__(self):
        return hash(self._schema)

    def encode(self, topic, datum):
        """
        Given a parsed Schema, encode a record for the given topic.  The
        record is expected to be a dictionary.
        The schema is registered with the subject of 'topic-value'

        :param str topic: Topic name
        :param Schema schema: Parsed Schema
        :param AvroDatum datum: An object to serialize
        :returns: Encoded record with schema ID as bytes
        :rtype: bytes

        :raises: SerializerError
        """

        if datum.data is None:
            return

        schema = datum.schema
        datum = datum.data

        subject = self.namer(topic, schema)
        self.registry_client.register(subject, schema)

        with ContextStringIO() as outf:
            # Write the magic byte and schema ID in network byte order (big endian)
            outf.write(struct.pack('>bI', MAGIC_BYTE, schema.id))

            # write the record to the rest of the buffer
            schemaless_writer(outf, schema.schema, datum)

            return outf.getvalue()

    def decode(self, data, reader_schema=None):
        """
        Decode a message from kafka that has been encoded for use with
        the schema registry.

        :param str|bytes or None data: message key or value to be decoded
        :param Schema reader_schema: Optional compatible Schema to project value on.
        :returns: Decoded value.
        :rtype object:
        """

        if data is None:
            return None

        if len(data) <= 5:
            raise self.error("message is too small to decode")

        with ContextStringIO(data) as payload:
            magic, schema_id = struct.unpack('>bI', payload.read(5))
            if magic != MAGIC_BYTE:
                raise self.error("message does not start with magic byte")

            schema = self.registry_client.get_by_id(schema_id)
            if reader_schema:
                return schemaless_reader(payload, schema.schema, reader_schema.schema)
            return schemaless_reader(payload, schema.schema)
