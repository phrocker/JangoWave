# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: Uid.proto

import sys
_b=sys.version_info[0]<3 and (lambda x:x) or (lambda x:x.encode('latin1'))
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor.FileDescriptor(
  name='Uid.proto',
  package='datawave.ingest.protobuf',
  syntax='proto2',
  serialized_options=_b('\n\030datawave.ingest.protobufH\001'),
  serialized_pb=_b('\n\tUid.proto\x12\x18\x64\x61tawave.ingest.protobuf\"]\n\x04List\x12\x0e\n\x06IGNORE\x18\x01 \x02(\x08\x12\r\n\x05\x43OUNT\x18\x02 \x02(\x04\x12\x0b\n\x03UID\x18\x03 \x03(\t\x12\x12\n\nREMOVEDUID\x18\x04 \x03(\t\x12\x15\n\rQUARANTINEUID\x18\x05 \x03(\tB\x1c\n\x18\x64\x61tawave.ingest.protobufH\x01')
)




_LIST = _descriptor.Descriptor(
  name='List',
  full_name='datawave.ingest.protobuf.List',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='IGNORE', full_name='datawave.ingest.protobuf.List.IGNORE', index=0,
      number=1, type=8, cpp_type=7, label=2,
      has_default_value=False, default_value=False,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='COUNT', full_name='datawave.ingest.protobuf.List.COUNT', index=1,
      number=2, type=4, cpp_type=4, label=2,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='UID', full_name='datawave.ingest.protobuf.List.UID', index=2,
      number=3, type=9, cpp_type=9, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='REMOVEDUID', full_name='datawave.ingest.protobuf.List.REMOVEDUID', index=3,
      number=4, type=9, cpp_type=9, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='QUARANTINEUID', full_name='datawave.ingest.protobuf.List.QUARANTINEUID', index=4,
      number=5, type=9, cpp_type=9, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto2',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=39,
  serialized_end=132,
)

DESCRIPTOR.message_types_by_name['List'] = _LIST
_sym_db.RegisterFileDescriptor(DESCRIPTOR)

List = _reflection.GeneratedProtocolMessageType('List', (_message.Message,), {
  'DESCRIPTOR' : _LIST,
  '__module__' : 'Uid_pb2'
  # @@protoc_insertion_point(class_scope:datawave.ingest.protobuf.List)
  })
_sym_db.RegisterMessage(List)


DESCRIPTOR._options = None
# @@protoc_insertion_point(module_scope)