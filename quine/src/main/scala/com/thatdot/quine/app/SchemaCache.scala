package com.thatdot.quine.app

import com.thatdot.quine.serialization.{AvroSchemaCache, ProtobufSchemaCache}

trait SchemaCache {
  def protobufSchemaCache: ProtobufSchemaCache
  def avroSchemaCache: AvroSchemaCache
}
