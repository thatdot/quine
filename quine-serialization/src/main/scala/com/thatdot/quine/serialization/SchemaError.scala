package com.thatdot.quine.serialization

import java.net.URL

sealed trait ProtobufSchemaError extends IllegalArgumentException
sealed trait AvroSchemaError extends IllegalArgumentException
sealed trait ProtobufSchemaMessageTypeException extends ProtobufSchemaError {
  def typeName: String
}

object AvroSchemaError {
  class UnreachableAvroSchema(val fileUri: URL, cause: java.io.IOException)
      extends IllegalArgumentException(s"Unreachable avro schema file: $fileUri", cause)
      with AvroSchemaError
  class InvalidAvroSchema(val fileUri: URL, cause: Throwable)
      extends IllegalArgumentException(s"Invalid avro schema file: $fileUri", cause)
      with AvroSchemaError

}

object ProtobufSchemaError {
  class UnreachableProtobufSchema(val fileUri: URL, cause: java.io.IOException)
      extends IllegalArgumentException(s"Unreachable protobuf schema file: $fileUri", cause)
      with ProtobufSchemaError

  class InvalidProtobufSchema(val fileUri: URL, cause: Throwable)
      extends IllegalArgumentException(s"Invalid protobuf schema file: $fileUri", cause)
      with ProtobufSchemaError

  class NoSuchMessageType(val typeName: String, val validTypes: Set[String])
      extends IllegalArgumentException(
        s"No protobuf message descriptor found with name $typeName in discovered types: $validTypes",
      )
      with ProtobufSchemaMessageTypeException

  class AmbiguousMessageType(val typeName: String, val possibleMatches: Set[String])
      extends IllegalArgumentException(
        s"""Multiple protobuf message descriptors found with name $typeName.
           |Consider using a fully-qualified name from among: $possibleMatches""".stripMargin.replace('\n', ' '),
      )
      with ProtobufSchemaMessageTypeException
}
