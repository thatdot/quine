package com.thatdot.quine.app.serialization

import java.net.URL

sealed trait ProtobufSchemaError extends IllegalArgumentException

object ProtobufSchemaError {
  class UnreachableProtobufSchema(val fileUri: URL, cause: java.io.IOException)
      extends IllegalArgumentException(s"Unreachable protobuf schema file: $fileUri", cause)
      with ProtobufSchemaError

  class InvalidProtobufSchema(val fileUri: URL, cause: Throwable)
      extends IllegalArgumentException(s"Invalid protobuf schema file: $fileUri", cause)
      with ProtobufSchemaError

  class NoSuchMessageType(val typeName: String, val validTypes: Set[String])
      extends IllegalArgumentException(
        s"No protobuf message descriptor found with name $typeName in discovered types: $validTypes"
      )
      with ProtobufSchemaError

  class AmbiguousMessageType(val typeName: String, val possibleMatches: Set[String])
      extends IllegalArgumentException(
        s"""Multiple protobuf message descriptors found with name $typeName.
           |Consider using a fully-qualified name from among: $possibleMatches""".stripMargin.replace('\n', ' ')
      )
      with ProtobufSchemaError
}
