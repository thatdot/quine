package com.thatdot.quine.app.model.ingest.serialization

import com.google.protobuf.Descriptors.Descriptor
import com.google.protobuf.{DynamicMessage, InvalidProtocolBufferException}

import com.thatdot.data.DataFoldableFrom
import com.thatdot.quine.app.data.QuineDataFoldersTo
import com.thatdot.quine.graph.cypher.Value

/** Parses Protobuf messages to cypher values according to a schema.
  */
class ProtobufParser(messageDescriptor: Descriptor) {

  @throws[InvalidProtocolBufferException]
  @throws[ClassCastException]
  def parseBytes(bytes: Array[Byte]): Value = {
    val dm: DynamicMessage = DynamicMessage.parseFrom(messageDescriptor, bytes)
    DataFoldableFrom.protobufDataFoldable.fold(dm, QuineDataFoldersTo.cypherValueFolder)
  }
}
