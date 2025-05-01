package com.thatdot.quine.persistor.codecs

import java.nio.ByteBuffer

import com.google.flatbuffers.FlatBufferBuilder

import com.thatdot.quine.graph.cypher.quinepattern.{QueryPlan, QuinePatternUnimplementedException}
import com.thatdot.quine.persistor.PackedFlatBufferBinaryFormat.Offset
import com.thatdot.quine.persistor.{BinaryFormat, PackedFlatBufferBinaryFormat}

object QueryPlanCodec extends PersistenceCodec[QueryPlan] {

  val format: BinaryFormat[QueryPlan] = new PackedFlatBufferBinaryFormat[QueryPlan] {
    def writeToBuffer(builder: FlatBufferBuilder, qp: QueryPlan): Offset = throw new QuinePatternUnimplementedException(
      "Serialization of query plans not implemented",
    )

    def readFromBuffer(buffer: ByteBuffer): QueryPlan = throw new QuinePatternUnimplementedException(
      "Deserialization of query plans not implemented",
    )
  }
}
