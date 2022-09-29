package com.thatdot.quine.persistor.codecs

import java.nio.ByteBuffer

import com.google.flatbuffers.FlatBufferBuilder

import com.thatdot.quine.model.QuineValue
import com.thatdot.quine.persistence
import com.thatdot.quine.persistor.PackedFlatBufferBinaryFormat.Offset
import com.thatdot.quine.persistor.{BinaryFormat, PackedFlatBufferBinaryFormat}

object QuineValueCodec extends PersistenceCodec[QuineValue] {

  val format: BinaryFormat[QuineValue] = new PackedFlatBufferBinaryFormat[QuineValue] {
    def writeToBuffer(builder: FlatBufferBuilder, quineValue: QuineValue): Offset =
      writeQuineValue(builder, quineValue)

    def readFromBuffer(buffer: ByteBuffer): QuineValue =
      readQuineValue(persistence.QuineValue.getRootAsQuineValue(buffer))
  }

}
