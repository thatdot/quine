package com.thatdot.quine.persistor

import java.nio.ByteBuffer

import scala.util.Try

import com.google.flatbuffers.FlatBufferBuilder

import com.thatdot.quine.util.Packing

/** Simplify writing a flatbuffer-based binary format that undergoes packing */
abstract class PackedFlatBufferBinaryFormat[A] extends BinaryFormat[A] {
  def writeToBuffer(builder: FlatBufferBuilder, a: A): Int

  def readFromBuffer(buffer: ByteBuffer): A

  final def write(a: A): Array[Byte] = {
    // FBS
    val builder = new FlatBufferBuilder()
    val offset = writeToBuffer(builder, a)
    builder.prep(8, 0)
    builder.finish(offset)

    // Packing
    Packing.pack(builder.sizedByteArray())
  }

  final def read(bytes: Array[Byte]): Try[A] = Try {
    // Unpacking
    val unpacked = Packing.unpack(bytes)

    // FBS
    readFromBuffer(ByteBuffer.wrap(unpacked))
  }
}

object PackedFlatBufferBinaryFormat {

  /** FlatBuffer serialization APIs return this to represent an offset in a buffer */
  type Offset = Int

  /** Offset for fields that are optional and absent */
  val NoOffset = 0

  def emptyTable(builder: FlatBufferBuilder): Offset = {
    builder.startTable(0)
    builder.endTable()
  }

  /** Convenience type for handling serialization of unions
    *
    * @param typ variant of the union
    * @param offset index of the written variant in the buffer (or 0 if none)
    */
  final case class TypeAndOffset(typ: Byte, offset: Offset)
}
