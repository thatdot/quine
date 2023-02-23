package com.thatdot.quine.bolt

import java.nio.ByteOrder
import java.nio.charset.StandardCharsets.UTF_8

import akka.util.{ByteIterator, ByteString, ByteStringBuilder}

import com.thatdot.quine.graph.cypher.{Expr, Value}
import com.thatdot.quine.model.QuineIdProvider

/** We need `idProvider` because serializing a `Node` involves converting to
  * cypher ID's.
  *
  * @see <https://boltprotocol.org/v1>
  */
final case class Serialization()(implicit idProvider: QuineIdProvider) {

  // Everything in Bolt assumes big-endian encoding
  implicit val byteOrder: ByteOrder = ByteOrder.BIG_ENDIAN

  // Marker bytes
  final private val NULL = 0xC0.toByte
  final private val FLOAT = 0xC1.toByte
  final private val FALSE = 0xC2.toByte
  final private val TRUE = 0xC3.toByte
  final private val INT8 = 0xC8.toByte
  final private val INT16 = 0xC9.toByte
  final private val INT32 = 0xCA.toByte
  final private val INT64 = 0xCB.toByte
  final private val STR8 = 0xD0.toByte
  final private val STR16 = 0xD1.toByte
  final private val STR32 = 0xD2.toByte
  final private val LIST8 = 0xD4.toByte
  final private val LIST16 = 0xD5.toByte
  final private val LIST32 = 0xD6.toByte
  final private val MAP8 = 0xD8.toByte
  final private val MAP16 = 0xD9.toByte
  final private val MAP32 = 0xDA.toByte
  final private val BYTE8 = 0xCC.toByte
  final private val BYTE16 = 0xCD.toByte
  final private val BYTE32 = 0xCE.toByte
  final private val STRT8 = 0xDC.toByte
  final private val STRT16 = 0xDD.toByte

  // High nibble marker bytes
  final private val TINY_STR = 0x80.toByte
  final private val TINY_LIST = 0x90.toByte
  final private val TINY_MAP = 0xA0.toByte
  final private val TINY_STRT = 0xB0.toByte

  final private val HIGH_NIBBLE = 0xF0.toByte
  final private val LOW_NIBBLE = 0x0F.toByte

  // This is all of the structured values we support in cypher
  final private val structuredValues: Map[Byte, Structured[_ <: Value]] =
    List[Structured[_ <: Value]](
      Structured.NodeStructure,
      Structured.RelationshipStructure
    ).map(s => s.signature -> s).toMap

  /** Given a de-serialization function acting on an iterator, read a value
    * entirely from a [[akka.util.ByteString]], expecting no leftover bytes.
    *
    * @param readingFunction how to extract a value from the bytes
    * @param payload the bytes
    * @return the extracted value
    */
  final def readFull[A](
    readingFunction: ByteIterator => A
  )(
    payload: ByteString
  ): A = {
    val buf = payload.iterator
    val value: A = readingFunction(buf)
    if (buf.hasNext) {
      val offset = payload.length - buf.len
      throw new IllegalArgumentException(
        s"Leftover bytes at offset $offset left over after reading $value"
      )
    }
    value
  }

  /** Given a serialization function acting on a bytestring builder, write a
    * value to a [[akka.util.ByteString]].
    *
    * @param writingFunction how to convert the value to bytes
    * @param value the values
    * @return the bytes
    */
  final def writeFull[A](
    writingFunction: (ByteStringBuilder, A) => Unit
  )(
    value: A
  ): ByteString = {
    val buf = new ByteStringBuilder()
    writingFunction(buf, value)
    buf.result()
  }

  /** De-serialize a cypher value from a byte iterator
    *
    * @param buf the byte iterator from which the bytes should be read
    */
  // format: off
  final def readValue(buf: ByteIterator): Value = buf.getByte match {
    // Null
    case NULL => Expr.Null

    // Float
    case FLOAT => Expr.Floating(buf.getDouble)

    // Boolean
    case FALSE => Expr.False
    case TRUE => Expr.True

    // Integer
    case b if (b & HIGH_NIBBLE) == 0xf0.toByte ||
              (b >= 0x00 && b <= 0x7f) => Expr.Integer(b.toLong)
    case INT8  => Expr.Integer(buf.getByte.toLong)
    case INT16 => Expr.Integer(buf.getShort.toLong)
    case INT32 => Expr.Integer(buf.getInt.toLong)
    case INT64 => Expr.Integer(buf.getLong)

    // String
    case b if (b & HIGH_NIBBLE) == TINY_STR => readString(b & LOW_NIBBLE, buf)
    case STR8  => readString(buf.getByte & 0xff, buf)
    case STR16 => readString(buf.getShort & 0xffff, buf)
    case STR32 => readString(buf.getInt, buf)

    // List
    case b if (b & HIGH_NIBBLE) == TINY_LIST => readList(b & LOW_NIBBLE, buf)
    case LIST8  => readList(buf.getByte & 0xff, buf)
    case LIST16 => readList(buf.getShort & 0xffff, buf)
    case LIST32 => readList(buf.getInt, buf)

    // Map
    case b if (b & HIGH_NIBBLE) == TINY_MAP => readMap(b & LOW_NIBBLE, buf)
    case MAP8  => readMap(buf.getByte & 0xff, buf)
    case MAP16 => readMap(buf.getShort & 0xffff, buf)
    case MAP32 => readMap(buf.getInt, buf)

    // Bytes (not part of BOLT - just doing what Neo4j does!)
    case BYTE8  => readBytes(buf.getByte & 0xff, buf)
    case BYTE16 => readBytes(buf.getShort & 0xffff, buf)
    case BYTE32 => readBytes(buf.getInt, buf)

    // Structures
    case b if (b & HIGH_NIBBLE) == TINY_STRT => readStructVal(b & LOW_NIBBLE, buf)
    case STRT8  => readStructVal(buf.getByte & 0xff, buf)
    case STRT16 => readStructVal(buf.getShort & 0xffff, buf)

    case b => throw new IllegalArgumentException(f"Unexpected byte $b%X")
  }

  /** De-serialize a structure from a byte iterator
    *
    * @param buf the byte iterator from which the bytes should be read
    */
  @inline
  final def readStructure(buf: ByteIterator): Structure = buf.getByte match {
    case b if (b & HIGH_NIBBLE) == TINY_STRT => readStructFields(b & LOW_NIBBLE, buf)
    case STRT8 => readStructFields(buf.getByte & 0xff, buf)
    case STRT16 => readStructFields(buf.getShort & 0xffff, buf)

    case b => throw new IllegalArgumentException(f"Unexpected byte $b%X")
  }

  @inline
  final private def readString(length: Int, buf: ByteIterator): Expr.Str = {
    val strBytes = buf.getBytes(length)
    Expr.Str(new String(strBytes, UTF_8))
  }

  @inline
  final private def readList(length: Int, buf: ByteIterator): Expr.List =
    Expr.List(Vector.fill(length)(readValue(buf)))

  @inline
  final private def readMap(length: Int, buf: ByteIterator): Expr.Map = {
    val map = Map.newBuilder[String, Value]
    for (_ <- 0 until length) {
      val key = readValue(buf) match {
        case Expr.Str(k) => k
        case other => throw new IllegalArgumentException(
          s"Expected a string key, but got $other"
        )
      }
      map += key -> readValue(buf)
    }
    Expr.Map(map.result())
  }

  @inline
  final private def readBytes(length: Int, buf: ByteIterator): Expr.Bytes = {
    val bytes: Array[Byte] = buf.getBytes(length)
    Expr.Bytes(bytes)
  }

  @inline
  final private def readStructFields(
    length: Int,
    buf: ByteIterator
  ): Structure = Structure(buf.getByte, List.fill(length)(readValue(buf)))

  @inline
  final private def readStructVal(
    length: Int,
    buf: ByteIterator
  ): Value = {
    val structure = readStructFields(length, buf)
    structuredValues.get(structure.signature) match {
      case Some(s) => s.fromStructure(structure)
      case None => throw new IllegalArgumentException(
        s"Unkown structure signature ${structure.signature}"
      )
    }
  }

  /** Serialize a cypher value to a byte string builder
    *
    * @param buf the builder into which the bytes should be written
    * @param value the cypher value
    */
  // format: off
  final def writeValue(buf: ByteStringBuilder, value: Value): Unit = value match {
    case Expr.Null =>             buf.putByte(NULL)

    case Expr.Floating(d) =>      buf.putByte(FLOAT).putDouble(d); ()

    case Expr.True =>             buf.putByte(TRUE)
    case Expr.False =>            buf.putByte(FALSE)

    case Expr.Integer(l) =>
      if (l >= -16L && l <= 127L) buf.putByte(l.toByte)
      else if (l.isValidByte)     buf.putByte(INT8).putByte(l.toByte)
      else if (l.isValidShort)    buf.putByte(INT16).putShort(l.toInt)
      else if (l.isValidInt)      buf.putByte(INT32).putInt(l.toInt)
      else                        buf.putByte(INT64).putLong(l.toLong)
      ()

    case Expr.Str(s) =>
      val bytes = s.getBytes(UTF_8)
      val l = bytes.length
      if (l <= 15)                buf.putByte((TINY_STR | l.toByte).toByte)
      else if (l <= 255)          buf.putByte(STR8).putByte(l.toByte)
      else if (l <= 65535)        buf.putByte(STR16).putShort(l)
      else                        buf.putByte(STR32).putInt(l)
      buf.putBytes(bytes)

    case Expr.List(x) =>
      val l = x.length
      if (l <= 15)                buf.putByte((TINY_LIST | l.toByte).toByte)
      else if (l <= 255)          buf.putByte(LIST8).putByte(l.toByte)
      else if (l <= 65535)        buf.putByte(LIST16).putShort(l)
      else                        buf.putByte(LIST32).putInt(l)
      x.foreach(writeValue(buf, _))

    case Expr.Map(m) =>
      val l = m.size
      if (l >= 0 && l <= 15)      buf.putByte((TINY_MAP | l.toByte).toByte)
      else if (l <= 255)          buf.putByte(MAP8).putByte(l.toByte)
      else if (l <= 65535)        buf.putByte(MAP16).putShort(l)
      else                        buf.putByte(MAP32).putInt(l)
      m.foreach { case (k, v) =>
        writeValue(buf, Expr.Str(k))
        writeValue(buf, v)
      }

    case Expr.Bytes(b, representsId @ _) =>
      val l = b.size
      if (l <= 255)               buf.putByte(BYTE8).putByte(l.toByte)
      else if (l <= 65535)        buf.putByte(BYTE16).putShort(l)
      else                        buf.putByte(BYTE32).putInt(l)
      buf.putBytes(b)

    case n: Expr.Node =>          writeStructure(buf, n)
    case r: Expr.Relationship =>  writeStructure(buf, r)

    // TODO: this is outright wrong - bolt has a way of serializing paths
    case p: Expr.Path =>          writeValue(buf, p.toList)

    // TODO: this is wrong, but Bolt version 1 doesn't have a way of handling these
    case Expr.DateTime(d) =>      writeValue(buf, Expr.Str(d.toString))
    case Expr.LocalDateTime(d) => writeValue(buf, Expr.Str(d.toString))
    case Expr.Duration(d) =>      writeValue(buf, Expr.Str(d.toString))
    case Expr.Date(d) =>          writeValue(buf, Expr.Str(d.toString))
    case Expr.Time(d) =>          writeValue(buf, Expr.Str(d.toString))
  }

  /** Serialize a structure to a byte string builder
    *
    * @param buf the builder into which the bytes should be written
    * @param value the structured value
    */
  // format: off
  @inline
  final def writeStructure[A](
    buf: ByteStringBuilder,
    value: A
  )(implicit
    structured: Structured[A]
  ): Unit = {
    val fields = structured.fields(value)
    val l = fields.length
    if (l >= 0 && l <= 15)     buf.putByte((TINY_STRT | l.toByte).toByte)
    else if (l <= 255)         buf.putByte(STR8).putByte(l.toByte)
    else if (l <= 65535)       buf.putByte(STR16).putShort(l)
    else                       throw new IllegalArgumentException(
      "Bolt protocol does not support structures with more than 2^16 fields"
    )
    buf.putByte(structured.signature)
    fields.foreach(writeValue(buf, _))
  }
}
