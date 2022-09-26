package com.thatdot.quine.model

import java.time.Instant

import scala.collection.compat._
import scala.collection.immutable.{Map => ScalaMap, SortedMap}
import scala.util.hashing.MurmurHash3

import org.msgpack.core.MessagePack.Code.EXT_TIMESTAMP
import org.msgpack.core.{MessageFormat, MessagePack, MessagePacker, MessageUnpacker}
import org.msgpack.value.ValueType

/** Values that are recognized by the Quine interpreter. When talking about Quine
  * as a graph interpreter, these are a part of the "values" handled by this
  * interpreter.
  */
sealed abstract class QuineValue {

  /** Underlying JVM type */
  type JvmType <: Any

  def quineType: QuineType

  def underlyingJvmValue: JvmType
}
object QuineValue {
  def apply(v: Str#JvmType): QuineValue = Str(v)
  def apply(v: Integer#JvmType): QuineValue = Integer(v)
  def apply(v: Int): QuineValue = Integer(v.toLong)
  def apply(v: Floating#JvmType): QuineValue = Floating(v)
  def apply(v: Float): QuineValue = Floating(v.toDouble)
  def apply(v: True.JvmType): QuineValue = if (v) True else False
  def apply(v: Null.JvmType): QuineValue = Null
  def apply(v: Bytes#JvmType): QuineValue = Bytes(v)
  def apply(v: Vector[QuineValue]): QuineValue = List(v)
  def apply(v: scala.collection.immutable.List[QuineValue]): QuineValue = List(v.toVector)
  def apply(v: ScalaMap[String, QuineValue]): QuineValue = Map(v)
  def apply[CustomIdType](v: CustomIdType)(implicit
    idProvider: QuineIdProvider.Aux[CustomIdType]
  ): QuineValue = Id(v)

  final case class Str(string: String) extends QuineValue {
    type JvmType = String

    def quineType = QuineType.Str
    def underlyingJvmValue = string
  }

  final case class Integer private (long: Long) extends QuineValue {
    type JvmType = Long

    def quineType = QuineType.Integer
    def underlyingJvmValue = long
  }
  object Integer {

    /* Cache of small integers from -128 to 127 inclusive, to share references
     * whenever possible (less allocations + faster comparisons)
     */
    private val integerCacheMin = -128L
    private val integerCacheMax = 127L
    private val integerCache: Array[Integer] =
      Array.tabulate((integerCacheMax - integerCacheMin + 1).toInt) { (i: Int) =>
        new Integer(i.toLong + integerCacheMin)
      }

    def apply(long: Long): Integer =
      if (long >= integerCacheMin && long <= integerCacheMax) {
        integerCache((long - integerCacheMin).toInt)
      } else {
        new Integer(long)
      }
  }

  final case class Floating(double: Double) extends QuineValue {
    type JvmType = Double

    def quineType = QuineType.Floating
    def underlyingJvmValue = double
  }

  case object True extends QuineValue {
    type JvmType = Boolean

    def quineType = QuineType.Boolean
    def underlyingJvmValue = true
  }

  case object False extends QuineValue {
    type JvmType = Boolean

    def quineType = QuineType.Boolean
    def underlyingJvmValue = false
  }

  case object Null extends QuineValue {
    type JvmType = Unit

    def quineType = QuineType.Null
    def underlyingJvmValue = ()
  }

  final case class Bytes(bytes: Array[Byte]) extends QuineValue {
    type JvmType = Array[Byte]

    override def hashCode: Int = MurmurHash3.bytesHash(bytes, 0x12345)
    override def equals(other: Any): Boolean =
      other match {
        case Bytes(bytesOther) => bytes.toSeq == bytesOther.toSeq
        case _ => false
      }

    def quineType = QuineType.Bytes
    def underlyingJvmValue = bytes
  }

  final case class List(list: Vector[QuineValue]) extends QuineValue {
    type JvmType = Vector[Any]

    def quineType = QuineType.List
    def underlyingJvmValue: JvmType = list.map(_.underlyingJvmValue)
  }

  final case class Map private (map: SortedMap[String, QuineValue]) extends QuineValue {
    type JvmType = SortedMap[String, Any]

    def quineType = QuineType.Map
    def underlyingJvmValue: SortedMap[String, Any] = SortedMap.from(map.view.mapValues(_.underlyingJvmValue))
  }
  object Map {
    def apply(entries: IterableOnce[(String, QuineValue)]): Map = new Map(SortedMap.from(entries))
  }

  final case class DateTime(timestamp: Instant) extends QuineValue {
    type JvmType = Instant

    def quineType = QuineType.DateTime
    def underlyingJvmValue = timestamp
  }

  object Id {
    def apply[CustomIdType](id: CustomIdType)(implicit
      idProvider: QuineIdProvider.Aux[CustomIdType]
    ): QuineValue.Id = Id(idProvider.customIdToQid(id))
  }
  final case class Id(id: QuineId) extends QuineValue {
    type JvmType = QuineId

    def quineType: QuineType = QuineType.Id
    def underlyingJvmValue: JvmType = id
  }

  /** Attempt to decoded a Quine value from a JSON-encoded value
    *
    * The right inverse of [[fromJson]] is [[toJson]], meaning that
    *
    * {{{
    * val roundtripped = fromJson(_).compose(toJson(_))
    * forAll { (json: ujson.Value) =>
    *   roundtripped(json) == json
    * }
    * }}}
    *
    * @see [[com.thatdot.quine.graph.cypher.Value.fromJson]]
    * @param jsonV json value to decode
    * @return decoded Quine value
    */
  def fromJson(jsonV: ujson.Value): QuineValue = jsonV match {
    case ujson.Null => QuineValue.Null
    case ujson.Str(s) => QuineValue.Str(s)
    case ujson.False => QuineValue.False
    case ujson.True => QuineValue.True

    // Numbers are entirely ambiguous. We go with Longs when whole, Doubles otherwise. Works when you squint.
    case ujson.Num(x) => if (x.isWhole) QuineValue.Integer(x.toLong) else QuineValue.Floating(x)

    case ujson.Arr(jvs) => QuineValue.List(jvs.view.map(fromJson).toVector)
    case ujson.Obj(jkvs) => QuineValue.Map(jkvs.view.mapValues(fromJson).toMap)
  }

  /** Encode a Quine value into JSON
    *
    * @see [[com.thatdot.quine.graph.cypher.Value.toJson]]
    * @param value Quine value to encode
    * @param idProvider ID provider used to try to serialize IDs nicely
    * @return encoded JSON value
    */
  def toJson(value: QuineValue)(implicit idProvider: QuineIdProvider): ujson.Value = value match {
    case QuineValue.Null => ujson.Null
    case QuineValue.Str(str) => ujson.Str(str)
    case QuineValue.False => ujson.False
    case QuineValue.True => ujson.True
    case QuineValue.Integer(lng) => ujson.Num(lng.toDouble)
    case QuineValue.Floating(dbl) => ujson.Num(dbl)
    case QuineValue.List(vs) => ujson.Arr.from(vs.view.map(toJson))
    case QuineValue.Map(kvs) => ujson.Obj.from(kvs.view.mapValues(toJson))
    case QuineValue.Bytes(byteArray) => ujson.Arr.from(byteArray)
    case QuineValue.DateTime(instant) => ujson.Str(instant.toString)
    case QuineValue.Id(qid) => ujson.Str(qid.pretty)
  }

  // Message pack extension tags
  final val IdExt: Byte = 32

  /** Read just the type of a [[QuineValue]] from a MessagePack payload
    *
    * @note up to exceptions, this is equivalent to `readMsgPack andThen quineType`
    * @param unpacker source of data
    * @return type of serialized value
    */
  def readMsgPackType(unpacker: MessageUnpacker): QuineType = {
    val format = unpacker.getNextFormat
    val typ = format.getValueType
    typ match {
      case ValueType.NIL => QuineType.Null
      case ValueType.BOOLEAN => QuineType.Boolean
      case ValueType.INTEGER => QuineType.Integer
      case ValueType.FLOAT => QuineType.Floating
      case ValueType.STRING => QuineType.Str
      case ValueType.BINARY => QuineType.Bytes
      case ValueType.ARRAY => QuineType.List
      case ValueType.MAP => QuineType.Map
      case ValueType.EXTENSION =>
        val extHeader = unpacker.unpackExtensionTypeHeader()
        extHeader.getType match {
          case IdExt => QuineType.Id
          case EXT_TIMESTAMP => QuineType.DateTime
          case other =>
            throw new IllegalArgumentException(s"Unsupported data extension $other")
        }
    }
  }

  /** Read a [[QuineValue]] from a MessagePack payload
    *
    * @param unpacker source of data
    * @return deserialized value
    */
  def readMsgPack(unpacker: MessageUnpacker): QuineValue = {
    val format = unpacker.getNextFormat()
    val typ = format.getValueType()
    typ match {
      case ValueType.NIL =>
        unpacker.unpackNil()
        QuineValue.Null

      case ValueType.BOOLEAN =>
        if (unpacker.unpackBoolean()) QuineValue.True else QuineValue.False

      case ValueType.INTEGER =>
        if (format.getValueType == MessageFormat.UINT64)
          throw new IllegalArgumentException("Unsigned 64-bit numbers are unsupported")
        QuineValue.Integer(unpacker.unpackLong())

      case ValueType.FLOAT =>
        QuineValue.Floating(unpacker.unpackDouble())

      case ValueType.STRING =>
        QuineValue.Str(unpacker.unpackString())

      case ValueType.BINARY =>
        val data = new Array[Byte](unpacker.unpackBinaryHeader())
        unpacker.readPayload(data)
        QuineValue.Bytes(data)

      case ValueType.ARRAY =>
        var len = unpacker.unpackArrayHeader()
        val builder = Vector.newBuilder[QuineValue]
        while (len > 0) {
          builder += readMsgPack(unpacker)
          len -= 1
        }
        QuineValue.List(builder.result())

      case ValueType.MAP =>
        var len = unpacker.unpackMapHeader()
        val builder = ScalaMap.newBuilder[String, QuineValue]
        while (len > 0) {
          builder += unpacker.unpackString() -> readMsgPack(unpacker)
          len -= 1
        }
        QuineValue.Map(builder.result())

      case ValueType.EXTENSION =>
        val extHeader = unpacker.unpackExtensionTypeHeader()
        extHeader.getType match {
          case EXT_TIMESTAMP =>
            QuineValue.DateTime(unpacker.unpackTimestamp(extHeader))

          case IdExt =>
            val extData = unpacker.readPayload(extHeader.getLength)
            QuineValue.Id(QuineId(extData))

          case other =>
            throw new IllegalArgumentException(s"Unsupported msgpack data extension $other")
        }
    }
  }

  /** Write a [[QuineValue]] into a MessagePack payload
    *
    * @param packer sink of data
    * @param quineValue value to write
    */
  def writeMsgPack(packer: MessagePacker, quineValue: QuineValue): Unit = {
    quineValue match {
      case QuineValue.Null =>
        packer.packNil()

      case QuineValue.True =>
        packer.packBoolean(true)

      case QuineValue.False =>
        packer.packBoolean(false)

      case QuineValue.Integer(lng) =>
        packer.packLong(lng)

      case QuineValue.Floating(dbl) =>
        packer.packDouble(dbl)

      case QuineValue.Str(str) =>
        packer.packString(str)

      case QuineValue.Bytes(bytes) =>
        packer.packBinaryHeader(bytes.length).addPayload(bytes)

      case QuineValue.List(elems) =>
        packer.packArrayHeader(elems.length)
        val iterator = elems.iterator
        while (iterator.hasNext) writeMsgPack(packer, iterator.next())

      case QuineValue.Map(elems) =>
        packer.packMapHeader(elems.size)
        val iterator = elems.iterator
        while (iterator.hasNext) {
          val (k, v) = iterator.next()
          writeMsgPack(packer.packString(k), v)
        }

      case QuineValue.DateTime(timestamp) =>
        packer.packTimestamp(timestamp)

      case QuineValue.Id(qid) =>
        val data = qid.array
        packer.packExtensionTypeHeader(IdExt, data.length).addPayload(data)
    }
    ()
  }

  def readMsgPackType(packed: Array[Byte]): QuineType =
    readMsgPackType(MessagePack.newDefaultUnpacker(packed))

  def readMsgPack(packed: Array[Byte]): QuineValue =
    readMsgPack(MessagePack.newDefaultUnpacker(packed))

  def writeMsgPack(quineValue: QuineValue): Array[Byte] = {
    val packer = MessagePack.newDefaultBufferPacker()
    writeMsgPack(packer, quineValue)
    packer.toByteArray()
  }
}

/** Types of [[QuineValue]], used for runtime type-mismatched exceptions */
sealed abstract class QuineType
object QuineType {
  case object Str extends QuineType
  case object Integer extends QuineType
  case object Floating extends QuineType
  case object Boolean extends QuineType
  case object Null extends QuineType
  case object Bytes extends QuineType
  case object List extends QuineType
  case object Map extends QuineType
  case object DateTime extends QuineType
  case object Id extends QuineType
}

final case class QuineValueTypeOperationMismatch(message: String) extends RuntimeException(message)
