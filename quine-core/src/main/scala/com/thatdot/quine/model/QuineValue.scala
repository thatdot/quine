package com.thatdot.quine.model

import java.time.{
  Duration => JavaDuration,
  LocalDate,
  LocalDateTime => JavaLocalDateTime,
  OffsetDateTime,
  OffsetTime,
  ZoneOffset
}

import scala.collection.immutable.{Map => ScalaMap, SortedMap}
import scala.util.hashing.MurmurHash3

import cats.implicits._
import io.circe.Json
import org.msgpack.core.MessagePack.Code.EXT_TIMESTAMP
import org.msgpack.core.{ExtensionTypeHeader, MessageFormat, MessagePack, MessagePacker, MessageUnpacker}
import org.msgpack.value.ValueType

import com.thatdot.quine.util.Log._

/** Values that are recognized by the Quine interpreter. When talking about Quine
  * as a graph interpreter, these are a part of the "values" handled by this
  * interpreter.
  */
sealed abstract class QuineValue {

  /** Underlying JVM type */
  type JvmType <: Any

  def quineType: QuineType

  def underlyingJvmValue: JvmType

  /** Return a presentable string representation */
  def pretty(implicit idProvider: QuineIdProvider, logConfig: LogConfig): String
}
object QuineValue {
  def apply(v: Str#JvmType): QuineValue = Str(v)
  def apply(v: Integer#JvmType): QuineValue = Integer(v)
  def apply(v: Int): QuineValue = Integer(v.toLong)
  def apply(v: Floating#JvmType): QuineValue = Floating(v)
  def apply(v: Float): QuineValue = Floating(v.toDouble)
  def apply(v: True.JvmType): QuineValue = fromBoolean(v)
  def apply(v: Null.JvmType): QuineValue = Null
  def apply(v: Bytes#JvmType): QuineValue = Bytes(v)
  def apply(v: Vector[QuineValue]): QuineValue = List(v)
  def apply(v: scala.collection.immutable.List[QuineValue]): QuineValue = List(v.toVector)
  def apply(v: ScalaMap[String, QuineValue]): QuineValue = Map(v)
  def apply[CustomIdType](v: CustomIdType)(implicit
    idProvider: QuineIdProvider.Aux[CustomIdType]
  ): QuineValue = Id(v)

  def fromBoolean(b: Boolean): QuineValue = if (b) True else False
  final case class Str(string: String) extends QuineValue {
    type JvmType = String

    def quineType = QuineType.Str
    def underlyingJvmValue = string

    def pretty(implicit idProvider: QuineIdProvider, logConfig: LogConfig): String = string
  }

  final case class Integer private (long: Long) extends QuineValue {
    type JvmType = Long

    def quineType = QuineType.Integer
    def underlyingJvmValue = long

    def pretty(implicit idProvider: QuineIdProvider, logConfig: LogConfig): String = long.toString
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

    def pretty(implicit idProvider: QuineIdProvider, logConfig: LogConfig): String = double.toString
  }

  case object True extends QuineValue {
    type JvmType = Boolean

    def quineType = QuineType.Boolean
    def underlyingJvmValue = true

    def pretty(implicit idProvider: QuineIdProvider, logConfig: LogConfig): String = "true"
  }

  case object False extends QuineValue {
    type JvmType = Boolean

    def quineType = QuineType.Boolean
    def underlyingJvmValue = false

    def pretty(implicit idProvider: QuineIdProvider, logConfig: LogConfig): String = "false"
  }

  case object Null extends QuineValue {
    type JvmType = Unit

    def quineType = QuineType.Null
    def underlyingJvmValue = ()

    def pretty(implicit idProvider: QuineIdProvider, logConfig: LogConfig): String = "null"
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

    def pretty(implicit idProvider: QuineIdProvider, logConfig: LogConfig): String = bytes.mkString("<", ",", ">")
  }

  final case class List(list: Vector[QuineValue]) extends QuineValue {
    type JvmType = Vector[Any]

    def quineType = QuineType.List
    def underlyingJvmValue: JvmType = list.map(_.underlyingJvmValue)

    def pretty(implicit idProvider: QuineIdProvider, logConfig: LogConfig): String =
      list.map(_.pretty).mkString("[", ",", "]")
  }

  final case class Map private (map: SortedMap[String, QuineValue]) extends QuineValue {
    type JvmType = SortedMap[String, Any]

    def quineType = QuineType.Map
    def underlyingJvmValue: SortedMap[String, Any] = SortedMap.from(map.view.mapValues(_.underlyingJvmValue))

    def pretty(implicit idProvider: QuineIdProvider, logConfig: LogConfig): String =
      map.map { case (k, v) => s"$k : ${v.pretty}" }.mkString("{", ",", "}")
  }
  object Map {
    def apply(entries: IterableOnce[(String, QuineValue)]): Map = new Map(SortedMap.from(entries))
  }

  /** @param instant A java.time.Instant models a single instantaneous point on the time-line.
    */
  final case class DateTime(instant: OffsetDateTime) extends QuineValue {
    type JvmType = OffsetDateTime

    def quineType = QuineType.DateTime
    def underlyingJvmValue = instant

    def pretty(implicit idProvider: QuineIdProvider, logConfig: LogConfig): String = instant.toString
  }

  /** @param duration  A java.time.Duration models a quantity or amount of time in terms of seconds and nanoseconds.
    */
  final case class Duration(duration: JavaDuration) extends QuineValue {

    type JvmType = JavaDuration

    def quineType = QuineType.Duration

    def underlyingJvmValue = duration

    def pretty(implicit idProvider: QuineIdProvider, logConfig: LogConfig): String = duration.toString
  }

  /** @param date A date without a time-zone in the ISO-8601 calendar system, such as 2007-12-03.
    */
  final case class Date(date: LocalDate) extends QuineValue {

    type JvmType = LocalDate

    def quineType = QuineType.Date

    def underlyingJvmValue = date

    def pretty(implicit idProvider: QuineIdProvider, logConfig: LogConfig): String = date.toString
  }

  /** @param time A time without a time-zone in the ISO-8601 calendar system, such as 10:15:30.
    */
  final case class LocalTime(time: java.time.LocalTime) extends QuineValue {

    type JvmType = java.time.LocalTime

    def quineType = QuineType.LocalTime

    def underlyingJvmValue = time

    def pretty(implicit idProvider: QuineIdProvider, logConfig: LogConfig): String = time.toString
  }

  final case class Time(time: OffsetTime) extends QuineValue {

    type JvmType = OffsetTime

    def quineType = QuineType.Time

    def underlyingJvmValue = time

    def pretty(implicit idProvider: QuineIdProvider, logConfig: LogConfig): String = time.toString
  }

  /** @param localDateTime A date-time without a time-zone in the ISO-8601 calendar system, such as 2007-12-03T10:15:30.
    */
  final case class LocalDateTime(localDateTime: JavaLocalDateTime) extends QuineValue {

    type JvmType = JavaLocalDateTime

    def quineType = QuineType.LocalDateTime

    def underlyingJvmValue = localDateTime

    def pretty(implicit idProvider: QuineIdProvider, logConfig: LogConfig): String = localDateTime.toString
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

    def pretty(implicit idProvider: QuineIdProvider, logConfig: LogConfig): String = idProvider.qidToPrettyString(id)
  }

  /** Attempt to decoded a Quine value from a JSON-encoded value
    *
    * The right inverse of [[fromJson]] is [[toJson]], meaning that
    *
    * {{{
    * val roundtripped = fromJson(_).compose(toJson(_))
    * forAll { (json: Json) =>
    *   roundtripped(json) == json
    * }
    * }}}
    *
    * @see [[com.thatdot.quine.graph.cypher.Value.fromJson]]
    * @param json json value to decode
    * @return decoded Quine value
    */
  def fromJson(json: Json): QuineValue = json.fold(
    QuineValue.Null,
    QuineValue.fromBoolean,
    num => num.toLong.fold[QuineValue](QuineValue.Floating(num.toDouble))(QuineValue.Integer(_)),
    QuineValue.Str,
    jsonVals => QuineValue.List(jsonVals map fromJson),
    jsonObj => QuineValue.Map(jsonObj.toMap.fmap(fromJson))
  )

  /** Encode a Quine value into JSON
    *
    * @see [[com.thatdot.quine.graph.cypher.Value.toJson]]
    * @param value Quine value to encode
    * @param idProvider ID provider used to try to serialize IDs nicely
    * @return encoded JSON value
    */
  def toJson(value: QuineValue)(implicit idProvider: QuineIdProvider, logConfig: LogConfig): Json = value match {
    case QuineValue.Null => Json.Null
    case QuineValue.Str(str) => Json.fromString(str)
    case QuineValue.False => Json.False
    case QuineValue.True => Json.True
    case QuineValue.Integer(lng) => Json.fromLong(lng)
    case QuineValue.Floating(dbl) => Json.fromDoubleOrString(dbl)
    case QuineValue.List(vs) => Json.fromValues(vs.map(toJson))
    case QuineValue.Map(kvs) => Json.fromFields(kvs.view.mapValues(toJson).toSeq)
    case QuineValue.Bytes(byteArray) => Json.fromValues(byteArray.map(b => Json.fromInt(b.intValue())))
    case QuineValue.DateTime(instant) => Json.fromString(instant.toString)
    case QuineValue.Date(d) => Json.fromString(d.toString)
    case QuineValue.Time(t) => Json.fromString(t.toString)
    case QuineValue.LocalTime(t) => Json.fromString(t.toString)
    case QuineValue.LocalDateTime(dt) => Json.fromString(dt.toString)
    case QuineValue.Duration(d) => Json.fromString(d.toString) //TODO Better String representation?
    case QuineValue.Id(qid) => Json.fromString(qid.pretty)
  }

  // Message pack extension tags
  final val IdExt: Byte = 32
  final val DurationExt: Byte = 33
  final val DateExt: Byte = 34
  final val LocalTimeExt: Byte = 35
  final val LocalDateTimeExt: Byte = 36
  final val DateTimeExt: Byte = 37
  final val TimeExt: Byte = 38

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
          case DurationExt => QuineType.Duration
          case DateExt => QuineType.Date
          case TimeExt => QuineType.Time
          case LocalTimeExt => QuineType.LocalTime
          case LocalDateTimeExt => QuineType.LocalDateTime
          case DateTimeExt => QuineType.DateTime
          case EXT_TIMESTAMP => QuineType.DateTime
          case other =>
            throw new IllegalArgumentException(s"Unsupported data extension $other")
        }
    }
  }

  // The size of bytes of various combinations of things we're putting in msgpack extensions:
  private val IntByteSize = java.lang.Integer.BYTES // 4
  private val LongByteSize = java.lang.Long.BYTES // 8
  private val ByteByteSize = java.lang.Byte.BYTES // 8
  private val LongAndByteByteSize = LongByteSize + ByteByteSize
  private val LongAndIntByteSize = LongByteSize + IntByteSize
  private val LongIntAndByteByteSize = LongAndIntByteSize + ByteByteSize

  val NanosPerSecond = 1000000000L
  val NanosPerDay: Long = NanosPerSecond * 60 * 60 * 24

  // The latest date representable as a 64-bit number of nanos since epoch
  // We use the start of the day as the cutoff, because could overflow the long at  23:47:16.854775296 on that final day
  private val Largest64BitNanoDate = LocalDate.ofEpochDay(Long.MaxValue / NanosPerDay).toEpochDay

  // All current offsets align with the hour, half-hour, or 15-minutes (e.g. +05:45)
  // So we store number of 15-minute increments between -12:00 and +14:00
  // https://en.wikipedia.org/wiki/List_of_UTC_offsets
  def offsetFromByte(byte: Byte): ZoneOffset = ZoneOffset.ofTotalSeconds(byte * 15 * 60)
  def offsetToByte(offset: ZoneOffset): Byte = (offset.getTotalSeconds / 60 / 15).toByte

  /** Read a [[QuineValue]] from a MessagePack payload
    *
    * @param unpacker source of data
    * @return deserialized value
    */
  def readMsgPack(unpacker: MessageUnpacker): QuineValue = {

    def validateExtHeaderLength(extHeader: ExtensionTypeHeader, expectedLength: Int) = if (
      extHeader.getLength != expectedLength
    )
      throw new InvalidHeaderLengthException(expectedLength.toString, extHeader.getLength)

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
          case DurationExt =>
            validateExtHeaderLength(extHeader, LongAndIntByteSize)
            val seconds = unpacker.unpackLong()
            val nanos = unpacker.unpackInt()
            QuineValue.Duration(JavaDuration.ofSeconds(seconds, nanos.toLong))

          case DateExt =>
            validateExtHeaderLength(extHeader, IntByteSize)
            val epochDay = unpacker.unpackInt()
            QuineValue.Date(LocalDate.ofEpochDay(epochDay.toLong))

          case TimeExt =>
            validateExtHeaderLength(extHeader, LongAndByteByteSize)
            val nanoDay = unpacker.unpackLong()
            val offset = unpacker.unpackByte()
            QuineValue.Time(OffsetTime.of(java.time.LocalTime.ofNanoOfDay(nanoDay), offsetFromByte(offset)))

          case LocalTimeExt =>
            validateExtHeaderLength(extHeader, LongByteSize)
            val nanoDay = unpacker.unpackLong()
            QuineValue.LocalTime(java.time.LocalTime.ofNanoOfDay(nanoDay))

          case LocalDateTimeExt =>
            validateExtHeaderLength(extHeader, LongAndIntByteSize)
            val epochDay = unpacker.unpackInt()
            val nanoDay = unpacker.unpackLong()
            QuineValue.LocalDateTime(
              JavaLocalDateTime.of(LocalDate.ofEpochDay(epochDay.toLong), java.time.LocalTime.ofNanoOfDay(nanoDay))
            )

          case DateTimeExt =>
            extHeader.getLength match {
              case LongAndByteByteSize =>
                import scala.math.Integral.Implicits._
                val epochNanos = unpacker.unpackLong()
                val offset = offsetFromByte(unpacker.unpackByte())
                val (epochDays, nanoOfDay) = epochNanos /% NanosPerDay
                // epoch days can be negative, but nano of day must be positive
                // e.g. - 7 days - 2 hours before the epoch corresponds to
                //      -8 days + 22 hours
                val dateTime =
                  if (nanoOfDay < 0)
                    OffsetDateTime.of(
                      LocalDate.ofEpochDay(epochDays - 1),
                      java.time.LocalTime.ofNanoOfDay(NanosPerDay + nanoOfDay),
                      offset
                    )
                  else
                    OffsetDateTime.of(
                      LocalDate.ofEpochDay(epochDays),
                      java.time.LocalTime.ofNanoOfDay(nanoOfDay),
                      offset
                    )
                QuineValue.DateTime(dateTime)

              case LongIntAndByteByteSize =>
                val epochDay = unpacker.unpackInt()
                val nanoOfDay = unpacker.unpackLong()
                val offset = offsetFromByte(unpacker.unpackByte())
                val dateTime =
                  OffsetDateTime.of(
                    LocalDate.ofEpochDay(epochDay.toLong),
                    java.time.LocalTime.ofNanoOfDay(nanoOfDay),
                    offset
                  )
                QuineValue.DateTime(dateTime)

              case other =>
                throw new InvalidHeaderLengthException(s"one of $LongAndByteByteSize, $LongIntAndByteByteSize", other)
            }

          // For reading legacy data. We no longer write timestamps w/out offsets.
          case EXT_TIMESTAMP =>
            QuineValue.DateTime(unpacker.unpackTimestamp(extHeader).atOffset(ZoneOffset.UTC))

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
        val localDate = timestamp.toLocalDate.toEpochDay
        val localTime = timestamp.toLocalTime.toNanoOfDay
        val offset = timestamp.getOffset
        if (Math.abs(localDate) < Largest64BitNanoDate) {
          val epochNanos = localDate * NanosPerDay + localTime
          packer
            .packExtensionTypeHeader(DateTimeExt, LongAndByteByteSize)
            .packLong(epochNanos)
            .packByte(offsetToByte(offset))
        } else {
          // It doesn't fit in a single long, so we'll write a int for date
          // and a long for time (nanos of date)
          packer
            .packExtensionTypeHeader(DateTimeExt, LongIntAndByteByteSize)
            .packInt(localDate.intValue)
            .packLong(localTime)
            .packByte(offsetToByte(offset))
        }

      case QuineValue.Duration(duration) =>
        packer
          .packExtensionTypeHeader(DurationExt, LongAndIntByteSize)
          .packLong(duration.getSeconds)
          .packInt(duration.getNano)

      case QuineValue.Date(date) =>
        packer.packExtensionTypeHeader(DateExt, IntByteSize).packInt(date.toEpochDay.intValue)
      case QuineValue.Time(time) =>
        packer
          .packExtensionTypeHeader(TimeExt, LongAndByteByteSize)
          .packLong(time.toLocalTime.toNanoOfDay)
          .packByte(offsetToByte(time.getOffset))
      case QuineValue.LocalTime(time) =>
        packer.packExtensionTypeHeader(LocalTimeExt, LongByteSize).packLong(time.toNanoOfDay)
      case QuineValue.LocalDateTime(localDateTime) =>
        packer
          .packExtensionTypeHeader(LocalDateTimeExt, LongAndIntByteSize)
          .packInt(localDateTime.toLocalDate.toEpochDay.intValue)
          .packLong(localDateTime.toLocalTime.toNanoOfDay)

      case QuineValue.Id(qid) =>
        val data = qid.array
        packer.packExtensionTypeHeader(IdExt, data.length).addPayload(data)
    }
    () // Just to get rid of the "discarded non-Unit value" warning from all of the above
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
class InvalidHeaderLengthException(expected: String, actual: Int)
    extends IllegalArgumentException(
      s"Invalid length for date time (expected $expected but got $actual)"
    )

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
  case object Duration extends QuineType
  case object Date extends QuineType
  case object Time extends QuineType
  case object LocalTime extends QuineType
  case object LocalDateTime extends QuineType
  case object Id extends QuineType
}

final case class QuineValueTypeOperationMismatch(message: String) extends RuntimeException(message)
