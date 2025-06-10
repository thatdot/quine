package com.thatdot.data

import java.time._
import java.time.format.DateTimeFormatter

import scala.collection.immutable.SortedMap

import io.circe.Json

import com.thatdot.common.util.ByteConversions

trait DataFolderTo[A] {
  def nullValue: A

  def trueValue: A
  def falseValue: A
  def integer(l: Long): A
  def string(s: String): A
  def bytes(b: Array[Byte]): A
  def floating(d: Double): A
  def date(d: LocalDate): A
  def time(t: OffsetTime): A
  def localTime(t: LocalTime): A
  def localDateTime(ldt: LocalDateTime): A
  def zonedDateTime(zdt: ZonedDateTime): A
  def duration(d: Duration): A
  def vectorBuilder(): DataFolderTo.CollectionBuilder[A]
  def mapBuilder(): DataFolderTo.MapBuilder[A]
}

object DataFolderTo {
  trait CollectionBuilder[A] {
    def add(a: A): Unit
    def finish(): A
  }

  trait MapBuilder[A] {
    def add(key: String, value: A): Unit
    def finish(): A
  }

  def apply[A](implicit df: DataFolderTo[A]): DataFolderTo[A] = df
  implicit val jsonFolder: DataFolderTo[Json] = new DataFolderTo[Json] {
    def nullValue: Json = Json.Null

    def trueValue: Json = Json.True

    def falseValue: Json = Json.False

    def integer(i: Long): Json = Json.fromLong(i)

    def string(s: String): Json = Json.fromString(s)

    def bytes(b: Array[Byte]): Json = Json.fromString(ByteConversions.formatHexBinary(b))

    def floating(f: Double): Json = Json.fromDoubleOrString(f)

    def date(d: LocalDate): Json = Json.fromString(d.format(DateTimeFormatter.ISO_LOCAL_DATE))

    def time(t: OffsetTime): Json = Json.fromString(t.format(DateTimeFormatter.ISO_TIME))

    def localTime(t: LocalTime): Json = Json.fromString(t.format(DateTimeFormatter.ISO_LOCAL_TIME))

    def localDateTime(ldt: LocalDateTime): Json = Json.fromString(ldt.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME))

    def zonedDateTime(zdt: ZonedDateTime): Json = Json.fromString(zdt.format(DateTimeFormatter.ISO_ZONED_DATE_TIME))

    def duration(d: Duration): Json = Json.fromString(d.toString)

    def vectorBuilder(): CollectionBuilder[Json] = new CollectionBuilder[Json] {
      private val elements = Vector.newBuilder[Json]
      def add(a: Json): Unit = elements += a

      def finish(): Json = Json.fromValues(elements.result())
    }

    def mapBuilder(): MapBuilder[Json] = new MapBuilder[Json] {
      private val fields = Seq.newBuilder[(String, Json)]
      def add(key: String, value: Json): Unit = fields += (key -> value)

      def finish(): Json = Json.fromFields(fields.result())
    }
  }

  val anyFolder: DataFolderTo[Any] = new DataFolderTo[Any] {

    override def nullValue: Any = null

    override def trueValue: Any = true

    override def falseValue: Any = false

    override def integer(l: Long): Any = l

    override def string(s: String): Any = s

    override def bytes(b: Array[Byte]): Any = b

    override def floating(d: Double): Any = d

    override def date(d: LocalDate): Any = d

    override def time(t: OffsetTime): Any = t

    override def localTime(t: LocalTime): Any = t

    override def localDateTime(ldt: LocalDateTime): Any = ldt

    override def zonedDateTime(zdt: ZonedDateTime): Any = zdt

    override def duration(d: Duration): Any = d

    override def vectorBuilder(): DataFolderTo.CollectionBuilder[Any] = new DataFolderTo.CollectionBuilder[Any] {
      private val elements = Vector.newBuilder[Any]

      def add(a: Any): Unit = elements += a

      def finish(): Any = elements.result()
    }

    def mapBuilder(): DataFolderTo.MapBuilder[Any] = new DataFolderTo.MapBuilder[Any] {
      private val kvs = SortedMap.newBuilder[String, Any]

      def add(key: String, value: Any): Unit = kvs += (key -> value)

      def finish(): Any = kvs.result()
    }
  }
}
