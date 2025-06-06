package com.thatdot.quine.app.model.ingest2.core

import java.time._
import java.time.format.DateTimeFormatter

import scala.collection.immutable.SortedMap

import com.thatdot.common.util.ByteConversions
import com.thatdot.quine.model.QuineValue

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
  def vectorBuilder(): CollectionBuilder[A]
  def mapBuilder(): MapBuilder[A]
}

object DataFolderTo {
  def apply[A](implicit df: DataFolderTo[A]): DataFolderTo[A] = df

  import io.circe.Json
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

  import com.thatdot.quine.graph.cypher

  implicit val cypherValueFolder: DataFolderTo[cypher.Value] = new DataFolderTo[cypher.Value] {
    import com.thatdot.quine.graph.cypher.{Expr => ce}
    def nullValue: cypher.Value = ce.Null

    def trueValue: cypher.Value = ce.True

    def falseValue: cypher.Value = ce.False

    def integer(l: Long): cypher.Value = ce.Integer(l)

    def string(s: String): cypher.Value = ce.Str(s)

    def bytes(b: Array[Byte]): cypher.Value = ce.Bytes(b, representsId = false)

    def floating(d: Double): cypher.Value = ce.Floating(d)

    def date(d: LocalDate): cypher.Value = ce.Date(d)

    def time(t: OffsetTime): cypher.Value = ce.Time(t)

    def localTime(t: LocalTime): cypher.Value = ce.LocalTime(t)

    def localDateTime(ldt: LocalDateTime): cypher.Value = ce.LocalDateTime(ldt)

    def zonedDateTime(zdt: ZonedDateTime): cypher.Value = ce.DateTime(zdt)

    def duration(d: Duration): cypher.Value = ce.Duration(d)

    def vectorBuilder(): CollectionBuilder[cypher.Value] = new CollectionBuilder[cypher.Value] {
      private val elements = Vector.newBuilder[cypher.Value]
      def add(a: cypher.Value): Unit = elements += a

      def finish(): cypher.Value = ce.List(elements.result())
    }

    def mapBuilder(): MapBuilder[cypher.Value] = new MapBuilder[cypher.Value] {
      private val kvs = SortedMap.newBuilder[String, cypher.Value]
      def add(key: String, value: cypher.Value): Unit = kvs += (key -> value)

      def finish(): cypher.Value = ce.Map(kvs.result())
    }
  }

  implicit val quineValueFolder: DataFolderTo[QuineValue] = new DataFolderTo[QuineValue] {
    private val QV = QuineValue

    override def nullValue: QuineValue = QV.Null

    override def trueValue: QuineValue = QV.True

    override def falseValue: QuineValue = QV.False

    override def integer(l: Long): QuineValue = QV.Integer(l)

    override def string(s: String): QuineValue = QV.Str(s)

    override def bytes(b: Array[Byte]): QuineValue = QV.Bytes(b)

    override def floating(d: Double): QuineValue = QV.Floating(d)

    override def date(d: LocalDate): QuineValue = QV.Date(d)

    override def time(t: OffsetTime): QuineValue = QV.Time(t)

    override def localTime(t: LocalTime): QuineValue = QV.LocalTime(t)

    override def localDateTime(ldt: LocalDateTime): QuineValue = QV.LocalDateTime(ldt)

    override def zonedDateTime(zdt: ZonedDateTime): QuineValue = QV.DateTime(zdt.toOffsetDateTime)

    override def duration(d: Duration): QuineValue = QV.Duration(d)

    override def vectorBuilder(): CollectionBuilder[QuineValue] = new CollectionBuilder[QuineValue] {
      private val builder = Vector.newBuilder[QuineValue]
      override def add(a: QuineValue): Unit = builder.addOne(a)

      override def finish(): QuineValue = QuineValue.List(builder.result())
    }

    override def mapBuilder(): MapBuilder[QuineValue] = new MapBuilder[QuineValue] {
      private val builder = Map.newBuilder[String, QuineValue]
      override def add(key: String, value: QuineValue): Unit = builder.addOne((key, value))

      override def finish(): QuineValue = QV.Map(builder.result())
    }
  }
}

trait CollectionBuilder[A] {
  def add(a: A): Unit
  def finish(): A
}

trait MapBuilder[A] {
  def add(key: String, value: A): Unit
  def finish(): A
}
