package com.thatdot.quine.serialization.data

import java.time.{Duration, LocalDate, LocalDateTime, LocalTime, OffsetTime, ZonedDateTime}

import com.thatdot.data.DataFolderTo
import com.thatdot.quine.model.QuineValue

object QuineSerializationFoldersTo {
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

    override def vectorBuilder(): DataFolderTo.CollectionBuilder[QuineValue] =
      new DataFolderTo.CollectionBuilder[QuineValue] {
        private val builder = Vector.newBuilder[QuineValue]

        override def add(a: QuineValue): Unit = builder.addOne(a)

        override def finish(): QuineValue = QuineValue.List(builder.result())
      }

    override def mapBuilder(): DataFolderTo.MapBuilder[QuineValue] = new DataFolderTo.MapBuilder[QuineValue] {
      private val builder = Map.newBuilder[String, QuineValue]

      override def add(key: String, value: QuineValue): Unit = builder.addOne((key, value))

      override def finish(): QuineValue = QV.Map(builder.result())
    }
  }
}
