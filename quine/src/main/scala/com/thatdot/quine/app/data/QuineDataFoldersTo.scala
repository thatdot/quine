package com.thatdot.quine.app.data

import java.time.{Duration, LocalDate, LocalDateTime, LocalTime, OffsetTime, ZonedDateTime}

import scala.collection.immutable.SortedMap

import com.thatdot.data.DataFolderTo
import com.thatdot.quine.graph.cypher
import com.thatdot.quine.graph.cypher.{Expr => ce}

object QuineDataFoldersTo {
  implicit val cypherValueFolder: DataFolderTo[cypher.Value] = new DataFolderTo[cypher.Value] {
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

    def vectorBuilder(): DataFolderTo.CollectionBuilder[cypher.Value] =
      new DataFolderTo.CollectionBuilder[cypher.Value] {
        private val elements = Vector.newBuilder[cypher.Value]

        def add(a: cypher.Value): Unit = elements += a

        def finish(): cypher.Value = ce.List(elements.result())
      }

    def mapBuilder(): DataFolderTo.MapBuilder[cypher.Value] = new DataFolderTo.MapBuilder[cypher.Value] {
      private val kvs = SortedMap.newBuilder[String, cypher.Value]

      def add(key: String, value: cypher.Value): Unit = kvs += (key -> value)

      def finish(): cypher.Value = ce.Map(kvs.result())
    }
  }

}
