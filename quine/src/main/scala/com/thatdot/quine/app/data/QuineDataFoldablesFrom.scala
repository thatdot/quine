package com.thatdot.quine.app.data

import com.thatdot.data.{DataFoldableFrom, DataFolderTo}
import com.thatdot.quine.graph.cypher
import com.thatdot.quine.graph.cypher.Expr

object QuineDataFoldablesFrom {
  implicit val cypherValueDataFoldable: DataFoldableFrom[cypher.Value] = new DataFoldableFrom[cypher.Value] {
    def fold[B](value: cypher.Value, folder: DataFolderTo[B]): B = value match {
      case Expr.Null => folder.nullValue
      case number: Expr.Number =>
        number match {
          case Expr.Integer(long) => folder.integer(long)
          case Expr.Floating(double) => folder.floating(double)
          case Expr.Null => folder.nullValue
        }
      case bool: Expr.Bool =>
        bool match {
          case Expr.True => folder.trueValue
          case Expr.False => folder.falseValue
          case Expr.Null => folder.nullValue
        }
      case value: Expr.PropertyValue =>
        value match {
          case Expr.Str(string) => folder.string(string)
          case Expr.Integer(long) => folder.integer(long)
          case Expr.Floating(double) => folder.floating(double)
          case Expr.True => folder.trueValue
          case Expr.False => folder.falseValue
          case Expr.Bytes(b, _) => folder.bytes(b)
          case Expr.List(list) =>
            val builder = folder.vectorBuilder()
            list.foreach(v => builder.add(fold(v, folder)))
            builder.finish()
          case Expr.Map(map) =>
            val builder = folder.mapBuilder()
            map.foreach { case (k, v) =>
              builder.add(k, fold(v, folder))
            }
            builder.finish()
          case Expr.LocalDateTime(localDateTime) => folder.localDateTime(localDateTime)
          case Expr.Date(date) => folder.date(date)
          case Expr.Time(offsetTime) => folder.time(offsetTime)
          case Expr.LocalTime(localTime) => folder.localTime(localTime)
          case Expr.DateTime(zonedDateTime) => folder.zonedDateTime(zonedDateTime)
          case Expr.Duration(duration) => folder.duration(duration)
        }
      case other @ (Expr.Node(_, _, _) | Expr.Relationship(_, _, _, _) | Expr.Path(_, _)) =>
        throw new Exception(s"Fold conversion not supported for $other")
    }
  }

}
