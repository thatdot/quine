package com.thatdot.quine.app.ingest2.core

import io.circe.{Json, JsonNumber, JsonObject}

import com.thatdot.quine.graph.cypher.Expr

trait DataFoldableFrom[A] {
  def fold[B](value: A, folder: DataFolderTo[B]): B
}

object DataFoldableFrom {
  def apply[A](implicit df: DataFoldableFrom[A]): DataFoldableFrom[A] = df

  implicit val jsonDataFoldable: DataFoldableFrom[Json] = new DataFoldableFrom[Json] {
    def fold[B](value: Json, folder: DataFolderTo[B]): B =
      value.foldWith(new Json.Folder[B] {
        def onNull: B = folder.nullValue

        def onBoolean(value: Boolean): B = if (value) folder.trueValue else folder.falseValue

        def onNumber(value: JsonNumber): B =
          value.toLong.fold(folder.floating(value.toDouble))(l => folder.integer(l))

        def onString(value: String): B = folder.string(value)

        def onArray(value: Vector[Json]): B = {
          val builder = folder.vectorBuilder()
          value.foreach(j => builder.add(fold[B](j, folder)))
          builder.finish()
        }

        def onObject(value: JsonObject): B = {
          val builder = folder.mapBuilder()
          value.toIterable.foreach { case (k, v) => builder.add(k, fold[B](v, folder)) }
          builder.finish()
        }
      })
  }

  implicit val stringDataFoldable: DataFoldableFrom[String] = new DataFoldableFrom[String] {
    def fold[B](value: String, folder: DataFolderTo[B]): B =
      folder.string(value)
  }

  import com.thatdot.quine.graph.cypher

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

  implicit val stringIterableDataFoldable: DataFoldableFrom[Iterable[String]] = new DataFoldableFrom[Iterable[String]] {
    override def fold[B](value: Iterable[String], folder: DataFolderTo[B]): B = {
      val builder = folder.vectorBuilder()
      value.foreach(v => builder.add(folder.string(v)))
      builder.finish()
    }
  }

  implicit val stringMapDataFoldable: DataFoldableFrom[Map[String, String]] =
    new DataFoldableFrom[Map[String, String]] {
      override def fold[B](value: Map[String, String], folder: DataFolderTo[B]): B = {
        val builder = folder.mapBuilder()
        value.foreach { case (name, value) =>
          builder.add(name, folder.string(value))
        }
        builder.finish()
      }
    }

}
