package com.thatdot.quine.app.data

import com.thatdot.data.{DataFoldableFrom, DataFolderTo}
import com.thatdot.quine.graph.cypher
import com.thatdot.quine.graph.cypher.Expr
import com.thatdot.quine.model.{QuineIdProvider, QuineValue}
import com.thatdot.quine.serialization.data.QuineSerializationFoldablesFrom

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
          case Expr.Bytes(b, true) => folder.id(b)
          case Expr.Bytes(b, false) => folder.bytes(b)
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
      // Nodes, relationships, and paths fold to the same JSON-object shape the text `/cypher` API
      // publishes via `cypher.Expr.toJson` — a node as `{id, labels, properties}`, a relationship as
      // `{start, end, name, properties}`, a path as the flattened alternating list of its elements.
      // Ids fold through `folder.id`, matching how a bare `Expr.Bytes(_, true)` id renders above so a
      // node's id is encoded identically to a standalone id value in the same output.
      case Expr.Node(id, labels, properties) =>
        val builder = folder.mapBuilder()
        builder.add("id", folder.id(id.array))
        val labelsBuilder = folder.vectorBuilder()
        labels.foreach(l => labelsBuilder.add(folder.string(l.name)))
        builder.add("labels", labelsBuilder.finish())
        builder.add("properties", foldProperties(properties, folder))
        builder.finish()
      case Expr.Relationship(start, name, properties, end) =>
        val builder = folder.mapBuilder()
        builder.add("start", folder.id(start.array))
        builder.add("end", folder.id(end.array))
        builder.add("name", folder.string(name.name))
        builder.add("properties", foldProperties(properties, folder))
        builder.finish()
      case path: Expr.Path => fold(path.toList, folder)
    }

    private def foldProperties[B](properties: Map[Symbol, cypher.Value], folder: DataFolderTo[B]): B = {
      val builder = folder.mapBuilder()
      properties.foreach { case (k, v) => builder.add(k.name, fold(v, folder)) }
      builder.finish()
    }
  }

  def quineValueDataFoldable(implicit idProvider: QuineIdProvider): DataFoldableFrom[QuineValue] =
    QuineSerializationFoldablesFrom.quineValueDataFoldableFrom
}
