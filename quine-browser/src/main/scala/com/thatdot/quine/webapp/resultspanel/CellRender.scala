package com.thatdot.quine.webapp.resultspanel

import com.raquo.laminar.api.L._
import io.circe.Printer.noSpaces
import io.circe.{Json, JsonObject}

import com.thatdot.quine.webapp.Styles

/** Renders one JSON result value into a single clipped line.
  *
  * Plain values render as syntax-highlighted **JSON** — one consistent syntax at every
  * depth (quoted keys, quoted strings, JSON delimiters), coloured so the type is always
  * clear: strings green, numbers blue, booleans a keyword colour, `null` muted, keys bold,
  * punctuation grey. The displayed characters are exactly `noSpaces.print(value)`, so the
  * visible cell (when it isn't clipped) is itself the recoverable value.
  *
  * Graph values are the one deliberate exception: a node shows its labels as chips and a
  * relationship its type as a chip, with the properties previewed as JSON. A chip is a
  * widget, not a competing text syntax, so this doesn't reintroduce the JSON/Cypher mixing
  * the plain-JSON rendering avoids. The full value (id included) is always on the hover
  * `title` and in the row drawer.
  */
object CellRender {

  def value(value: Json): HtmlElement =
    value.asObject.map(GraphValue.classify) match {
      case Some(GraphValue.Node(_, labels, props)) => nodeCell(labels, props, noSpaces.print(value))
      case Some(GraphValue.Relationship(name, props)) => relationshipCell(name, props, noSpaces.print(value))
      case _ => jsonCell(value)
    }

  /** A plain value as syntax-highlighted, compact JSON, clipped to the column width. */
  private def jsonCell(value: Json): HtmlElement =
    span(cls := Styles.cellClip, title := noSpaces.print(value), jsonTokens(value))

  /** Node: label chips + a JSON preview of the properties. Id + full value on hover/drawer. */
  private def nodeCell(labels: Vector[String], props: Option[JsonObject], full: String): HtmlElement =
    span(
      cls := Styles.cellNode,
      title := full,
      labels.map(label => span(cls := Styles.cellLabelChip, label)),
      props.map(p => span(cls := Styles.cellNodeProps, jsonTokens(Json.fromJsonObject(p)))),
    )

  private def relationshipCell(name: String, props: Option[JsonObject], full: String): HtmlElement =
    span(
      cls := Styles.cellNode,
      title := full,
      span(cls := Styles.cellRelChip, name),
      props.map(p => span(cls := Styles.cellNodeProps, jsonTokens(Json.fromJsonObject(p)))),
    )

  /** Compact JSON as a sequence of coloured token elements. The concatenated text is exactly
    * `noSpaces.print(value)` — nothing is added, dropped, or reordered, only coloured — so a
    * cell's visible characters are the value's own JSON.
    */
  private def jsonTokens(value: Json): Vector[HtmlElement] = value.fold(
    jsonNull = Vector(span(cls := Styles.cellNull, "null")),
    jsonBoolean = bool => Vector(span(cls := Styles.cellBool, bool.toString)),
    jsonNumber = num => Vector(span(cls := Styles.cellNumber, num.toString)),
    jsonString = str => Vector(stringToken(str, Styles.cellString)),
    jsonArray = arr => {
      val inner = arr.zipWithIndex.flatMap { case (el, i) =>
        val toks = jsonTokens(el)
        if (i == 0) toks else punct(",") +: toks
      }
      (punct("[") +: inner) :+ punct("]")
    },
    jsonObject = obj => {
      val inner = obj.toVector.zipWithIndex.flatMap { case ((k, v), i) =>
        val pair = stringToken(k, Styles.cellKey) +: (punct(":") +: jsonTokens(v))
        if (i == 0) pair else punct(",") +: pair
      }
      (punct("{") +: inner) :+ punct("}")
    },
  )

  /** A JSON string literal (`"…"`, properly escaped) with punctuation-coloured quotes; the
    * content carries `contentClass` — the string colour for values, the key colour for
    * object keys.
    */
  private def stringToken(text: String, contentClass: String): HtmlElement = {
    val printed = noSpaces.print(Json.fromString(text)) // surrounding quotes + JSON escaping
    val inner = printed.substring(1, printed.length - 1)
    span(cls := contentClass, span(cls := Styles.cellPunct, "\""), inner, span(cls := Styles.cellPunct, "\""))
  }

  private def punct(text: String): HtmlElement = span(cls := Styles.cellPunct, text)
}
