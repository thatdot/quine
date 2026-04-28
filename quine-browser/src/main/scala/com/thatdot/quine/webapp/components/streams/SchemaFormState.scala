package com.thatdot.quine.webapp.components.streams

import io.circe.Json

/** Utilities for treating a `Var[Json]` as a nested form state store.
  * Supports getting and setting values at arbitrary JSON paths, including array indices.
  *
  * Path segments are interpreted as array indices when they consist solely of digits
  * (e.g. `"0"`, `"12"`) and the current node is a JSON array — or, on write, when the
  * current node is not an object. This lets array-typed schemas (like `destinations: [...]`)
  * coexist with object-typed schemas within a single nested form state.
  */
object SchemaFormState {

  private def isArrayIndex(s: String): Boolean = s.nonEmpty && s.forall(_.isDigit)

  /** Get a value at a nested path within a JSON tree.
    * Numeric path segments index into arrays; other segments look up object keys.
    */
  def getAt(json: Json, path: List[String]): Option[Json] =
    path match {
      case Nil => Some(json)
      case head :: tail =>
        val child =
          if (isArrayIndex(head)) json.asArray.flatMap(_.lift(head.toInt))
          else json.hcursor.downField(head).focus
        child.flatMap(getAt(_, tail))
    }

  /** Set a value at a nested path, creating intermediate objects or arrays as needed.
    * A numeric segment writes into an array (extending with `null` padding if needed);
    * any other segment writes into an object.
    */
  def setAt(json: Json, path: List[String], value: Json): Json =
    path match {
      case Nil => value
      case head :: tail =>
        if (isArrayIndex(head)) {
          val idx = head.toInt
          val arr = json.asArray.getOrElse(Vector.empty)
          val current = arr.lift(idx).getOrElse(Json.Null)
          val updated = setAt(current, tail, value)
          val newArr =
            if (idx < arr.size) arr.updated(idx, updated)
            else arr.padTo(idx, Json.Null) :+ updated
          Json.fromValues(newArr)
        } else {
          val obj = json.asObject.map(_.toMap).getOrElse(Map.empty)
          val child = obj.getOrElse(head, Json.obj())
          val updatedChild = setAt(child, tail, value)
          Json.fromJsonObject(
            io.circe.JsonObject.fromMap(obj.updated(head, updatedChild)),
          )
        }
    }

  /** Remove a value at a nested path.
    * For arrays, the element is spliced out (array length shrinks).
    * For objects, the key is removed (intermediate objects remain as `{}`).
    */
  def removeAt(json: Json, path: List[String]): Json =
    path match {
      case Nil => Json.Null
      case head :: Nil =>
        if (isArrayIndex(head)) {
          val idx = head.toInt
          json.asArray match {
            case Some(arr) if idx < arr.size => Json.fromValues(arr.patch(idx, Nil, 1))
            case _ => json
          }
        } else {
          json.asObject match {
            case Some(obj) => Json.fromJsonObject(obj.remove(head))
            case None => json
          }
        }
      case head :: tail =>
        if (isArrayIndex(head)) {
          val idx = head.toInt
          json.asArray match {
            case Some(arr) if idx < arr.size =>
              Json.fromValues(arr.updated(idx, removeAt(arr(idx), tail)))
            case _ => json
          }
        } else {
          json.asObject match {
            case Some(obj) =>
              obj(head) match {
                case Some(child) =>
                  val updatedChild = removeAt(child, tail)
                  Json.fromJsonObject(obj.add(head, updatedChild))
                case None => json
              }
            case None => json
          }
        }
    }

  /** Humanize a camelCase or snake_case property name into a label.
    * Example: "bootstrapServers" → "Bootstrap Servers"
    * Example: "auto_offset_reset" → "Auto Offset Reset"
    */
  def humanizeFieldName(name: String): String = {
    val spaced = name
      .replaceAll("([a-z])([A-Z])", "$1 $2")
      .replaceAll("_", " ")
    spaced.split(" ").map(w => if (w.nonEmpty) s"${w.head.toUpper}${w.tail}" else w).mkString(" ")
  }
}
