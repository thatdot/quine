package com.thatdot.quine.webapp.resultspanel

import io.circe.JsonObject

/** The structured shapes a Cypher JSON object can take, classified once so the
  * "what is a node / relationship" knowledge lives in a single place rather than as
  * inline key-sniffing in each renderer.
  *
  * The shapes mirror what the backend emits for graph values (see `Expr.toJson` in
  * quine-core): a node carries `id` + a `labels` array + a `properties` object; a
  * relationship carries `start`/`end` + a `name` + `properties`. Anything else is a
  * plain object.
  */
sealed abstract class GraphValue
object GraphValue {

  final case class Node(id: Option[String], labels: Vector[String], properties: Option[JsonObject]) extends GraphValue
  final case class Relationship(name: String, properties: Option[JsonObject]) extends GraphValue
  final case class Plain(obj: JsonObject) extends GraphValue

  def classify(obj: JsonObject): GraphValue =
    if (isNode(obj))
      Node(
        id = obj("id").flatMap(_.asString),
        labels = obj("labels").flatMap(_.asArray).getOrElse(Vector.empty).flatMap(_.asString),
        properties = obj("properties").flatMap(_.asObject).filter(_.nonEmpty),
      )
    else if (isRelationship(obj))
      Relationship(
        name = obj("name").flatMap(_.asString).getOrElse(""),
        properties = obj("properties").flatMap(_.asObject).filter(_.nonEmpty),
      )
    else Plain(obj)

  private def isNode(obj: JsonObject): Boolean =
    obj.contains("id") && obj("labels").exists(_.isArray) && obj("properties").exists(_.isObject)

  private def isRelationship(obj: JsonObject): Boolean =
    obj.contains("start") && obj.contains("end") && obj("name").exists(_.isString) &&
    obj("properties").exists(_.isObject)
}
