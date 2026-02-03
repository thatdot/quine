package com.thatdot.quine.language.domain

sealed trait Constraint

sealed trait GraphType

object GraphType {
  case class Uknown(tbd: String) extends GraphType
  case class Struct(name: String, fields: Map[String, String]) extends GraphType
  case object String extends GraphType
  case object Int extends GraphType
}

sealed trait FieldPattern

object FieldPattern {
  case class TypedField(withType: GraphType) extends FieldPattern
}

sealed trait NodePattern

object NodePattern {
  case class WithLabels(labels: List[String]) extends NodePattern
  case class WithFields(fields: Map[String, FieldPattern])
}

sealed trait Graph

object Graph {
  case class Node(labels: List[String]) extends Graph
  case class Edge(src: NodePattern, dest: NodePattern, labels: List[String]) extends Graph
}
