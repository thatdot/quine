package com.thatdot.quine.graph.cypher.quinepattern

import scala.annotation.tailrec
import scala.collection.immutable.SortedMap

import cats.implicits._

import com.thatdot.common.quineid.QuineId
import com.thatdot.cypher.{ast => Cypher}
import com.thatdot.language.ast._
import com.thatdot.quine.graph.cypher
import com.thatdot.quine.graph.cypher.Expr
import com.thatdot.quine.model.{EdgeDirection, PropertyValue, QuineIdProvider, QuineValue}

class QuinePatternUnimplementedException(msg: String) extends RuntimeException(msg)

object CypherAndQuineHelpers {

  def cypherValueToPatternValue(idProvider: QuineIdProvider)(value: cypher.Value): Value = value match {
    case value: Expr.PropertyValue =>
      value match {
        case Expr.Str(string) => Value.Text(string)
        case Expr.Integer(long) => Value.Integer(long)
        case Expr.Floating(double) => Value.Real(double)
        case Expr.True => Value.True
        case Expr.False => Value.False
        case bytes: Expr.Bytes =>
          if (bytes.representsId) Value.NodeId(QuineId(bytes.b))
          else throw new QuinePatternUnimplementedException(s"Don't know how to convert bytes to a pattern value")
        case Expr.List(list) => Value.List(list.toList.map(cypherValueToPatternValue(idProvider)))
        case Expr.Map(map) => Value.Map(map.map(p => Symbol(p._1) -> cypherValueToPatternValue(idProvider)(p._2)))
        case ldt: Expr.LocalDateTime => Value.DateTimeLocal(ldt.localDateTime)
        case _: Expr.Date =>
          throw new QuinePatternUnimplementedException(s"Don't know how to convert date to a pattern value")
        case _: Expr.Time =>
          throw new QuinePatternUnimplementedException(s"Don't know how to convert time to a pattern value")
        case _: Expr.LocalTime =>
          throw new QuinePatternUnimplementedException(s"Don't know how to convert local time to a pattern value")
        case Expr.DateTime(zonedDateTime) => Value.DateTime(zonedDateTime)
        case Expr.Duration(duration) => Value.Duration(duration)
      }
    case n: Expr.Number =>
      n match {
        case Expr.Integer(long) => Value.Integer(long)
        case Expr.Floating(double) => Value.Real(double)
        case Expr.Null => Value.Null
      }
    case Expr.Bool(value) => if (value) Value.True else Value.False
    case Expr.Node(id, labels, properties) =>
      Value.Node(
        id,
        labels,
        Value.Map(SortedMap.from(properties.map(p => p._1 -> cypherValueToPatternValue(idProvider)(p._2)))),
      )
    case _: Expr.Relationship =>
      throw new QuinePatternUnimplementedException(s"Don't know how to convert relationship to a pattern value")
    case _: Expr.Path =>
      throw new QuinePatternUnimplementedException(s"Don't know how to convert path to a pattern value")
  }

  /** Convert a pattern value to a property value.
    *
    * TODO There are currently some QuinePattern values that aren't representable
    * TODO as property values. We need a bigger discussion on what value algebras
    * TODO to support going forward.
    *
    * @param value
    * @return
    */
  def patternValueToPropertyValue(value: Value): Option[PropertyValue] =
    value match {
      case Value.Null => None
      case Value.True => Some(PropertyValue.apply(true))
      case Value.False => Some(PropertyValue.apply(false))
      case Value.Integer(n) => Some(PropertyValue.apply(n))
      case Value.Real(d) => Some(PropertyValue.apply(d))
      case Value.Text(str) => Some(PropertyValue.apply(str))
      case Value.DateTime(zdt) => Some(PropertyValue(QuineValue.DateTime(zdt.toOffsetDateTime)))
      case Value.DateTimeLocal(ldt) => Some(PropertyValue(QuineValue.LocalDateTime(ldt)))
      case Value.Duration(d) => Some(PropertyValue(QuineValue.Duration(d)))
      case Value.List(values) =>
        val qvs = values.map(patternValueToPropertyValue).map(_.get).map(_.deserialized.get)
        Some(PropertyValue.apply(qvs))
      case Value.Map(values) =>
        Some(PropertyValue.apply(values.map(p => p._1.name -> patternValueToPropertyValue(p._2).get.deserialized.get)))
      case _: Value.NodeId => throw new RuntimeException("Node IDs cannot be represented as property values")
      case _: Value.Node => throw new RuntimeException("Nodes cannot be represented as property values")
    }

  def quineValueToPatternValue(value: QuineValue): Value = value match {
    case QuineValue.Str(string) => Value.Text(string)
    case QuineValue.Integer(long) => Value.Integer(long)
    case QuineValue.Floating(double) => Value.Real(double)
    case QuineValue.True => Value.True
    case QuineValue.False => Value.False
    case QuineValue.Null => Value.Null
    case _: QuineValue.Bytes =>
      throw new QuinePatternUnimplementedException(s"Don't know how to convert bytes to a pattern value")
    case QuineValue.List(list) => Value.List(list.toList.map(quineValueToPatternValue))
    case QuineValue.Map(map) => Value.Map(map.map(p => Symbol(p._1) -> quineValueToPatternValue(p._2)))
    case QuineValue.DateTime(instant) => Value.DateTime(instant.toZonedDateTime)
    case d: QuineValue.Duration => Value.Duration(d.duration)
    case _: QuineValue.Date =>
      throw new QuinePatternUnimplementedException(s"Don't know how to convert date to a pattern value")
    case _: QuineValue.LocalTime =>
      throw new QuinePatternUnimplementedException(s"Don't know how to convert local time to a pattern value")
    case _: QuineValue.Time =>
      throw new QuinePatternUnimplementedException(s"Don't know how to convert time to a pattern value")
    case ldt: QuineValue.LocalDateTime => Value.DateTimeLocal(ldt.localDateTime)
    case QuineValue.Id(id) => Value.NodeId(id)
  }

  def propertyValueToPatternValue(value: PropertyValue): Value = PropertyValue.unapply(value) match {
    case Some(qv) => quineValueToPatternValue(qv)
    case None =>
      throw new QuinePatternUnimplementedException(s"Property value $value did not correctly convert to a quine value")
  }

  @tailrec
  def maybeGetByIndex[A](xs: List[A], index: Int): Option[A] = index match {
    case n if n < 0 => None
    case 0 => xs.headOption
    case _ => if (xs.isEmpty) None else maybeGetByIndex(xs.tail, index - 1)
  }

  def getNode(value: Value): Value.Node = value match {
    case node: Value.Node => node
    case _ => throw new RuntimeException(s"Expected a node, got $value")
  }

}

sealed trait QueryPlan

object QueryPlan {
  case class AllNodeScan(nodeInstructions: QueryPlan) extends QueryPlan
  case class AdjustContext(keepOnly: Set[Symbol]) extends QueryPlan
  case class SpecificId(idExp: Expression, nodeInstructions: QueryPlan) extends QueryPlan
  case class TraverseEdge(label: Symbol, direction: EdgeDirection, nodeInstructions: QueryPlan) extends QueryPlan
  case class Product(left: QueryPlan, right: QueryPlan) extends QueryPlan
  case class Unwind(listExp: Expression, alias: Identifier, over: QueryPlan) extends QueryPlan
  case class LoadNode(binding: Identifier) extends QueryPlan
  case class Filter(filterExpression: Expression) extends QueryPlan
  case class Effect(effect: Cypher.Effect) extends QueryPlan
  case class Project(projection: Expression, as: Symbol) extends QueryPlan
  case class CreateHalfEdge(from: Expression, to: Expression, direction: EdgeDirection, label: Symbol) extends QueryPlan
  case class ProcedureCall(name: Symbol, args: List[Expression], yields: List[Symbol]) extends QueryPlan
  case object UnitPlan extends QueryPlan

  def unit: QueryPlan = UnitPlan
}
