package com.thatdot.quine.graph.cypher.quinepattern

import scala.collection.immutable.SortedMap

import cats.implicits._

import com.thatdot.common.quineid.QuineId
import com.thatdot.cypher.{ast => Cypher}
import com.thatdot.language.ast._
import com.thatdot.quine.graph.cypher
import com.thatdot.quine.graph.cypher.{Expr, QueryContext}
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
          if (bytes.representsId) Value.NodeId(QuineId(bytes.b)) else ???
        case Expr.List(list) => Value.List(list.toList.map(cypherValueToPatternValue(idProvider)))
        case Expr.Map(map) => Value.Map(map.map(p => Symbol(p._1) -> cypherValueToPatternValue(idProvider)(p._2)))
        case Expr.LocalDateTime(localDateTime) => ???
        case Expr.Date(date) => ???
        case Expr.Time(time) => ???
        case Expr.LocalTime(localTime) => ???
        case Expr.DateTime(zonedDateTime) => Value.DateTime(zonedDateTime)
        case Expr.Duration(duration) => ???
      }
    case n: Expr.Number =>
      n match {
        case Expr.Integer(long) => Value.Integer(long)
        case Expr.Floating(double) => Value.Real(double)
        case Expr.Null => Value.Null
      }
    case _: Expr.Bool => ???
    case Expr.Node(id, labels, properties) =>
      Value.Node(
        id,
        labels,
        Value.Map(SortedMap.from(properties.map(p => p._1 -> cypherValueToPatternValue(idProvider)(p._2)))),
      )
    case Expr.Relationship(start, name, properties, end) => ???
    case Expr.Path(head, tails) => ???
  }

  def patternValueToPropertyValue(value: Value): Option[PropertyValue] =
    value match {
      case Value.Null => None
      case Value.True => Some(PropertyValue.apply(true))
      case Value.False => Some(PropertyValue.apply(false))
      case Value.Integer(n) => Some(PropertyValue.apply(n))
      case Value.Real(d) => Some(PropertyValue.apply(d))
      case Value.Text(str) => Some(PropertyValue.apply(str))
      case Value.DateTime(zdt) => Some(PropertyValue(QuineValue.DateTime(zdt.toOffsetDateTime)))
      case Value.List(values) =>
        val qvs = values.map(patternValueToPropertyValue).map(_.get).map(_.deserialized.get)
        Some(PropertyValue.apply(qvs))
      case Value.Map(values) =>
        Some(PropertyValue.apply(values.map(p => p._1.name -> patternValueToPropertyValue(p._2).get.deserialized.get)))
      case Value.NodeId(_) => ???
      case _: Value.Node => ???
    }

  def quineValueToPatternValue(value: QuineValue): Value = value match {
    case QuineValue.Str(string) => Value.Text(string)
    case QuineValue.Integer(long) => Value.Integer(long)
    case QuineValue.Floating(double) => Value.Real(double)
    case QuineValue.True => ???
    case QuineValue.False => ???
    case QuineValue.Null => ???
    case QuineValue.Bytes(bytes) => ???
    case QuineValue.List(list) => Value.List(list.toList.map(quineValueToPatternValue))
    case QuineValue.Map(map) => Value.Map(map.map(p => Symbol(p._1) -> quineValueToPatternValue(p._2)))
    case QuineValue.DateTime(instant) => Value.DateTime(instant.toZonedDateTime)
    case QuineValue.Duration(duration) => ???
    case QuineValue.Date(date) => ???
    case QuineValue.LocalTime(time) => ???
    case QuineValue.Time(time) => ???
    case QuineValue.LocalDateTime(localDateTime) => ???
    case QuineValue.Id(id) => ???
  }

  def propertyValueToPatternValue(value: PropertyValue): Value = PropertyValue.unapply(value) match {
    case Some(qv) => quineValueToPatternValue(qv)
    case None => ???
  }

  def maybeGetByIndex[A](xs: List[A], index: Int): Option[A] = index match {
    case n if n < 0 => None
    case 0 => xs.headOption
    case _ => if (xs.isEmpty) None else maybeGetByIndex(xs.tail, index - 1)
  }

  def getTextValue(value: Value): Option[String] = value match {
    case Value.Text(str) => Some(str)
    case Value.Null => None
    case _ => throw new Exception("This should be either a text value or null.")
  }

  def getIdentifier(exp: Expression): Expression.Ident = exp match {
    case ident: Expression.Ident => ident
    case _ => ???
  }

  def getNode(value: Value): Value.Node = value match {
    case node: Value.Node => node
    case _ => ???
  }

  def getNodeIdFromIdent(identifier: Identifier, queryContext: QueryContext): QuineId =
    queryContext(identifier.name) match {
      case node: Expr.Node => node.id
      case _ => ???
    }

  def invert(direction: EdgeDirection): EdgeDirection = direction match {
    case EdgeDirection.Outgoing => EdgeDirection.Incoming
    case EdgeDirection.Incoming => EdgeDirection.Outgoing
    case EdgeDirection.Undirected => EdgeDirection.Undirected
  }
}

sealed trait QueryPlan

object QueryPlan {
  case class AllNodeScan(nodeInstructions: List[QueryPlan]) extends QueryPlan
  case class SpecificId(idExp: Expression, nodeInstructions: List[QueryPlan]) extends QueryPlan
  case class TraverseEdge(label: Symbol, direction: EdgeDirection, nodeInstructions: List[QueryPlan]) extends QueryPlan
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
