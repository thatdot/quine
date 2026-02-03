package com.thatdot.quine.graph.cypher.quinepattern

import scala.annotation.tailrec
import scala.collection.immutable.SortedMap

import cats.implicits._

import com.thatdot.common.quineid.QuineId
import com.thatdot.language.ast._
import com.thatdot.quine.graph.cypher
import com.thatdot.quine.graph.cypher.CypherException.Runtime
import com.thatdot.quine.graph.cypher.{CypherException, Expr}
import com.thatdot.quine.model.{PropertyValue, QuineIdProvider, QuineValue}

class QuinePatternUnimplementedException(msg: String) extends RuntimeException(msg)

object CypherAndQuineHelpers {

  def cypherValueToPatternValue(idProvider: QuineIdProvider)(value: cypher.Value): Either[Runtime, Value] =
    value match {
      case value: Expr.PropertyValue =>
        value match {
          case Expr.Str(string) => Right(Value.Text(string))
          case Expr.Integer(long) => Right(Value.Integer(long))
          case Expr.Floating(double) => Right(Value.Real(double))
          case Expr.True => Right(Value.True)
          case Expr.False => Right(Value.False)
          case cyhperBytes: Expr.Bytes =>
            Right(
              if (cyhperBytes.representsId) Value.NodeId(QuineId(cyhperBytes.b))
              else Value.Bytes(cyhperBytes.b),
            )
          case Expr.List(list) => list.toList.traverse(cypherValueToPatternValue(idProvider)).map(Value.List(_))
          case Expr.Map(map) =>
            map.toList
              .traverse(p => cypherValueToPatternValue(idProvider)(p._2).map(v => Symbol(p._1) -> v))
              .map(xs => Value.Map(SortedMap.from(xs)))
          case ldt: Expr.LocalDateTime => Right(Value.DateTimeLocal(ldt.localDateTime))
          case cypherDate: Expr.Date => Right(Value.Date(cypherDate.date))
          case _: Expr.Time =>
            throw new QuinePatternUnimplementedException(s"Don't know how to convert time to a pattern value")
          case _: Expr.LocalTime =>
            throw new QuinePatternUnimplementedException(s"Don't know how to convert local time to a pattern value")
          case Expr.DateTime(zonedDateTime) => Right(Value.DateTime(zonedDateTime))
          case Expr.Duration(duration) => Right(Value.Duration(duration))
        }
      case n: Expr.Number =>
        Right(n match {
          case Expr.Integer(long) => Value.Integer(long)
          case Expr.Floating(double) => Value.Real(double)
          case Expr.Null => Value.Null
        })
      case Expr.Bool(value) => Right(if (value) Value.True else Value.False)
      case Expr.Node(id, labels, properties) =>
        properties.toList
          .traverse(p => cypherValueToPatternValue(idProvider)(p._2).map(v => p._1 -> v))
          .map(xs => Value.Map(SortedMap.from(xs)))
          .map { pmap =>
            Value.Node(id, labels, pmap)
          }
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
      case Value.Bytes(bytes) => Some(PropertyValue.apply(QuineValue.Bytes(bytes)))
      case Value.Date(date) => Some(PropertyValue.apply(QuineValue.Date(date)))
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
      case _: Value.Relationship => throw new RuntimeException("Relationships cannot be represented as property values")
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

  /** Convert a pattern value to a QuineValue.
    *
    * This is the inverse of quineValueToPatternValue.
    */
  def patternValueToQuineValue(value: Value): QuineValue = value match {
    case Value.Text(string) => QuineValue.Str(string)
    case Value.Integer(long) => QuineValue.Integer(long)
    case Value.Real(double) => QuineValue.Floating(double)
    case Value.True => QuineValue.True
    case Value.False => QuineValue.False
    case Value.Null => QuineValue.Null
    case Value.Bytes(bytes) => QuineValue.Bytes(bytes)
    case Value.Date(date) => QuineValue.Date(date)
    case Value.DateTime(zdt) => QuineValue.DateTime(zdt.toOffsetDateTime)
    case Value.DateTimeLocal(ldt) => QuineValue.LocalDateTime(ldt)
    case Value.Duration(d) => QuineValue.Duration(d)
    case Value.List(values) => QuineValue.List(values.map(patternValueToQuineValue).toVector)
    case Value.Map(values) => QuineValue.Map(values.map { case (k, v) => k.name -> patternValueToQuineValue(v) })
    case Value.NodeId(id) => QuineValue.Id(id)
    case Value.Node(id, _, props) =>
      // Convert node to a map representation with id and properties
      // props is a Value.Map which contains .values: SortedMap[Symbol, Value]
      val propMap = props.values.map { case (k, v) => k.name -> patternValueToQuineValue(v) }
      QuineValue.Map(propMap + ("_id" -> QuineValue.Id(id)))
    case Value.Relationship(start, edgeType, props, end) =>
      // Convert relationship to a map representation
      val propMap = props.map { case (k, v) => k.name -> patternValueToQuineValue(v) }
      QuineValue.Map(
        propMap +
        ("_start" -> QuineValue.Id(start)) +
        ("_end" -> QuineValue.Id(end)) +
        ("_type" -> QuineValue.Str(edgeType.name)),
      )
  }

  @tailrec
  def maybeGetByIndex[A](xs: List[A], index: Int): Option[A] = index match {
    case n if n < 0 => None
    case 0 => xs.headOption
    case _ => if (xs.isEmpty) None else maybeGetByIndex(xs.tail, index - 1)
  }

  def getNode(value: Value): Either[Runtime, Value.Node] = value match {
    case node: Value.Node => Right(node)
    case _ => Left(CypherException.Runtime(s"Expected a node shaped value, got $value"))
  }

}
