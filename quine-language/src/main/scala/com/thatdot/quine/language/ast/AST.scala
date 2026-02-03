package com.thatdot.quine.language.ast

import java.time.ZonedDateTime

import scala.collection.immutable.SortedMap

import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.cypher.ast.{GraphPattern, NodePattern}
import com.thatdot.quine.language.types.Type

case class CypherIdentifier(name: Symbol)
case class QuineIdentifier(name: Int)

sealed trait Source

object Source {
  case class TextSource(start: Int, end: Int) extends Source
  case object NoSource extends Source
}

sealed trait Operator

object Operator {
  case object Plus extends Operator
  case object Minus extends Operator
  case object Asterisk extends Operator
  case object Slash extends Operator
  case object Percent extends Operator
  case object Carat extends Operator
  case object Equals extends Operator
  case object NotEquals extends Operator
  case object LessThan extends Operator
  case object LessThanEqual extends Operator
  case object GreaterThan extends Operator
  case object GreaterThanEqual extends Operator
  case object And extends Operator
  case object Or extends Operator
  case object Xor extends Operator
  case object Not extends Operator
}

sealed trait Value

object Value {
  case object Null extends Value
  case object True extends Value
  case object False extends Value
  case class Integer(n: Long) extends Value
  case class Real(d: Double) extends Value
  case class Text(str: String) extends Value
  case class Bytes(arr: Array[Byte]) extends Value
  case class Duration(value: java.time.Duration) extends Value
  case class Date(value: java.time.LocalDate) extends Value
  case class DateTime(value: ZonedDateTime) extends Value
  case class DateTimeLocal(value: java.time.LocalDateTime) extends Value
  case class List(values: scala.List[Value]) extends Value
  case class Map(values: SortedMap[Symbol, Value]) extends Value
  case class NodeId(id: QuineId) extends Value
  case class Node(id: QuineId, labels: Set[Symbol], props: Value.Map) extends Value
  case class Relationship(
    start: QuineId,
    edgeType: Symbol,
    properties: scala.collection.immutable.Map[Symbol, Value],
    end: QuineId,
  ) extends Value
}

case class SpecificCase(condition: Expression, value: Expression)

sealed trait Expression {
  val source: Source
  val ty: Option[Type]
}

object Expression {
  case class IdLookup(source: Source, nodeIdentifier: Either[CypherIdentifier, QuineIdentifier], ty: Option[Type])
      extends Expression
  case class SynthesizeId(source: Source, from: List[Expression], ty: Option[Type]) extends Expression
  case class AtomicLiteral(source: Source, value: Value, ty: Option[Type]) extends Expression
  case class ListLiteral(source: Source, value: List[Expression], ty: Option[Type]) extends Expression
  case class MapLiteral(source: Source, value: Map[Symbol, Expression], ty: Option[Type]) extends Expression
  case class Ident(source: Source, identifier: Either[CypherIdentifier, QuineIdentifier], ty: Option[Type])
      extends Expression
  case class Parameter(source: Source, name: Symbol, ty: Option[Type]) extends Expression
  case class Apply(source: Source, name: Symbol, args: List[Expression], ty: Option[Type]) extends Expression
  case class UnaryOp(source: Source, op: Operator, exp: Expression, ty: Option[Type]) extends Expression
  case class BinOp(source: Source, op: Operator, lhs: Expression, rhs: Expression, ty: Option[Type]) extends Expression
  case class FieldAccess(source: Source, of: Expression, fieldName: Symbol, ty: Option[Type]) extends Expression
  case class IndexIntoArray(source: Source, of: Expression, index: Expression, ty: Option[Type]) extends Expression
  case class IsNull(source: Source, of: Expression, ty: Option[Type]) extends Expression
  case class CaseBlock(source: Source, cases: List[SpecificCase], alternative: Expression, ty: Option[Type])
      extends Expression

  def mkAtomicLiteral(source: Source, value: Value): Expression =
    AtomicLiteral(source, value, None)
}

sealed trait Direction

object Direction {
  case object Left extends Direction
  case object Right extends Direction
}

sealed trait LocalEffect

object LocalEffect {
  case class SetProperty(field: Expression.FieldAccess, to: Expression) extends LocalEffect
  case class SetLabels(on: Either[CypherIdentifier, QuineIdentifier], labels: Set[Symbol]) extends LocalEffect
  case class CreateNode(
    identifier: Either[CypherIdentifier, QuineIdentifier],
    labels: Set[Symbol],
    maybeProperties: Option[Expression.MapLiteral],
  ) extends LocalEffect
  case class CreateEdge(
    labels: Set[Symbol],
    direction: Direction,
    left: Either[CypherIdentifier, QuineIdentifier],
    right: Either[CypherIdentifier, QuineIdentifier],
    binding: Either[CypherIdentifier, QuineIdentifier],
  ) extends LocalEffect
}

case class Projection(expression: Expression, as: Either[CypherIdentifier, QuineIdentifier])

sealed trait Operation

object Operation {
  case object Call extends Operation
  case class Unwind(expression: Expression, as: Either[CypherIdentifier, QuineIdentifier]) extends Operation
  case class Effect(cypherEffect: com.thatdot.quine.cypher.ast.Effect) extends Operation
}

case class QueryDescription(
  graphPatterns: List[GraphPattern],
  nodePatterns: List[NodePattern],
  constraints: List[Expression],
  operations: List[Operation],
  projections: List[Projection],
)
