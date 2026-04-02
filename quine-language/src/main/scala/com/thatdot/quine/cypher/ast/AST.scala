package com.thatdot.quine.cypher.ast

import com.thatdot.quine.language.ast.{BindingId, CypherIdentifier, Direction, Expression, Source}

case class Projection(source: Source, expression: Expression, as: Either[CypherIdentifier, BindingId])

/** A single item in an ORDER BY clause.
  * @param source Source location in the query text
  * @param expression The expression to sort by
  * @param ascending True for ASC (default), false for DESC
  */
case class SortItem(source: Source, expression: Expression, ascending: Boolean)

sealed trait Effect {
  val source: Source
}

object Effect {
  case class Foreach(
    source: Source,
    binding: Either[CypherIdentifier, BindingId],
    in: Expression,
    effects: List[Effect],
  ) extends Effect
  case class SetProperty(source: Source, property: Expression.FieldAccess, value: Expression) extends Effect
  case class SetProperties(source: Source, of: Either[CypherIdentifier, BindingId], properties: Expression)
      extends Effect
  case class SetLabel(source: Source, on: Either[CypherIdentifier, BindingId], labels: Set[Symbol]) extends Effect
  case class Create(source: Source, patterns: List[GraphPattern]) extends Effect
}

case class EdgePattern(
  source: Source,
  maybeBinding: Option[Either[CypherIdentifier, BindingId]],
  direction: Direction,
  edgeType: Symbol,
)

case class Connection(edge: EdgePattern, dest: NodePattern)

/** Match(LiteralNodePattern(...))
  *
  * (a)
  * ()
  * (:Foo {x = 3})
  * (a :Foo {x = 3, y = "bob})
  * (a :Foo | Bar)
  * ($that)
  */
case class NodePattern(
  source: Source,
  maybeBinding: Option[Either[CypherIdentifier, BindingId]],
  labels: Set[Symbol],
  maybeProperties: Option[Expression],
)

case class GraphPattern(source: Source, initial: NodePattern, path: List[Connection])

sealed trait ReadingClause {
  val source: Source
}

/** Represents a single item in a YIELD clause.
  * @param resultField The name of the field returned by the procedure
  * @param boundAs The variable it's bound to in the query scope (starts as CypherIdentifier,
  *                rewritten to BindingId during symbol analysis)
  *
  * Examples:
  *   - `YIELD edge` -> YieldItem(resultField = 'edge, boundAs = Left(CypherIdentifier('edge)))
  *   - `YIELD result AS r` -> YieldItem(resultField = 'result, boundAs = Left(CypherIdentifier('r)))
  */
case class YieldItem(resultField: Symbol, boundAs: Either[CypherIdentifier, BindingId])

object ReadingClause {
  case class FromPatterns(
    source: Source,
    patterns: List[GraphPattern],
    maybePredicate: Option[Expression],
    isOptional: Boolean = false,
  ) extends ReadingClause
  case class FromUnwind(source: Source, list: Expression, as: Either[CypherIdentifier, BindingId]) extends ReadingClause
  case class FromProcedure(source: Source, name: Symbol, args: List[Expression], yields: List[YieldItem])
      extends ReadingClause
  case class FromSubquery(
    source: Source,
    bindings: List[Either[CypherIdentifier, BindingId]],
    subquery: Query,
  ) extends ReadingClause
}

case class WithClause(
  source: Source,
  hasWildCard: Boolean,
  isDistinct: Boolean,
  bindings: List[Projection],
  maybePredicate: Option[Expression],
  orderBy: List[SortItem] = Nil,
  maybeSkip: Option[Expression] = None,
  maybeLimit: Option[Expression] = None,
)

sealed trait QueryPart

object QueryPart {
  case class ReadingClausePart(readingClause: ReadingClause) extends QueryPart
  case class WithClausePart(withClause: WithClause) extends QueryPart
  case class EffectPart(effect: Effect) extends QueryPart
}

sealed trait Query {
  val source: Source
}

object Query {
  case class Union(source: Source, all: Boolean, lhs: Query, rhs: SingleQuery) extends Query

  sealed trait SingleQuery extends Query

  object SingleQuery {
    case class MultipartQuery(source: Source, queryParts: List[QueryPart], into: SinglepartQuery) extends SingleQuery
    case class SinglepartQuery(
      source: Source,
      queryParts: List[QueryPart],
      hasWildcard: Boolean,
      isDistinct: Boolean,
      bindings: List[Projection],
      orderBy: List[SortItem] = Nil,
      maybeSkip: Option[Expression] = None,
      maybeLimit: Option[Expression] = None,
    ) extends SingleQuery
  }
}
