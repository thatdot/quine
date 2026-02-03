package com.thatdot.quine.graph.cypher.quinepattern

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorRef
import org.apache.pekko.actor.Status.Success
import org.apache.pekko.stream.CompletionStrategy
import org.apache.pekko.stream.scaladsl.Source

import com.thatdot.quine.graph.behavior.QuinePatternCommand
import com.thatdot.quine.graph.cypher
import com.thatdot.quine.graph.cypher.CypherException.Runtime
import com.thatdot.quine.graph.cypher.{CypherException, Expr, QueryContext}
import com.thatdot.quine.language.ast.{CypherIdentifier, Direction, Expression, QuineIdentifier, Value}
import com.thatdot.quine.model.EdgeDirection

object QuinePatternHelpers {

  def patternValueToCypherValue(value: Value): cypher.Value =
    value match {
      case Value.Null => Expr.Null
      case Value.True => Expr.True
      case Value.False => Expr.False
      case Value.Integer(n) => Expr.Integer(n)
      case Value.Real(d) => Expr.Floating(d)
      case Value.Bytes(bytes) => Expr.Bytes(bytes)
      case Value.Date(date) => Expr.Date(date)
      case Value.Text(str) => Expr.Str(str)
      case Value.DateTime(zdt) => Expr.DateTime(zdt)
      case Value.List(values) => Expr.List(values.toVector.map(patternValueToCypherValue))
      case Value.Map(values) => Expr.Map(values.map(p => p._1.name -> patternValueToCypherValue(p._2)))
      case Value.NodeId(qid) => Expr.Bytes(qid.array, representsId = true)
      case Value.Node(id, labels, props) =>
        Expr.Node(id, labels, props.values.map(p => p._1 -> patternValueToCypherValue(p._2)))
      case Value.Relationship(start, edgeType, properties, end) =>
        Expr.Relationship(start, edgeType, properties.map(p => p._1 -> patternValueToCypherValue(p._2)), end)
      case Value.Duration(d) => Expr.Duration(d)
      case Value.DateTimeLocal(dtl) => Expr.LocalDateTime(dtl)
    }

  def createNodeActionSource(nodeAction: ActorRef => NotUsed): Source[QueryContext, NotUsed] =
    Source
      .actorRefWithBackpressure[QueryContext](
        ackMessage = QuinePatternCommand.QuinePatternAck,
        completionMatcher = { case _: Success =>
          CompletionStrategy.immediately
        },
        failureMatcher = PartialFunction.empty,
      )
      .mapMaterializedValue(nodeAction)

  def directionToEdgeDirection(direction: Direction): EdgeDirection = direction match {
    case Direction.Left => EdgeDirection.Outgoing
    case Direction.Right => EdgeDirection.Incoming
  }

  def getRootId(expression: Expression): Either[Runtime, Either[CypherIdentifier, QuineIdentifier]] = expression match {
    case Expression.FieldAccess(_, of, _, _) => getRootId(of)
    case Expression.Ident(_, id, _) => Right(id)
    case Expression.Parameter(_, name, _) => Right(Left(CypherIdentifier(name)))
    case _ => Left(CypherException.Runtime(s"Cannot extract a root id from: $expression"))
  }
}
