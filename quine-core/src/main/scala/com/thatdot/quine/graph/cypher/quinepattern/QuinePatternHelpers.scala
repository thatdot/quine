package com.thatdot.quine.graph.cypher.quinepattern

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorRef
import org.apache.pekko.actor.Status.Success
import org.apache.pekko.stream.CompletionStrategy
import org.apache.pekko.stream.scaladsl.Source

import com.thatdot.language.ast.{Direction, Expression, Identifier, Value}
import com.thatdot.quine.graph.behavior.QuinePatternCommand
import com.thatdot.quine.graph.cypher
import com.thatdot.quine.graph.cypher.{Expr, QueryContext}
import com.thatdot.quine.model.EdgeDirection

object QuinePatternHelpers {
  def reallyBadStringifier(exp: Expression): String =
    exp match {
      case idl: Expression.IdLookup => s"id(${idl.nodeIdentifier.name.name})"
      case _: Expression.SynthesizeId => throw new QuinePatternUnimplementedException(s"Not yet implemented: $exp")
      case _: Expression.AtomicLiteral => throw new QuinePatternUnimplementedException(s"Not yet implemented: $exp")
      case _: Expression.ListLiteral => throw new QuinePatternUnimplementedException(s"Not yet implemented: $exp")
      case _: Expression.MapLiteral => throw new QuinePatternUnimplementedException(s"Not yet implemented: $exp")
      case id: Expression.Ident => s"${id.identifier.name.name}"
      case _: Expression.Parameter => throw new QuinePatternUnimplementedException(s"Not yet implemented: $exp")
      case _: Expression.Apply => throw new QuinePatternUnimplementedException(s"Not yet implemented: $exp")
      case _: Expression.UnaryOp => throw new QuinePatternUnimplementedException(s"Not yet implemented: $exp")
      case _: Expression.BinOp => throw new QuinePatternUnimplementedException(s"Not yet implemented: $exp")
      case fa: Expression.FieldAccess => s"${reallyBadStringifier(fa.of)}.${fa.fieldName.name.name}"
      case _: Expression.IndexIntoArray => throw new QuinePatternUnimplementedException(s"Not yet implemented: $exp")
      case _: Expression.IsNull => throw new QuinePatternUnimplementedException(s"Not yet implemented: $exp")
      case _: Expression.CaseBlock => throw new QuinePatternUnimplementedException(s"Not yet implemented: $exp")
    }

  def patternValueToCypherValue(value: Value): cypher.Value =
    value match {
      case Value.Null => Expr.Null
      case Value.True => Expr.True
      case Value.False => Expr.False
      case Value.Integer(n) => Expr.Integer(n)
      case Value.Real(d) => Expr.Floating(d)
      case Value.Text(str) => Expr.Str(str)
      case Value.DateTime(zdt) => Expr.DateTime(zdt)
      case Value.List(values) => Expr.List(values.toVector.map(patternValueToCypherValue))
      case Value.Map(values) => Expr.Map(values.map(p => p._1.name -> patternValueToCypherValue(p._2)))
      case Value.NodeId(qid) => Expr.Bytes(qid)
      case Value.Node(id, labels, props) =>
        Expr.Node(id, labels, props.values.map(p => p._1 -> patternValueToCypherValue(p._2)))
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

  def getRootId(expression: Expression): Identifier = expression match {
    case Expression.FieldAccess(_, of, _, _) => getRootId(of)
    case Expression.Ident(_, id, _) => id
    case Expression.Parameter(_, name, _) => Identifier(name, None)
    case _ => throw new QuinePatternUnimplementedException(s"Unexpected expression: $expression")
  }
}
