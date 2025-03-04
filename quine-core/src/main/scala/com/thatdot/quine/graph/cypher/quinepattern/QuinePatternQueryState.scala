package com.thatdot.quine.graph.cypher.quinepattern

import java.util.concurrent.ConcurrentHashMap

import org.apache.pekko.actor.ActorRef

import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.common.quineid.QuineId
import com.thatdot.cypher.{ast => Cypher}
import com.thatdot.language.ast.Source.NoSource
import com.thatdot.language.ast.{Expression, Identifier, Value}
import com.thatdot.quine.graph.behavior.{QuinePatternCommand, QuinePatternQueryBehavior}
import com.thatdot.quine.graph.cypher.Expr.toQuineValue
import com.thatdot.quine.graph.cypher.quinepattern.CypherAndQuineHelpers.quineValueToPatternValue
import com.thatdot.quine.graph.cypher.quinepattern.LazyQuinePatternQueryPlanner.LazyQueryPlan
import com.thatdot.quine.graph.cypher.{Expr, Parameters, QueryContext}
import com.thatdot.quine.graph.messaging.SpaceTimeQuineId
import com.thatdot.quine.graph.quinepattern.QuinePatternOpsGraph
import com.thatdot.quine.graph.{RunningStandingQuery, StandingQueryId, StandingQueryResult}
import com.thatdot.quine.model.{EdgeDirection, HalfEdge, PropertyValue, QuineValue}

sealed trait QuinePatternQueryState {
  val quinePatternNode: QuinePatternQueryBehavior
  def publish(ctx: QueryContext, sqid: StandingQueryId): Unit
}

case class ProductState(quinePatternNode: QuinePatternQueryBehavior, toNotify: StandingQueryId, me: StandingQueryId)
    extends QuinePatternQueryState {
  val localState: ConcurrentHashMap[StandingQueryId, QueryContext] =
    new ConcurrentHashMap[StandingQueryId, QueryContext]()

  override def publish(ctx: QueryContext, sqid: StandingQueryId): Unit = {
    localState.put(sqid, ctx)

    var resultContext = QueryContext.empty

    localState.values().forEach(ctx => resultContext = resultContext ++ ctx)

    quinePatternNode.states.get(toNotify).publish(resultContext, me)
  }
}

case class FilterState(
  quinePatternNode: QuinePatternQueryBehavior,
  toNotify: StandingQueryId,
  me: StandingQueryId,
  filter: Expression,
  projections: List[Cypher.Projection],
  isDistinct: Boolean,
) extends QuinePatternQueryState {

  def rewriteFilter(expression: Expression, context: QueryContext): Expression = expression match {
    case idl: Expression.IdLookup =>
      if (context.environment.contains(Symbol(QuinePatternHelpers.reallyBadStringifier(idl))))
        Expression.Ident(NoSource, Identifier(Symbol(QuinePatternHelpers.reallyBadStringifier(idl)), None), None)
      else
        idl
    case _: Expression.SynthesizeId => throw new QuinePatternUnimplementedException(s"Not yet implemented: $expression")
    case literal: Expression.AtomicLiteral => literal
    case _: Expression.ListLiteral => throw new QuinePatternUnimplementedException(s"Not yet implemented: $expression")
    case _: Expression.MapLiteral => throw new QuinePatternUnimplementedException(s"Not yet implemented: $expression")
    case id: Expression.Ident => id
    case _: Expression.Parameter => throw new QuinePatternUnimplementedException(s"Not yet implemented: $expression")
    case _: Expression.Apply => throw new QuinePatternUnimplementedException(s"Not yet implemented: $expression")
    case unary: Expression.UnaryOp => unary.copy(exp = rewriteFilter(unary.exp, context))
    case binop: Expression.BinOp =>
      val rl = rewriteFilter(binop.lhs, context)
      val rr = rewriteFilter(binop.rhs, context)
      binop.copy(lhs = rl, rhs = rr)
    case fa: Expression.FieldAccess =>
      if (context.environment.contains(Symbol(QuinePatternHelpers.reallyBadStringifier(fa))))
        Expression.Ident(NoSource, Identifier(Symbol(QuinePatternHelpers.reallyBadStringifier(fa)), None), None)
      else
        fa.copy(of = rewriteFilter(fa.of, context))
    case _: Expression.IndexIntoArray =>
      throw new QuinePatternUnimplementedException(s"Not yet implemented: $expression")
    case isNull: Expression.IsNull => isNull.copy(of = rewriteFilter(isNull.of, context))
    case _: Expression.CaseBlock => throw new QuinePatternUnimplementedException(s"Not yet implemented: $expression")
  }

  def rewriteProjection(projection: Cypher.Projection, context: QueryContext): Cypher.Projection =
    if (context.environment.contains(projection.as.name))
      projection.copy(expression = Expression.Ident(NoSource, Identifier(projection.as.name, None), None))
    else
      projection.copy(expression = rewriteFilter(projection.expression, context))

  var published = false

  override def publish(ctx: QueryContext, sqid: StandingQueryId): Unit = {
    val moddedFilter = rewriteFilter(filter, ctx)
    val moddedProjections = projections.map(p => rewriteProjection(p, ctx))

    QuinePatternExpressionInterpreter.eval(moddedFilter, quinePatternNode.idProvider, ctx, Map.empty) match {
      case Value.True =>
        val finalCtx = moddedProjections.foldLeft(ctx) { (ctx, p) =>
          ctx + (p.as.name -> QuinePatternHelpers.patternValueToCypherValue(
            QuinePatternExpressionInterpreter.eval(p.expression, quinePatternNode.idProvider, ctx, Map.empty),
          ))
        }
        if (
          !finalCtx.environment.values.exists {
            case Expr.Null => true
            case _ => false
          }
        ) {
          if (isDistinct) {
            if (!published) {
              quinePatternNode.states.get(toNotify).publish(finalCtx, me)
              published = true
            }
          } else {
            quinePatternNode.states.get(toNotify).publish(finalCtx, me)
          }
        }
      case Value.False => ()
      case _ => throw new QuinePatternUnimplementedException(s"Not yet implemented: $moddedFilter")
    }
  }
}

case class WatchState(
  quinePatternNode: QuinePatternQueryBehavior,
  properties: Map[Symbol, PropertyValue],
  toNotify: StandingQueryId,
  me: StandingQueryId,
  exp: Expression,
) extends QuinePatternQueryState {
  exp match {
    case _: Expression.IdLookup =>
      val binding = QuinePatternHelpers.reallyBadStringifier(exp)
      val idBytes = Expr.Bytes(quinePatternNode.qid)
      val ctx = QueryContext(Map(Symbol(binding) -> idBytes))
      quinePatternNode.states.get(toNotify).publish(ctx, me)
    case Expression.FieldAccess(_, _, fieldName, _) =>
      properties.get(fieldName.name) match {
        case None => ()
        case Some(value) =>
          val str = QuinePatternHelpers.reallyBadStringifier(exp)
          val ctx = QueryContext(
            Map(
              Symbol(str) -> QuinePatternHelpers.patternValueToCypherValue(
                quineValueToPatternValue(value.deserialized.get),
              ),
            ),
          )
          quinePatternNode.states.get(toNotify).publish(ctx, me)
      }
    case _ => () // Probably other cases I need to handle here?
  }

  // This should always be a leaf node?
  override def publish(ctx: QueryContext, sqid: StandingQueryId): Unit =
    throw new QuinePatternUnimplementedException(s"Not yet implemented: publish for WatchState")

  def propertyAdded(propertyName: Symbol, value: PropertyValue): Unit =
    exp match {
      case _: Expression.IdLookup => ()
      case _: Expression.SynthesizeId => throw new QuinePatternUnimplementedException(s"Not yet implemented: $exp")
      case _: Expression.AtomicLiteral => throw new QuinePatternUnimplementedException(s"Not yet implemented: $exp")
      case _: Expression.ListLiteral => throw new QuinePatternUnimplementedException(s"Not yet implemented: $exp")
      case _: Expression.MapLiteral => throw new QuinePatternUnimplementedException(s"Not yet implemented: $exp")
      case _: Expression.Ident => throw new QuinePatternUnimplementedException(s"Not yet implemented: $exp")
      case _: Expression.Parameter => throw new QuinePatternUnimplementedException(s"Not yet implemented: $exp")
      case _: Expression.Apply => throw new QuinePatternUnimplementedException(s"Not yet implemented: $exp")
      case _: Expression.UnaryOp => throw new QuinePatternUnimplementedException(s"Not yet implemented: $exp")
      case _: Expression.BinOp => throw new QuinePatternUnimplementedException(s"Not yet implemented: $exp")
      case fa: Expression.FieldAccess =>
        if (fa.fieldName.name == propertyName) {
          val binding = QuinePatternHelpers.reallyBadStringifier(fa)
          val ctx = QueryContext(
            Map(
              Symbol(binding) -> QuinePatternHelpers.patternValueToCypherValue(
                quineValueToPatternValue(value.deserialized.get),
              ),
            ),
          )
          quinePatternNode.states.get(toNotify).publish(ctx, me)
        }
      case _: Expression.IndexIntoArray => throw new QuinePatternUnimplementedException(s"Not yet implemented: $exp")
      case _: Expression.IsNull => throw new QuinePatternUnimplementedException(s"Not yet implemented: $exp")
      case _: Expression.CaseBlock => throw new QuinePatternUnimplementedException(s"Not yet implemented: $exp")
    }
}

case class SendAcrossEdgeState(
  quinePatternNode: QuinePatternQueryBehavior,
  toNotify: StandingQueryId,
  me: StandingQueryId,
  output: ActorRef,
) extends QuinePatternQueryState {
  override def publish(ctx: QueryContext, sqid: StandingQueryId): Unit = {
    output ! QuinePatternCommand.LazyQueryUpdate(toNotify, me, ctx)
    ()
  }
}

case class PublishToOutput(
  quinePatternNode: QuinePatternQueryBehavior,
  graph: QuinePatternOpsGraph,
  rsq: RunningStandingQuery,
) extends QuinePatternQueryState {
  override def publish(ctx: QueryContext, sqid: StandingQueryId): Unit = {
    val r = StandingQueryResult(
      true,
      ctx.environment.map { p =>
        p._1.name -> (p._2.eval(ctx)(graph.idProvider, Parameters.empty, LogConfig.permissive) match {
          case Left(value) => throw new QuinePatternUnimplementedException(s"Not yet implemented: $value")
          case Right(value) =>
            value match {
              case Expr.Node(_, _, properties) =>
                val thing: Map[String, QuineValue] = properties.map { p =>
                  val rv = toQuineValue(p._2) match {
                    case Left(value) => throw new QuinePatternUnimplementedException(s"Not yet implemented: $value")
                    case Right(value) => value
                  }
                  p._1.name -> rv
                }
                QuineValue(thing)
              case _ =>
                toQuineValue(value) match {
                  case Left(value) => throw new QuinePatternUnimplementedException(s"Not yet implemented: $value")
                  case Right(value) => value
                }
            }
        })
      },
    )

    rsq.offerResult(r)(LogConfig.permissive)
    ()
  }
}

case class EdgeState(
  quinePatternNode: QuinePatternQueryBehavior,
  edges: Set[HalfEdge],
  graph: QuinePatternOpsGraph,
  toNotify: StandingQueryId,
  me: StandingQueryId,
  edgeType: Symbol,
  edgeDirection: EdgeDirection,
  plan: LazyQueryPlan,
) extends QuinePatternQueryState {

  edges.foreach { halfEdge =>
    if (halfEdge.edgeType == edgeType && halfEdge.direction == edgeDirection) {
      val stqid = SpaceTimeQuineId(halfEdge.other, quinePatternNode.namespace, None)
      graph.relayTell(stqid, QuinePatternCommand.LoadLazyPlan(me, plan, quinePatternNode.self))
    }
  }

  override def publish(ctx: QueryContext, sqid: StandingQueryId): Unit = {
    val nextState = quinePatternNode.states.get(toNotify)
    nextState.publish(ctx, me)
  }

  def eventState(otherId: QuineId): Unit = {
    val stqid = SpaceTimeQuineId(otherId, quinePatternNode.namespace, None)
    graph.relayTell(stqid, QuinePatternCommand.LoadLazyPlan(me, plan, quinePatternNode.self))
  }
}
