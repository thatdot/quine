package com.thatdot.quine.graph.behavior

import scala.collection.immutable.SortedMap
import scala.concurrent.ExecutionContext

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.Status.Success
import org.apache.pekko.actor.{Actor, ActorRef}
import org.apache.pekko.stream.CompletionStrategy
import org.apache.pekko.stream.scaladsl.{Flow, Source}

import com.thatdot.language.ast.{Expression, Identifier, Value}
import com.thatdot.quine.graph.EdgeEvent.EdgeAdded
import com.thatdot.quine.graph.PropertyEvent.{PropertyRemoved, PropertySet}
import com.thatdot.quine.graph.cypher.{
  CypherAndQuineHelpers,
  ExpressionInterpreter,
  QueryContext,
  QueryPlan,
  QuinePatternUnimplementedException,
}
import com.thatdot.quine.graph.messaging.{QuineIdOps, QuineMessage, QuineRefOps}
import com.thatdot.quine.graph.{
  BaseNodeActor,
  NamespaceId,
  QuinePatternLoaderMessage,
  QuinePatternOpsGraph,
  StandingQueryId,
  StandingQueryOpsGraph,
}
import com.thatdot.quine.model.{EdgeDirection, HalfEdge}
import com.thatdot.quine.util.Log.LazySafeLogging

sealed trait QuinePatternCommand extends QuineMessage

object QuinePatternCommand {
  case object QuinePatternAck extends QuinePatternCommand

  case class RegisterPattern(
    quinePattern: QueryPlan,
    pid: StandingQueryId,
    queryStream: Source[QueryContext, NotUsed],
  ) extends QuinePatternCommand

  case class LoadPlan(
    queryPlan: List[QueryPlan],
    namespaceId: NamespaceId,
    product: List[QueryPlan],
    standingQueryId: StandingQueryId,
    parameters: Map[Symbol, com.thatdot.quine.graph.cypher.Value],
    output: Source[QueryContext, NotUsed],
  ) extends QuinePatternCommand

  //Local execution instructions
  case class LoadNode(binding: Identifier, ctx: QueryContext, output: ActorRef) extends QuinePatternCommand
  case class SetProperty(
    propertyName: Symbol,
    to: Expression,
    ctx: QueryContext,
    parameters: Map[Symbol, com.thatdot.quine.graph.cypher.Value],
    output: ActorRef,
  ) extends QuinePatternCommand
  case class SetLabels(labels: Set[Symbol], ctx: QueryContext, output: ActorRef) extends QuinePatternCommand
  case class CreateEdge(
    from: Expression,
    to: Expression,
    direction: EdgeDirection,
    label: Symbol,
    ctx: QueryContext,
    parameters: Map[Symbol, com.thatdot.quine.graph.cypher.Value],
    output: ActorRef,
  ) extends QuinePatternCommand
}

trait QuinePatternQueryBehavior
    extends Actor
    with BaseNodeActor
    with QuineIdOps
    with QuineRefOps
    with StandingQueryBehavior
    with LazySafeLogging {

  protected def graph: QuinePatternOpsGraph with StandingQueryOpsGraph

  def quinePatternQueryBehavior(command: QuinePatternCommand): Unit = command match {
    case QuinePatternCommand.QuinePatternAck => ()
    case QuinePatternCommand.LoadNode(binding, ctx, output) =>
      val nodeVal = Value.Node(
        this.qid,
        this.getLabels().getOrElse(Set.empty),
        Value.Map(
          SortedMap.from(this.properties.map(p => p._1 -> CypherAndQuineHelpers.propertyValueToPatternValue(p._2))),
        ),
      )
      val newCtx = ctx + (binding.name -> CypherAndQuineHelpers.patternValueToCypherValue(nodeVal))
      output ! newCtx
      output ! Success("Done")
    case QuinePatternCommand.SetLabels(labels, ctx, output) =>
      setLabels(labels).onComplete { _ =>
        output ! ctx
        output ! Success("Done")
      }(ExecutionContext.parasitic)
    case QuinePatternCommand.SetProperty(propertyName, to, ctx, parameters, output) =>
      //TODO This needs to update the image in the query context, but we can't do that yet
      val event = ExpressionInterpreter.eval(to, graph.idProvider, ctx, parameters) match {
        case Value.Null =>
          // remove the property
          properties.get(propertyName) match {
            case Some(oldValue) => Some(PropertyRemoved(propertyName, oldValue))
            case None =>
              // there already was no property at query.key -- no-op
              None
          }
        case value => Some(PropertySet(propertyName, CypherAndQuineHelpers.patternValueToPropertyValue(value).get))
      }
      processPropertyEvents(event.toList).onComplete { _ =>
        output ! ctx
        output ! Success("Done")
      }(ExecutionContext.parasitic)
    case QuinePatternCommand.CreateEdge(from, to, direction, label, ctx, parameters, output) =>
      val toVal = ExpressionInterpreter.eval(to, graph.idProvider, ctx, parameters) match {
        case Value.NodeId(id) => id
        case _ => ???
      }
      val halfEdge = HalfEdge(label, direction, toVal)
      val event = EdgeAdded(halfEdge)
      processEdgeEvent(event).onComplete { _ =>
        output ! ctx
        output ! Success("Done")
      }(ExecutionContext.parasitic)
    case QuinePatternCommand.LoadPlan(queryPlan, namespace, product, sqid, parameters, output) =>
      val stream = queryPlan.foldLeft(Flow[QueryContext].map(identity)) { (flow, step) =>
        step match {
          case QueryPlan.UnitPlan => flow
          case QueryPlan.LoadNode(binding) =>
            flow.via(Flow[QueryContext].flatMapConcat { ctx =>
              Source
                .actorRefWithBackpressure(
                  ackMessage = QuinePatternCommand.QuinePatternAck,
                  completionMatcher = { case _: Success =>
                    CompletionStrategy.immediately
                  },
                  failureMatcher = PartialFunction.empty,
                )
                .mapMaterializedValue { ref =>
                  self ! QuinePatternCommand.LoadNode(binding, ctx, ref)
                }
            })
          case QueryPlan.Effect(com.thatdot.cypher.ast.Effect.SetProperty(_, property, value)) =>
            flow.via(Flow[QueryContext].flatMapConcat { ctx =>
              Source
                .actorRefWithBackpressure(
                  ackMessage = QuinePatternCommand.QuinePatternAck,
                  completionMatcher = { case _: Success =>
                    CompletionStrategy.immediately
                  },
                  failureMatcher = PartialFunction.empty,
                )
                .mapMaterializedValue { ref =>
                  self ! QuinePatternCommand.SetProperty(property.fieldName.name, value, ctx, parameters, ref)
                }
            })
          case QueryPlan.Effect(com.thatdot.cypher.ast.Effect.SetLabel(_, _, labels)) =>
            flow.via(Flow[QueryContext].flatMapConcat { ctx =>
              Source
                .actorRefWithBackpressure(
                  ackMessage = QuinePatternCommand.QuinePatternAck,
                  completionMatcher = { case _: Success =>
                    CompletionStrategy.immediately
                  },
                  failureMatcher = PartialFunction.empty,
                )
                .mapMaterializedValue { ref =>
                  self ! QuinePatternCommand.SetLabels(labels, ctx, ref)
                }
            })
          case QueryPlan.CreateHalfEdge(from, to, direction, label) =>
            flow.via(Flow[QueryContext].flatMapConcat { ctx =>
              Source
                .actorRefWithBackpressure(
                  ackMessage = QuinePatternCommand.QuinePatternAck,
                  completionMatcher = { case _: Success =>
                    CompletionStrategy.immediately
                  },
                  failureMatcher = PartialFunction.empty,
                )
                .mapMaterializedValue { ref =>
                  self ! QuinePatternCommand.CreateEdge(from, to, direction, label, ctx, parameters, ref)
                }
            })
          case _ => throw new QuinePatternUnimplementedException(s"Not yet implemented: $step")
        }
      }
      val finalStream = output.flatMapConcat(ctx => Source.single(ctx).via(stream))
      if (product.isEmpty) graph.getLoader ! QuinePatternLoaderMessage.MergeIngestStream(sqid, finalStream)
      else graph.getLoader ! QuinePatternLoaderMessage.LoadMore(namespace, sqid, product, parameters, finalStream)
    case _ => ???
  }
}
