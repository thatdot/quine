package com.thatdot.quine.graph.behavior

import java.util.concurrent.ConcurrentHashMap

import scala.collection.immutable.SortedMap
import scala.concurrent.ExecutionContext

import org.apache.pekko.actor.Status.Success
import org.apache.pekko.actor.{Actor, ActorRef}

import com.thatdot.common.logging.Log.LazySafeLogging
import com.thatdot.common.quineid.QuineId
import com.thatdot.language.ast.{Expression, Identifier, Value}
import com.thatdot.quine.graph.EdgeEvent.EdgeAdded
import com.thatdot.quine.graph.PropertyEvent.{PropertyRemoved, PropertySet}
import com.thatdot.quine.graph.cypher.QueryContext
import com.thatdot.quine.graph.cypher.quinepattern.CypherAndQuineHelpers.quineValueToPatternValue
import com.thatdot.quine.graph.cypher.quinepattern.LazyQuinePatternQueryPlanner.LazyQueryPlan
import com.thatdot.quine.graph.cypher.quinepattern.{
  CypherAndQuineHelpers,
  EdgeState,
  FilterState,
  ProductState,
  PublishToOutput,
  QuinePatternExpressionInterpreter,
  QuinePatternHelpers,
  QuinePatternQueryState,
  QuinePatternUnimplementedException,
  SendAcrossEdgeState,
  WatchState,
}
import com.thatdot.quine.graph.messaging.{QuineIdOps, QuineMessage, QuineRefOps}
import com.thatdot.quine.graph.quinepattern.QuinePatternOpsGraph
import com.thatdot.quine.graph.{BaseNodeActor, StandingQueryId, StandingQueryOpsGraph}
import com.thatdot.quine.model.{EdgeDirection, HalfEdge, PropertyValue, QuineValue}

/** `QuinePatternCommand` represents commands or instructions used within the Quine graph processing system.
  * This sealed trait defines a hierarchy of messages that are utilized for managing and interacting
  * with standing queries, node updates, property modifications, and edge creation within the system.
  *
  * It extends `QuineMessage`, enabling these commands to be relayable across the Quine graph
  * using specific mechanisms such as `relayTell` or `relayAsk`.
  *
  * The implementations of this trait include commands for:
  * - Acknowledgements and control such as stopping a pattern.
  * - Loading and managing lazy standing query plans.
  * - Updating lazy query relationships.
  * - Local node operations like loading nodes, modifying properties, setting labels, or creating edges.
  * - Performing atomic operations such as property increments.
  */
sealed trait QuinePatternCommand extends QuineMessage

object QuinePatternCommand {
  case object QuinePatternAck extends QuinePatternCommand

  case object QuinePatternStop extends QuinePatternCommand

  case class LoadLazyPlan(sqid: StandingQueryId, plan: LazyQueryPlan, output: ActorRef) extends QuinePatternCommand

  case class LazyQueryUpdate(toNotify: StandingQueryId, from: StandingQueryId, ctx: QueryContext)
      extends QuinePatternCommand

  //Local execution instructions
  case class LoadNode(binding: Identifier, ctx: QueryContext, output: ActorRef) extends QuinePatternCommand
  case class SetProperty(
    of: Identifier,
    propertyName: Symbol,
    to: Expression,
    ctx: QueryContext,
    parameters: Map[Symbol, com.thatdot.quine.graph.cypher.Value],
    output: ActorRef,
  ) extends QuinePatternCommand
  case class SetProperties(props: Map[Symbol, Value], ctx: QueryContext, output: ActorRef) extends QuinePatternCommand
  case class SetLabels(labels: Set[Symbol], ctx: QueryContext, output: ActorRef) extends QuinePatternCommand
  case class CreateEdge(
    to: QuineId,
    direction: EdgeDirection,
    label: Symbol,
    ctx: QueryContext,
    output: ActorRef,
  ) extends QuinePatternCommand
  case class AtomicIncrement(propertyExpr: String, by: Int, binding: Symbol, ctx: QueryContext, output: ActorRef)
      extends QuinePatternCommand
}

/** A trait that defines the behavior for Quine's pattern-based query system. This trait implements
  * functionality for managing pattern queries, handling various query commands, and maintaining state.
  * It builds upon foundational classes and traits such as `Actor`, `BaseNodeActor`, `QuineIdOps`,
  * `QuineRefOps`, and `StandingQueryBehavior`.
  *
  * The trait provides mechanisms for:
  * - Creating and managing pattern query states.
  * - Loading and executing lazy queries.
  * - Responding to various commands related to pattern queries, such as stopping queries, updating properties,
  * setting labels, creating edges, and publishing state updates.
  *
  * It is meant to facilitate interactions with Quine's node graph and integrates with its underlying
  * subsystems for handling sophisticated graph operations and query behaviors.
  */
trait QuinePatternQueryBehavior
    extends Actor
    with BaseNodeActor
    with QuineIdOps
    with QuineRefOps
    with StandingQueryBehavior
    with LazySafeLogging {

  protected def graph: QuinePatternOpsGraph with StandingQueryOpsGraph

  val states: ConcurrentHashMap[StandingQueryId, QuinePatternQueryState] =
    new ConcurrentHashMap[StandingQueryId, QuinePatternQueryState]()

  /** Creates and sets up the states associated with a given query plan within the specified context.
    *
    * @param queryPlan the query plan to create states for, defining the structure and flow of operations
    * @param sqid      the unique identifier of the standing query within the current context
    * @return the unique identifier for the parent standing query ID in the context of this plan
    * @throws QuinePatternUnimplementedException if the query plan contains unimplemented cases
    */
  def createQuinePatternQueryStatesFor(queryPlan: LazyQueryPlan, sqid: StandingQueryId): StandingQueryId =
    queryPlan match {
      case LazyQueryPlan.Product(plans) =>
        val newSqid = StandingQueryId.fresh()
        states.put(newSqid, ProductState(this, sqid, newSqid))
        plans.foreach(p => createQuinePatternQueryStatesFor(p, newSqid))
        sqid
      case LazyQueryPlan.FilterMap(filter, projections, isDistinct, plan) =>
        val newSqid = StandingQueryId.fresh()
        states.put(newSqid, FilterState(this, sqid, newSqid, filter, projections, isDistinct))
        createQuinePatternQueryStatesFor(plan, newSqid)
      case LazyQueryPlan.Watch(exp) =>
        val newSqid = StandingQueryId.fresh()
        states.put(newSqid, WatchState(this, this.properties, sqid, newSqid, exp))
        newSqid
      case LazyQueryPlan.WatchEdge(label, direction, plan) =>
        val newSqid = StandingQueryId.fresh()
        states.put(
          newSqid,
          EdgeState(
            this,
            this.edges.toSet,
            this.graph,
            sqid,
            newSqid,
            label,
            QuinePatternHelpers.directionToEdgeDirection(direction),
            plan,
          ),
        )
        newSqid
      case _: LazyQueryPlan.DoEffect =>
        throw new QuinePatternUnimplementedException(s"Not yet implemented: $queryPlan")
      case _: LazyQueryPlan.ReAnchor =>
        throw new QuinePatternUnimplementedException(s"Not yet implemented: $queryPlan")
      case _: LazyQueryPlan.Unwind => throw new QuinePatternUnimplementedException(s"Not yet implemented: $queryPlan")
    }

  def loadLazyQuery(sqid: StandingQueryId, plan: LazyQueryPlan, output: ActorRef): Unit = {
    val newSqid = StandingQueryId.fresh()
    states.put(newSqid, SendAcrossEdgeState(this, sqid, newSqid, output))
    createQuinePatternQueryStatesFor(plan, newSqid)
    ()
  }

  def loadQuinePatternLazyQueries(): Unit =
    graph.quinePatternLazyQueries.foreach { case (sqid, lazyQuery) =>
      graph.standingQueries(namespace).foreach { sqog =>
        sqog.runningStandingQuery(sqid).foreach { runningSq =>
          states.put(sqid, PublishToOutput(this, this.graph, runningSq))

          createQuinePatternQueryStatesFor(lazyQuery.plan, sqid)
        }
      }
    }

  def quinePatternQueryBehavior(command: QuinePatternCommand): Unit = command match {
    case QuinePatternCommand.QuinePatternStop => states.clear()
    case QuinePatternCommand.QuinePatternAck => ()
    case QuinePatternCommand.AtomicIncrement(propName, by, binding, ctx, output) =>
      val prop = properties.get(Symbol(propName))
      val event = prop match {
        case None => PropertySet(Symbol(propName), PropertyValue(by))
        case Some(pval) =>
          pval.deserialized.get match {
            case QuineValue.Integer(long) => PropertySet(Symbol(propName), PropertyValue(long + by))
            case other =>
              throw new QuinePatternUnimplementedException(
                s"Not yet implemented: increment counter for values like $other",
              )
          }
      }
      processPropertyEvents(List(event)).onComplete { _ =>
        val newCtx = ctx + (binding -> QuinePatternHelpers.patternValueToCypherValue(
          quineValueToPatternValue(properties(Symbol(propName)).deserialized.get),
        ))
        output ! newCtx
        output ! Success("Done")
      }(ExecutionContext.parasitic)
    case QuinePatternCommand.LazyQueryUpdate(toNotify, from, ctx) =>
      val state = states.get(toNotify)
      state.publish(ctx, from)
    case QuinePatternCommand.LoadNode(binding, ctx, output) =>
      val nodeVal = Value.Node(
        this.qid,
        this.getLabels().getOrElse(Set.empty),
        Value.Map(
          SortedMap.from(this.properties.map(p => p._1 -> CypherAndQuineHelpers.propertyValueToPatternValue(p._2))),
        ),
      )
      val newCtx = ctx + (binding.name -> QuinePatternHelpers.patternValueToCypherValue(nodeVal))
      output ! newCtx
      output ! Success("Done")
    case QuinePatternCommand.SetLabels(labels, ctx, output) =>
      setLabels(labels).onComplete { _ =>
        output ! ctx
        output ! Success("Done")
      }(ExecutionContext.parasitic)
    case QuinePatternCommand.SetProperty(_, propertyName, to, ctx, parameters, output) =>
      val event = QuinePatternExpressionInterpreter.eval(to, graph.idProvider, ctx, parameters) match {
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
    case QuinePatternCommand.SetProperties(props, ctx, output) =>
      val events = props.flatMap { case (k, v) =>
        v match {
          case Value.Null =>
            properties.get(k) match {
              case Some(oldValue) => List(PropertyRemoved(k, oldValue))
              case None => Nil
            }
          case value => List(PropertySet(k, CypherAndQuineHelpers.patternValueToPropertyValue(value).get))
        }
      }.toList
      processPropertyEvents(events).onComplete { _ =>
        output ! ctx
        output ! Success("Done")
      }(ExecutionContext.parasitic)
    case QuinePatternCommand.CreateEdge(to, direction, label, ctx, output) =>
      val halfEdge = HalfEdge(label, direction, to)
      val event = EdgeAdded(halfEdge)
      processEdgeEvent(event).onComplete { _ =>
        states.values.forEach {
          case es: EdgeState => if (es.edgeType == label && es.edgeDirection == direction) es.eventState(to) else ()
          case _ => ()
        }
        output ! ctx
        output ! Success("Done")
      }(ExecutionContext.parasitic)
    case QuinePatternCommand.LoadLazyPlan(sqid, plan, output) => loadLazyQuery(sqid, plan, output)
    case _ => throw new QuinePatternUnimplementedException(s"Not yet implemented: $command")
  }
}
