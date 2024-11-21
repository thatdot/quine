package com.thatdot.quine.graph.behavior

import scala.collection.immutable.SortedMap
import scala.concurrent.{ExecutionContext, Future, Promise}

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.Actor
import org.apache.pekko.stream.scaladsl.Source

import com.thatdot.language.ast.{Expression, Identifier, Instruction, Value}
import com.thatdot.quine.graph.EdgeEvent.EdgeAdded
import com.thatdot.quine.graph.PropertyEvent.{PropertyRemoved, PropertySet}
import com.thatdot.quine.graph.cypher.{
  CypherAndQuineHelpers,
  Expr,
  ExpressionInterpreter,
  QueryContext,
  QuinePattern,
  QuinePatternUnimplementedException,
}
import com.thatdot.quine.graph.messaging.{QuineIdOps, QuineMessage, QuineRefOps}
import com.thatdot.quine.graph.{BaseNodeActor, QuinePatternOpsGraph, StandingQueryId, StandingQueryOpsGraph}
import com.thatdot.quine.model.{EdgeDirection, HalfEdge, QuineId}
import com.thatdot.quine.util.Log.LazySafeLogging

sealed trait QuinePatternCommand extends QuineMessage

object QuinePatternCommand {
  case class RegisterPattern(
    quinePattern: QuinePattern,
    pid: StandingQueryId,
    queryStream: Source[QueryContext, NotUsed],
  ) extends QuinePatternCommand

  case class LoadNode(boundTo: Identifier, env: QueryContext, result: Promise[QueryContext]) extends QuinePatternCommand
  case class SetLabels(labels: Set[Symbol], binding: Identifier, env: QueryContext, result: Promise[QueryContext])
      extends QuinePatternCommand
  case class PopulateNode(
    labels: Set[Symbol],
    maybeProperties: Option[Expression.MapLiteral],
    binding: Identifier,
    env: QueryContext,
    result: Promise[QueryContext],
  ) extends QuinePatternCommand
  case class AddEdge(
    edgeType: Symbol,
    edgeDirection: EdgeDirection,
    other: QuineId,
    env: QueryContext,
    result: Promise[QueryContext],
  ) extends QuinePatternCommand
  case class SetProperty(
    propertyName: Symbol,
    valueExp: Expression,
    binding: Identifier,
    env: QueryContext,
    result: Promise[QueryContext],
  ) extends QuinePatternCommand

  case class RunInstruction(
    instruction: Instruction,
    currentEnv: QueryContext,
    result: Promise[QueryContext],
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
    case QuinePatternCommand.LoadNode(binding, env, result) =>
      val updatedEnv = env + (binding.name -> Expr.Node(
        this.qid,
        this.getLabels().getOrElse(Set.empty[Symbol]),
        this.properties.map(p => p._1 -> Expr.fromQuineValue(p._2.deserialized.get)),
      ))
      result.success(updatedEnv)
    case QuinePatternCommand.SetLabels(labels, binding, env, result) =>
      val node = env(binding.name) match {
        case n: Expr.Node => n
        case _ => throw new QuinePatternUnimplementedException("Unexpected value")
      }
      implicit val ec: ExecutionContext = ExecutionContext.parasitic
      val future = for {
        _ <- this.setLabels(node.labels.union(labels)).map(_ => ())
        updated <- Future.apply(node.copy(labels = this.getLabels().getOrElse(Set.empty[Symbol])))
      } yield env + (binding.name -> updated)
      future.onComplete(tqc => result.success(tqc.get))
    case QuinePatternCommand.PopulateNode(labels, maybeProperties, binding, env, result) =>
      val node = env(binding.name) match {
        case n: Expr.Node => n
        case _ => throw new QuinePatternUnimplementedException("Unexpected value")
      }
      implicit val ec: ExecutionContext = ExecutionContext.parasitic
      val properties = maybeProperties
        .map(mapLiteral => ExpressionInterpreter.eval(mapLiteral, graph.idProvider, env))
        .getOrElse(Value.Map(SortedMap.empty[Symbol, Value])) match {
        case propertyMap: Value.Map => propertyMap
        case _ => throw new QuinePatternUnimplementedException("Unexpected value")
      }
      val events = properties.values.toList.map(p =>
        PropertySet(p._1, CypherAndQuineHelpers.patternValueToPropertyValue(p._2).get),
      )
      val future = for {
        _ <- this.setLabels(labels)
        _ <- processPropertyEvents(events)
      } yield {
        val updatedNode = node.copy(properties =
          properties.values.map(p => p._1 -> CypherAndQuineHelpers.patternValueToCypherValue(p._2)),
        )
        env + (binding.name -> updatedNode)
      }
      future.onComplete(tqc => result.success(tqc.get))
    case QuinePatternCommand.AddEdge(edgeType, edgeDirection, other, env, result) =>
      implicit val ec: ExecutionContext = ExecutionContext.parasitic
      val event = EdgeAdded(HalfEdge(edgeType, edgeDirection, other))
      val future = for {
        _ <- this.processEdgeEvent(event)
      } yield env
      future.onComplete(tqc => result.success(tqc.get))
    case QuinePatternCommand.SetProperty(propertyName, valueExp, binding, env, result) =>
      implicit val ec: ExecutionContext = ExecutionContext.parasitic
      val node = env(binding.name) match {
        case n: Expr.Node => n
        case _ => throw new QuinePatternUnimplementedException("Unexpected value")
      }
      val value = ExpressionInterpreter.eval(valueExp, graph.idProvider, env)
      val events = value match {
        case Value.Null =>
          properties.get(propertyName) match {
            case Some(v) => List(PropertyRemoved(propertyName, v))
            case None => Nil
          }
        case _ => List(PropertySet(propertyName, CypherAndQuineHelpers.patternValueToPropertyValue(value).get))
      }
      val future = for {
        _ <- this.processPropertyEvents(events)
      } yield {
        val updatedNode = node.copy(properties =
          node.properties + (propertyName -> CypherAndQuineHelpers.patternValueToCypherValue(value)),
        )
        env + (binding.name -> updatedNode)
      }
      future.onComplete(tqc => result.success(tqc.get))
    case unknownMsg => throw new QuinePatternUnimplementedException(s"Received unexpected message $unknownMsg")
  }
}
