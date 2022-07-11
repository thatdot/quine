package com.thatdot.quine.graph.behavior

import scala.compat.CompatBuildFrom.implicitlyBF
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.Success

import akka.actor.Actor
import akka.stream.scaladsl.Source
import akka.util.Timeout

import com.thatdot.quine.graph.NodeChangeEvent._
import com.thatdot.quine.graph._
import com.thatdot.quine.graph.messaging.BaseMessage.Done
import com.thatdot.quine.graph.messaging.LiteralMessage._
import com.thatdot.quine.graph.messaging.MergeMessage.MergeIntoNode
import com.thatdot.quine.graph.messaging.{QuineIdOps, QuineRefOps}
import com.thatdot.quine.model.{HalfEdge, PropertyValue, QuineValue}

trait LiteralCommandBehavior extends Actor with BaseNodeActor with QuineIdOps with QuineRefOps {

  def debugNodeInternalState(): Future[NodeInternalState]

  def getSqState(): SqStateResults

  protected def literalCommandBehavior(command: LiteralCommand): Unit = command match {
    case c: GetHalfEdgesCommand =>
      val matchingEdges: Iterator[HalfEdge] = c match {
        case GetHalfEdgesCommand(None, None, None, _, _) => edges.all
        case GetHalfEdgesCommand(None, None, Some(id), _, _) => edges.matching(id)
        case GetHalfEdgesCommand(None, Some(dir), None, _, _) => edges.matching(dir)
        case GetHalfEdgesCommand(None, Some(dir), Some(id), _, _) => edges.matching(dir, id)
        case GetHalfEdgesCommand(Some(edgeType), None, None, _, _) => edges.matching(edgeType)
        case GetHalfEdgesCommand(Some(edgeType), Some(dir), None, _, _) => edges.matching(edgeType, dir)
        case GetHalfEdgesCommand(Some(edgeType), None, Some(id), _, _) => edges.matching(edgeType, id)
        case GetHalfEdgesCommand(Some(edgeType), Some(dir), Some(id), _, _) => edges.matching(edgeType, dir, id)
      }
      c ?! Source(matchingEdges.map(HalfEdgeMessage).toList)

    case a @ AddHalfEdgeCommand(he, _) => a ?! processEvents(EdgeAdded(he) :: Nil)

    case r @ RemoveHalfEdgeCommand(he, _) => r ?! processEvents(EdgeRemoved(he) :: Nil)

    case g @ GetPropertiesCommand(_) =>
      val a = Source((properties - graph.labelsProperty).map({ case (key, value) =>
        PropertyMessage(Left((key, value)))
      }))
      val b = getLabels() match {
        case Some(labels) => Source(labels.toList).map(l => PropertyMessage(Right(l)))
        case None => Source.empty
      }
      g ?! (a concat b)

    case r @ GetPropertiesAndEdges(_) =>
      val a = Source(properties.toList).map(p => PropertyOrEdgeMessage(Left(p)))
      val b = Source(edges.all.toList).map(e => PropertyOrEdgeMessage(Right(e)))
      r ?! (a concat b)

    case s @ SetPropertyCommand(key, value, _) => s ?! processEvents(PropertySet(key, value) :: Nil)

    case r @ RemovePropertyCommand(key, _) =>
      r ?! properties.get(key).fold(Future.successful(Done)) { value =>
        processEvents(PropertyRemoved(key, value) :: Nil)
      }

    case d @ DeleteNodeCommand(shouldDeleteEdges, _) =>
      implicit val timeout = Timeout(5 seconds)

      if (!shouldDeleteEdges && edges.nonEmpty) {
        d ?! Future.successful(DeleteNodeCommand.Failed(edges.size))
      } else {
        // Clear properties, half edges, and request removal of the reciprocal half edges.
        val propertyRemovalEvents = properties.map { case (k, v) => PropertyRemoved(k, v) }
        val edgeRemovalEvents = edges.all.map(EdgeRemoved).toSeq
        val otherSidesRemoved =
          edgeRemovalEvents.map(ev => ev.edge.other.?(RemoveHalfEdgeCommand(ev.edge.reflect(qid), _)).flatten)
        // Confirmation future completes when every bit of the removal is done
        d ?! processEvents(edgeRemovalEvents ++ propertyRemovalEvents).flatMap(_ =>
          Future
            .sequence(otherSidesRemoved)(implicitlyBF, context.dispatcher)
            .map(_ => DeleteNodeCommand.Success)(context.dispatcher)
        )(context.dispatcher)
      }

    case MergeIntoNodeCommand(other, replyTo) => self ! MergeIntoNode(other, replyTo)

    case m: GetNodeId => m ?! QuineIdResponse(qid)

    case i @ IncrementProperty(propKey, incAmount, _) =>
      i ?! (properties.get(propKey).map(_.deserialized) match {
        case None =>
          val newValue = PropertyValue(QuineValue.Integer(incAmount))
          processEvents(PropertySet(propKey, newValue) :: Nil)
          IncrementProperty.Success(incAmount)
        case Some(Success(QuineValue.Integer(i))) =>
          val newValue = PropertyValue(QuineValue.Integer(i + incAmount))
          processEvents(PropertySet(propKey, newValue) :: Nil)
          IncrementProperty.Success(i + incAmount)
        case Some(Success(other)) => IncrementProperty.Failed(other)
        case _ => IncrementProperty.Failed(QuineValue.Null)
      })

    case s @ SetLabels(labels, _) => s ?! setLabels(labels)

    case l: LogInternalState => l ?! debugNodeInternalState()

    case m @ GetSqState(_) => m ?! getSqState()
  }
}
