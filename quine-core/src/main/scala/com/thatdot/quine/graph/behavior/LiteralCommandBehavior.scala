package com.thatdot.quine.graph.behavior

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.Success

import akka.actor.Actor
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
      c ?! HalfEdgeSet(c.withLimit.fold(matchingEdges)(matchingEdges.take).toSet)

    case a @ AddHalfEdgeCommand(he, _) => a ?! processEvent(EdgeAdded(he))

    case r @ RemoveHalfEdgeCommand(he, _) => r ?! processEvent(EdgeRemoved(he))

    case g @ GetRawPropertiesCommand(_) => g ?! RawPropertiesMap(getLabels(), properties - graph.labelsProperty)

    case r @ GetPropertiesAndEdges(_) =>
      r ?! PropertiesAndEdges(qid, properties, edges.toSet)

    case s @ SetPropertyCommand(key, value, _) => s ?! processEvent(PropertySet(key, value))

    case r @ RemovePropertyCommand(key, _) =>
      r ?! properties.get(key).fold(Future.successful(Done)) { value =>
        processEvent(PropertyRemoved(key, value))
      }

    case d @ DeleteNodeCommand(deleteEdges, _) =>
      implicit val ec = context.dispatcher
      implicit val timeout = Timeout(5 seconds)

      if (!deleteEdges && edges.nonEmpty) {
        d ?! Future.successful(DeleteNodeCommand.Failed(edges.size))
      } else {

        // Clear edges, properties, and property collections
        val edgesRemoved = Future.traverse(edges.all) { (halfEdge: HalfEdge) =>
          val thisHalfRemoved = processEvent(EdgeRemoved(halfEdge))
          val otherHalfRemoved = halfEdge.other ? (RemoveHalfEdgeCommand(halfEdge.reflect(qid), _))
          thisHalfRemoved.zip(otherHalfRemoved)
        }
        val propertiesRemoved = Future.traverse(properties.toSeq) { case (key, value) =>
          processEvent(PropertyRemoved(key, value))
        }

        // Confirmation future completes when every bit of the removal is done
        d ?! (edgesRemoved zip propertiesRemoved).map(_ => DeleteNodeCommand.Success)
      }

    case MergeIntoNodeCommand(other, replyTo) => self ! MergeIntoNode(other, replyTo)

    case m: GetNodeId => m ?! QuineIdResponse(qid)

    case i @ IncrementProperty(propKey, incAmount, _) =>
      i ?! (properties.get(propKey).map(_.deserialized) match {
        case None =>
          val newValue = PropertyValue(QuineValue.Integer(incAmount))
          processEvent(PropertySet(propKey, newValue))
          IncrementProperty.Success(incAmount)
        case Some(Success(QuineValue.Integer(i))) =>
          val newValue = PropertyValue(QuineValue.Integer(i + incAmount))
          processEvent(PropertySet(propKey, newValue))
          IncrementProperty.Success(i + incAmount)
        case Some(Success(other)) => IncrementProperty.Failed(other)
        case _ => IncrementProperty.Failed(QuineValue.Null)
      })

    case s @ SetLabels(labels, _) =>
      s ?! setLabels(labels)

    case l: LogInternalState =>
      l ?! debugNodeInternalState()
  }
}
