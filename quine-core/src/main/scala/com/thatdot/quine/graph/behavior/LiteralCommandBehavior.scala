package com.thatdot.quine.graph.behavior

import scala.annotation.nowarn
import scala.compat.CompatBuildFrom.implicitlyBF
import scala.compat.ExecutionContexts
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.Success

import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.Timeout

import com.thatdot.quine.graph.EdgeEvent.{EdgeAdded, EdgeRemoved}
import com.thatdot.quine.graph.PropertyEvent.{PropertyRemoved, PropertySet}
import com.thatdot.quine.graph._
import com.thatdot.quine.graph.messaging.BaseMessage.Done
import com.thatdot.quine.graph.messaging.LiteralMessage._
import com.thatdot.quine.graph.messaging.{QuineIdOps, QuineRefOps}
import com.thatdot.quine.model.{HalfEdge, PropertyValue, QuineValue}

trait LiteralCommandBehavior extends BaseNodeActor with QuineIdOps with QuineRefOps {

  def debugNodeInternalState(): Future[NodeInternalState]

  def getNodeHashCode(): GraphNodeHashCode

  def getSqState(): SqStateResults

  @nowarn("msg=class IncrementProperty in object LiteralMessage is deprecated")
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

    case a @ AddHalfEdgeCommand(he, _) => a ?! processEdgeEvents(EdgeAdded(he) :: Nil)

    case r @ RemoveHalfEdgeCommand(he, _) => r ?! processEdgeEvents(EdgeRemoved(he) :: Nil)

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

    case s @ SetPropertyCommand(key, value, _) => s ?! processPropertyEvents(PropertySet(key, value) :: Nil)

    case r @ RemovePropertyCommand(key, _) =>
      r ?! properties.get(key).fold(Future.successful(Done)) { value =>
        processPropertyEvents(PropertyRemoved(key, value) :: Nil)
      }

    case d @ DeleteNodeCommand(shouldDeleteEdges, _) =>
      implicit val timeout = Timeout(5 seconds)

      if (!shouldDeleteEdges && edges.nonEmpty) {
        d ?! Future.successful(DeleteNodeCommand.Failed(edges.size))
      } else {
        // Clear properties, half edges, and request removal of the reciprocal half edges.
        val propertyRemovalEvents = properties.map { case (k, v) => PropertyRemoved(k, v) }
        val allEdges = edges.all
        val edgeRemovalEvents = allEdges.map(EdgeRemoved).toList
        val otherSidesRemoved = Future.traverse(allEdges)(edge =>
          edge.other.?(RemoveHalfEdgeCommand(edge.reflect(qid), _)).flatten
        )(implicitlyBF, context.dispatcher)

        // Confirmation future completes when every bit of the removal is done
        d ?! processEdgeEvents(edgeRemovalEvents)
          .zip(processPropertyEvents(propertyRemovalEvents.toList))
          .zip(otherSidesRemoved)
          .map(_ => DeleteNodeCommand.Success)(ExecutionContexts.parasitic)
      }

    case ip @ IncrementProperty(propKey, incAmount, _) =>
      ip ?! (properties.get(propKey).map(_.deserialized) match {
        case None =>
          val newValue = PropertyValue(QuineValue.Integer(incAmount))
          processPropertyEvents(PropertySet(propKey, newValue) :: Nil)
          IncrementProperty.Success(incAmount)
        case Some(Success(QuineValue.Integer(i))) =>
          val newValue = PropertyValue(QuineValue.Integer(i + incAmount))
          processPropertyEvents(PropertySet(propKey, newValue) :: Nil)
          IncrementProperty.Success(i + incAmount)
        case Some(Success(other)) => IncrementProperty.Failed(other)
        case _ => IncrementProperty.Failed(QuineValue.Null)
      })

    case msg @ AddToAtomic.Int(propKey, incAmount, _) =>
      msg ?! (properties.get(propKey).map(_.deserialized) match {
        case None =>
          processPropertyEvents(PropertySet(propKey, PropertyValue(incAmount)) :: Nil)
          msg.success(incAmount)
        case Some(Success(QuineValue.Integer(prevValue))) =>
          val newValue = QuineValue.Integer(prevValue + incAmount.long)
          processPropertyEvents(PropertySet(propKey, PropertyValue(newValue)) :: Nil)
          msg.success(newValue)
        case Some(Success(other)) => msg.failure(other)
        case _ => msg.failure(QuineValue.Null)
      })

    case msg @ AddToAtomic.Float(propKey, incAmount, _) =>
      msg ?! (properties.get(propKey).map(_.deserialized) match {
        case None =>
          processPropertyEvents(PropertySet(propKey, PropertyValue(incAmount)) :: Nil)
          msg.success(incAmount)
        case Some(Success(QuineValue.Floating(prevValue))) =>
          val newValue = QuineValue.Floating(prevValue + incAmount.double)
          processPropertyEvents(PropertySet(propKey, PropertyValue(newValue)) :: Nil)
          msg.success(newValue)
        case Some(Success(other)) => msg.failure(other)
        case _ => msg.failure(QuineValue.Null)
      })

    case msg @ AddToAtomic.Set(propKey, QuineValue.List(newElems), _) =>
      msg ?! (properties.get(propKey).map(_.deserialized) match {
        case None =>
          val newSet = QuineValue.List(newElems.distinct)
          processPropertyEvents(PropertySet(propKey, PropertyValue(newSet)) :: Nil)
          msg.success(newSet)
        case Some(Success(QuineValue.List(oldElems))) =>
          // Set behavior: newElem is not yet in ths list stored at this key, so update the list
          val newElementsDeduplicated = newElems.filterNot(oldElems.contains).distinct
          val updatedSet = QuineValue.List(oldElems ++ newElementsDeduplicated)
          // peephole optimization: if the sets are identical, no need to wait until processEvents runs to discover that
          if (newElementsDeduplicated.nonEmpty) {
            processPropertyEvents(PropertySet(propKey, PropertyValue(updatedSet)) :: Nil)
          }
          msg.success(updatedSet)
        case Some(Success(other)) => msg.failure(other)
        case _ => msg.failure(QuineValue.Null)
      })

    case s @ SetLabels(labels, _) => s ?! setLabels(labels)

    case l: LogInternalState => l ?! debugNodeInternalState()

    case h: GetNodeHashCode => h ?! getNodeHashCode()

    case m @ GetSqState(_) => m ?! getSqState()
  }
}
