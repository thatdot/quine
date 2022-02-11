package com.thatdot.quine.graph.behavior

import scala.concurrent.Future

import akka.actor.Actor

import com.typesafe.scalalogging.LazyLogging

import com.thatdot.quine.graph.NodeChangeEvent.{EdgeAdded, EdgeRemoved, MergedHere, MergedIntoOther, PropertySet}
import com.thatdot.quine.graph.messaging.BaseMessage.Done
import com.thatdot.quine.graph.messaging.MergeMessage.{
  DoNotForward,
  MergeAdd,
  MergeAddContext,
  MergeCommand,
  MergeIntoNode,
  RedirectEdges
}
import com.thatdot.quine.graph.messaging.{QuineIdAtTime, QuineIdOps, QuineMessage, QuineRefOps}
import com.thatdot.quine.graph.{BaseGraph, BaseNodeActor, NodeControlMessage}
import com.thatdot.quine.model.QuineId

trait MergeNodeBehavior extends Actor with BaseNodeActor with QuineIdOps with QuineRefOps {

  protected def mergeBehavior(command: MergeCommand): Unit = command match {
    case msg @ MergeIntoNode(otherNode, _) =>
      if (otherNode != qid) {
        val edgeSet = edges.toSet
        otherNode ! MergeAdd(properties, edgeSet, MergeAddContext(qid, otherNode))
        edgeSet.foreach(e => e.other ! RedirectEdges(qid, otherNode))
        processEvent(MergedIntoOther(otherNode))
      }
      msg ?! Future.successful(Done)

    case MergeAdd(ps, es, cntxt) =>
      processEvent(MergedHere(cntxt.mergedFrom))
      ps.foreach { case (k, v) => processEvent(PropertySet(k, v)) }
      es.foreach(e => processEvent(EdgeAdded(e)))
    // TODO: Consider whether these events should be processed in bulk. See old `MergeAdd` for example.

    case RedirectEdges(from, to) =>
      val oldEdges = edges.matching(from).toVector
      val newEdges = oldEdges.map(_.copy(other = to))
      val eventList = oldEdges.map(e => EdgeRemoved(e)) ++ newEdges.map(e => EdgeAdded(e))
      eventList.foreach(processEvent(_))
    // TODO: Consider whether these events should be processed in bulk. See old `RedirectEdges` for example.
  }
}

object MergeNodeBehavior extends LazyLogging {

  private def temporarilyProcessWithOriginalBehavior(
    msg: Any,
    thisNodeActor: BaseNodeActor,
    graph: BaseGraph,
    mergedIntoOther: QuineId
  ) = {
    thisNodeActor.context.unbecome()
    thisNodeActor.receive(msg)
    thisNodeActor.context.become(this.mergedMessageHandling(thisNodeActor, graph, mergedIntoOther), discardOld = false)
  }

  /** This should only be called synchronously inside of a node when applying the MergedIntoOther NodeChangeEvent
    * or when restoring from a snapshot
    *
    * @param thisNodeActor
    * @param mergedIntoOther
    * @return
    */
  def mergedMessageHandling(
    thisNodeActor: BaseNodeActor,
    graph: BaseGraph,
    mergedIntoOther: QuineId
  ): PartialFunction[Any, Unit] = {
    // TODO: If an edge is trying to be added to this ID specifically after it has been merged, this node should relay the edge creation, but it should also send instruction back to the node on the originating end of the edge to update their edge to point to the new ID.
    // TODO: It should probably also rewrite IDs in a DomainGraphBranch which have been identified to this node as pointing to the new ID.
    // TODO: Handle case of a node merged into an already-merged node. Note: there is a message delivery race condition to consider.
    // TODO: merge standing query v.4 states?
    case DoNotForward(msg) =>
      temporarilyProcessWithOriginalBehavior(msg, thisNodeActor, graph, mergedIntoOther)
    case msg: NodeControlMessage =>
      logger.info(
        s"Merged node: ${thisNodeActor.idProvider.qidToPrettyString(thisNodeActor.qid)} received control message: $msg"
      )
      temporarilyProcessWithOriginalBehavior(msg, thisNodeActor, graph, mergedIntoOther)
    case StashedMessage(msg) =>
      this.mergedMessageHandling(thisNodeActor, graph, mergedIntoOther)(msg)
    case m: QuineMessage =>
      graph.relayTell(QuineIdAtTime(mergedIntoOther, thisNodeActor.atTime), m, thisNodeActor.sender())
    case m =>
      logger.error(
        s"Received an unexpected message: $m on a merged node: ${thisNodeActor.idProvider.qidToPrettyString(thisNodeActor.qid)}"
      )
  }
}

// TODO: Unmerge?
