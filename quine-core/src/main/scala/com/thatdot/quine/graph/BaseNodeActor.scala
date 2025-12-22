package com.thatdot.quine.graph

import scala.concurrent.Future

import com.thatdot.quine.graph.edges.EdgeProcessor
import com.thatdot.quine.graph.messaging.BaseMessage.Done
import com.thatdot.quine.model.{PropertyValue, QuineValue}

/** Basic operations that can be issued on a node actor */
trait BaseNodeActor extends BaseNodeActorView {

  /** The following four methods are for applying mutations to the properties and half-edges of the node.
    * They both write the event to the journal, and apply it to the node's current state.
    * The reason there's two of each is to allow the single-event variant to optionally take an external time
    * for the event (I'd think we could support that on the collection case, too - just as long as you promise the
    * atTimeOverride function you pass in returns a new number each time).
    *
    * === Thread safety ===
    *
    * This function modifies NodeActor state on the current thread, hence is ''NOT'' thread safe. If processing
    * multiple events, call this function once and pass the events in as a sequence.
    * Events which do not modify the NodeActor state (because they are redundant with existing state) will be ignored.
    *
    * {{{
    * val l: List[NodeChangeEvent] = ...
    *
    * // Good
    * processEvents(l)
    *
    * // Bad: `processEvents` calls are sequential and are not running on the actor thread
    * l.foldLeft(Future.successful(Done))((acc, b) => acc.flatMap(_ => processEvents(b :: Nil)))
    *
    * // Bad: `processEvents` calls are concurrent, potentially corrupting in-memory state
    * Future.sequence(l.par.map(e => processEvents(e :: Nil)))
    * }}}
    *
    * === Redundant events ===
    *
    * If an event won't have an effect on node state (e.g. it is removing and edge that doesn't exist, or is setting to
    * a property a value that is already the property's value), it is filtered from the incoming list. If the list is or
    * becomes empty from this, the function short-circuits and returns a successful
    * future without ever saving the effect-less event(s) to the node's journal.
    *
    * @param event a single event that is being applied individually
    * @param atTimeOverride overrides the time at which the event occurs (take great care if using this!)
    * @return future tracking completion
    */

  protected def processPropertyEvent(
    event: PropertyEvent,
    atTimeOverride: Option[EventTime] = None,
  ): Future[Done.type]

  protected def processPropertyEvents(
    events: List[PropertyEvent],
  ): Future[Done.type]

  protected def processEdgeEvent(
    event: EdgeEvent,
    atTimeOverride: Option[EventTime] = None,
  ): Future[Done.type]

  // The only place this is called with a collection is when deleting a node.
  protected def processEdgeEvents(
    events: List[EdgeEvent],
  ): Future[Done.type]

  /** Set the labels on the node
    *
    * @param labels new labels for this node (overwriting previously set labels)
    * @return future signaling when the write is done
    */
  protected def setLabels(labels: Set[Symbol]): Future[Done.type] = {
    val propertyEvent = if (labels.isEmpty) {
      // When all labels are removed, remove the property entirely rather than setting to empty list.
      // This ensures properties.isEmpty returns true when the node has no labels.
      properties.get(graph.labelsProperty) match {
        case Some(oldValue) => PropertyEvent.PropertyRemoved(graph.labelsProperty, oldValue)
        case None => return Future.successful(Done) // Already no labels, nothing to do
      }
    } else {
      val labelsValue = QuineValue.List(labels.map(_.name).toVector.sorted.map(QuineValue.Str))
      PropertyEvent.PropertySet(graph.labelsProperty, PropertyValue(labelsValue))
    }
    processPropertyEvent(propertyEvent)
  }

  protected def edges: EdgeProcessor

  /** Record that some update pertinent to snapshots has occurred */
  protected def updateRelevantToSnapshotOccurred(): Unit

  /** Serializes a snapshot and also resets the `latestUpdateAfterSnapshot` */
  protected def toSnapshotBytes(time: EventTime): Array[Byte]
}
