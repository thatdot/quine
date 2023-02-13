package com.thatdot.quine.graph

import scala.concurrent.Future

import com.thatdot.quine.graph.messaging.BaseMessage.Done
import com.thatdot.quine.model.{PropertyValue, QuineValue}

/** Basic operations that can be issued on a node actor */
trait BaseNodeActor extends BaseNodeActorView {

  /** Apply an event to this node and save the event to the persistor.
    *
    * === Thread safety ===
    *
    * This function is ''NOT'' thread safe. If processing
    * multiple events, call this function once and pass the events in as a sequence. Redundant events will be stripped
    * out in order of the sequence in which they are passed, but after after stripping out redundant events, the effects
    * may not be strictly applied in the same order.
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
    * a property a value that is already the property's value), the function short-circuits and returns a successful
    * future without ever saving the effect-less event to the node's journal.
    *
    * If a list of events is passed in, but the list contains redundant events or events which reverse the effects of
    * other events in this list, the extra events will be filtered out. e.g. If a list of events includes setting the
    * property "foo" to "bar" and another that sets "foo" to "baz", then only the last event that sets "foo" to "baz"
    * will be applied to the event. There will be no record in the node's journal of having ever been set to "bar".
    *
    * @param event a single event that is being applied individually
    * @param atTimeOverride overrides the time at which the event occurs (take great care if using this!)
    * @return future tracking completion
    */

  protected def processPropertyEvents(
    events: Seq[PropertyEvent],
    atTimeOverride: Option[EventTime] = None
  ): Future[Done.type]

  // The only place this is called with a collection is when deleting a node.
  protected def processEdgeEvents(
    events: Seq[EdgeEvent],
    atTimeOverride: Option[EventTime] = None
  ): Future[Done.type]

  /** Set the labels on the node
    *
    * @param labels new labels for this node (overwriting previously set labels)
    * @return future signaling when the write is done
    */
  protected def setLabels(labels: Set[Symbol]): Future[Done.type] = {
    val labelsValue = QuineValue.List(labels.map(_.name).toVector.sorted.map(QuineValue.Str))
    val propertyEvent = PropertyEvent.PropertySet(graph.labelsProperty, PropertyValue(labelsValue))
    processPropertyEvents(propertyEvent :: Nil)
  }

  /** Record that some update pertinent to snapshots has occurred */
  protected def updateRelevantToSnapshotOccurred(): Unit

  /** Serializes a snapshot and also resets the `latestUpdateAfterSnapshot` */
  protected def toSnapshotBytes(time: EventTime): Array[Byte]
}
