package com.thatdot.quine.graph

import scala.concurrent.Future

import com.thatdot.quine.graph.messaging.BaseMessage.Done
import com.thatdot.quine.model.{PropertyValue, QuineValue}

/** Basic operations that can be issued on a node actor */
trait BaseNodeActor extends BaseNodeActorView {

  /** Mutate this node according to a fresh node event
    *
    * This changes local actor state as well as iniating remote actions for writing the event to
    * durable storage and updating the index.
    *
    * === Thread safety ===
    *
    * This function is ''NOT'' thread safe. However, it can be called multiple times without waiting
    * for the returned futures to complete since the thread-unsafe computation happens before the
    * future gets returned.
    *
    * {{{
    * val l: List[NodeChangeEvent] = ...
    *
    * // Good
    * Future.traverse(l)(processEvent).map(_ => Done)
    *
    * // Probably bad: `processEvent` calls are sequential, but aren't running on the actor thread
    * l.foldLeft(Future.successful(Done))((acc, b) => acc.flatMap(_ => processEvent(b)))
    *
    * // Bad: `processEvent` calls are concurrent, potentially corrupting in-memory state
    * Future.sequence(l.par.map(processEvent)).map(_ => Done)
    * }}}
    *
    * === Redundant events ===
    *
    * If an event won't have an effect on node state (eg. it is removing and edge that doesn't
    * exist, or is setting to a property a value that is already the property's value), the
    * function short-circuits and returns a successful future.
    *
    * @param event a single event that is being applied individually
    * @param atTimeOverride overrides the time at which the event occurs (take great care if using this!)
    * @return future tracking completion of off-node actions
    */
  protected def processEvent(
    event: NodeChangeEvent,
    atTimeOverride: Option[EventTime] = None
  ): Future[Done.type]

  /** Set the labels on the node
    *
    * @param labels new labels for this node (overwriting previously set labels)
    * @return future signaling when the write is done
    */
  protected def setLabels(labels: Set[Symbol]): Future[Done.type] = {
    val labelsValue = QuineValue.List(labels.map(_.name).toVector.sorted.map(QuineValue.Str(_)))
    val propertyEvent = NodeChangeEvent.PropertySet(graph.labelsProperty, PropertyValue(labelsValue))
    processEvent(propertyEvent)
  }

  /** Record that some update pertinent to snapshots has occurred */
  protected def updateRelevantToSnapshotOccurred(): Unit

  /** Serializes a snapshot and also resets the `latestUpdateAfterSnapshot` */
  protected def toSnapshotBytes(): Array[Byte]
}
