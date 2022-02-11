package com.thatdot.quine.graph

import akka.actor.Actor
import akka.stream.Materializer

import com.thatdot.quine.graph.edgecollection.EdgeCollectionView
import com.thatdot.quine.graph.messaging.QuineIdAtTime
import com.thatdot.quine.model.{Milliseconds, PropertyValue, QuineId, QuineIdProvider, QuineValue}

/** Read-only view of a node actor */
trait BaseNodeActorView extends Actor {

  /** Handle to the enclsoing graph */
  def graph: BaseGraph

  /** Materializer */
  implicit val materializer: Materializer = graph.materializer

  /** Properties of the node */
  def properties: Map[Symbol, PropertyValue]

  /** Edges of the node */
  def edges: EdgeCollectionView

  /** Unique ID of the node tracked by this node actor
    *
    * @note this is safe to close over - it is immutable
    */
  def qid: QuineId

  /** Moment in time being tracked by this node actor
    *
    * @note this is safe to close over - it is immutable
    */
  def atTime: Option[Milliseconds]

  def qidAtTime: QuineIdAtTime

  /** ID provider */
  implicit def idProvider: QuineIdProvider

  /** Metrics about the quine system */
  def metrics: HostQuineMetrics

  /** Fetch the labels of this node
    *
    * @note returns [[None]] if the property defined but not a list of strings
    * @return the labels on this node
    */
  def getLabels(): Option[Set[Symbol]] =
    properties.get(graph.labelsProperty) match {
      // Property value is not defined
      case None => Some(Set.empty)

      case Some(quineValue) =>
        quineValue.deserialized.toOption match {
          case Some(QuineValue.List(lst)) =>
            val acc = Set.newBuilder[Symbol]
            val elemIterator = lst.iterator
            while (elemIterator.hasNext)
              elemIterator.next() match {
                case QuineValue.Str(lbl) => acc += Symbol(lbl)
                case _ => return None // Malformed label field
              }
            Some(acc.result())

          case _ => None // Malformed label field
        }
    }

  def latestUpdateAfterSnapshot: Option[EventTime]
}
