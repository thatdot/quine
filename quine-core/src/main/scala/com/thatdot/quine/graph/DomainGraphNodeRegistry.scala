package com.thatdot.quine.graph
import java.util.concurrent.ConcurrentHashMap

import scala.concurrent._
import scala.jdk.CollectionConverters._

import com.thatdot.quine.model.DomainGraphNode.DomainGraphNodeId
import com.thatdot.quine.model.{DomainGraphBranch, DomainGraphNode, DomainGraphNodePackage, IdentifiedDomainGraphNode}

/** Service for [[DomainGraphNode]] (DGN).
  * Manages in-memory store ID-DGN mapping with write-through to [[PersistenceAgent]].
  */
class DomainGraphNodeRegistry(
  registerGaugeDomainGraphNodeCount: (() => Int) => Unit,
  persistDomainGraphNodes: Map[DomainGraphNodeId, DomainGraphNode] => Future[Unit],
  removeDomainGraphNodes: Set[DomainGraphNodeId] => Future[Unit]
) {

  private case class DGNWithRef(dgn: DomainGraphNode, standingQueries: Set[StandingQueryId])

  /** Mapping of Domain Graph Node identities to class instances.
    * The value also includes a set of Standing Query IDs which is managed by
    * [[put]] and [[remove]].
    */
  private val dgnRegistryMap: ConcurrentHashMap[DomainGraphNodeId, DGNWithRef] =
    new ConcurrentHashMap[DomainGraphNodeId, DGNWithRef]()

  /** Size of map collection, for unit tests and metrics. */
  def size: Int = dgnRegistryMap.size

  /** Count of Standing Query Query references to DGNs, for unit tests. */
  def referenceCount: Int = dgnRegistryMap.asScala.values.map(_.standingQueries.size).sum

  // Report the number of records in the map as a metric
  registerGaugeDomainGraphNodeCount(() => size)

  /** Optionally returns the [[DomainGraphNode]] for its [[DomainGraphNodeId]]. */
  def getDomainGraphNode(dgnId: DomainGraphNodeId): Option[DomainGraphNode] =
    Option(dgnRegistryMap.get(dgnId)).map(_.dgn)

  /** Calls callback function [[f]] with the [[DomainGraphNode]] for the requested [[DomainGraphNodeId]].
    * Callback function [[f]] is only called if the [[DomainGraphNode]] exists in the registry. If the
    * [[DomainGraphNode]] does not exist, the function is not called. The return value is the value returned
    * by [[f]] wrapped in `Some`, or `None` if the [[DomainGraphNode]] does not exist.
    */
  def withDomainGraphNode[A](dgnId: DomainGraphNodeId)(f: DomainGraphNode => A): Option[A] =
    getDomainGraphNode(dgnId).map(f)

  /** Optionally returns the [[IdentifiedDomainGraphNode]] for its [[DomainGraphNodeId]]. An
    * [[IdentifiedDomainGraphNode]] is just a [[DomainGraphNode]] composed with its associated [[DomainGraphNodeId]].
    */
  def getIdentifiedDomainGraphNode(dgnId: DomainGraphNodeId): Option[IdentifiedDomainGraphNode] =
    getDomainGraphNode(dgnId).map(IdentifiedDomainGraphNode(dgnId, _))

  /** Calls callback function [[f]] with the [[IdentifiedDomainGraphNode]] for the requested [[DomainGraphNodeId]].
    * Callback function [[f]] is only called if the [[DomainGraphNode]] exists in the registry. If the
    * [[DomainGraphNode]] does not exist, the function is not called. The return value is the value returned
    * by [[f]] wrapped in `Some`, or `None` if the [[DomainGraphNode]] does not exist.
    */
  def withIdentifiedDomainGraphNode[B](dgnId: DomainGraphNodeId)(f: IdentifiedDomainGraphNode => B): Option[B] =
    getIdentifiedDomainGraphNode(dgnId).map(f)

  /** Optionally returns the [[DomainGraphBranch]] for its [[DomainGraphNodeId]]. A [[DomainGraphBranch]] is
    * converted from a [[DomainGraphNode]] by recursively rehydrating its thirsty children.
    */
  def getDomainGraphBranch(dgnId: DomainGraphNodeId): Option[DomainGraphBranch] =
    DomainGraphBranch.fromDomainGraphNodeId(dgnId, getDomainGraphNode)

  /** Calls callback function [[f]] with the [[DomainGraphBranch]] for the requested [[DomainGraphNodeId]].
    * Callback function [[f]] is only called if the [[DomainGraphNode]] exists in the registry. If the
    * [[DomainGraphNode]] does not exist, the function is not called. The return value is the value returned
    * by [[f]] wrapped in `Some`, or `None` if the [[DomainGraphNode]] does not exist.
    */
  def withDomainGraphBranch[C](dgnId: DomainGraphNodeId)(f: DomainGraphBranch => C): Option[C] =
    getDomainGraphBranch(dgnId).map(f)

  /** Optionally updates the in-memory mapping from identifiers to Domain Graph Nodes.
    * Guarantees that a mapping exists from [[dgnId]] to [[dgn]], and that the [[standingQueryId]]
    * is associated with that mapping.
    *
    * @return True if this is the 1st reference to the [[DomainGraphNode]].
    */
  def put(dgnId: DomainGraphNodeId, dgn: DomainGraphNode, standingQueryId: StandingQueryId): Boolean =
    dgnRegistryMap
      .compute(
        dgnId,
        {
          case (_, null) => DGNWithRef(dgn, Set(standingQueryId))
          case (_, DGNWithRef(existingDgn, standingQueries)) =>
            DGNWithRef(existingDgn, standingQueries + standingQueryId)
        }
      )
      .standingQueries
      .size == 1

  /** Updates the in-memory mapping of identifiers to [[DomainGraphNode]]s by removing
    * the [[standingQueryId]] from the mapping. If the set of associated Standing Queries
    * is now empty, then the entire mapping record is removed.
    *
    * @return True if this was the last reference to the [[DomainGraphNode]] (and it was
    *         therefore removed).
    */
  def remove(dgnId: DomainGraphNodeId, standingQueryId: StandingQueryId): Boolean = {
    var removedLast = false
    dgnRegistryMap.compute(
      dgnId,
      {
        case (_, null) => null
        case (_, DGNWithRef(dgn, standingQueries)) =>
          val updatedStandingQueries = standingQueries - standingQueryId
          if (updatedStandingQueries.isEmpty) {
            removedLast = true
            null
          } else {
            DGNWithRef(dgn, updatedStandingQueries)
          }
      }
    )
    removedLast
  }

  /** Updates [[dgnRegistryMap]] by calling [[put]] for every DGN in the package.
    * Returns the subset of the package that represents newly added [[DomainGraphNode]]s.
    * ([[DomainGraphNode]]s with a single Standing Query reference.)
    */
  def registerDomainGraphNodePackage(
    dgnPackage: DomainGraphNodePackage,
    standingQueryId: StandingQueryId
  ): Map[DomainGraphNodeId, DomainGraphNode] =
    dgnPackage.population.filter { case (dgnId, dgn) =>
      put(dgnId, dgn, standingQueryId)
    }

  /** Updates [[dgnRegistryMap]] by calling [[registerDomainGraphNodePackage]].
    * Optionally persists newly added [[DomainGraphNode]]s.
    * Returns a future that completes when persistence is successful.
    */
  def registerAndPersistDomainGraphNodePackage(
    dgnPackage: DomainGraphNodePackage,
    standingQueryId: StandingQueryId,
    skipPersistor: Boolean
  ): Future[Unit] = {
    val newNodes = registerDomainGraphNodePackage(dgnPackage, standingQueryId)
    if (!skipPersistor && newNodes.nonEmpty) persistDomainGraphNodes(newNodes)
    else Future.unit
  }

  /** Recursively unregisters this node and all of its ancestors.
    * This includes removing the Standing Query reference of this and all ancestors, and deleting nodes
    * where the reference set size has gone to zero.
    */
  def unregisterDomainGraphNodePackage(
    dgnPackage: DomainGraphNodePackage,
    standingQueryId: StandingQueryId,
    skipPersistor: Boolean = false
  ): Future[Unit] = {
    val decrementedToZeroDomainGraphNodeIds = for {
      decrementDomainGraphNodeId <- dgnPackage.population.keySet
      if remove(decrementDomainGraphNodeId, standingQueryId)
    } yield decrementDomainGraphNodeId
    if (!skipPersistor && decrementedToZeroDomainGraphNodeIds.nonEmpty)
      removeDomainGraphNodes(decrementedToZeroDomainGraphNodeIds)
    else Future.unit
  }
}
