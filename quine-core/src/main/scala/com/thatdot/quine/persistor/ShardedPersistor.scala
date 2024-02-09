package com.thatdot.quine.persistor

import com.thatdot.quine.graph.NamespaceId
import com.thatdot.quine.model.QuineId

/** Persistence agent that multiplexes nodes across a pre-determined number of underlying
  * persistence agents.
  *
  * @param shards persistors across which data is partitioned
  * @param partitionFunction function to pick the shard (if the result is greater than the ID of the
  *                          highest-ID shard, this will be used modulo the number of shards)
  */
class ShardedPersistor(
  shards: Vector[PersistenceAgent],
  val persistenceConfig: PersistenceConfig,
  partitionFunction: QuineId => Int = _.hashCode
) extends PartitionedPersistenceAgent {

  val allShardsAreInSameNamespace: Boolean = shards.headOption.fold(true) { h =>
    shards.tail.forall(_.namespace == h.namespace)
  }
  require(
    allShardsAreInSameNamespace,
    "Cannot instantiate ShardedPersistor with constituent PersistenceAgents from different namespaces."
  )
  require(shards.nonEmpty, "Cannot instantiate ShardedPersistor with no PersistenceAgents")
  val namespace: NamespaceId = shards.head.namespace

  private[this] val numShards = shards.size
  require(numShards > 0, "ShardedPersistor needs at least one persistor")

  @inline
  final def getAgent(id: QuineId): PersistenceAgent =
    shards.apply(Math.floorMod(partitionFunction(id), numShards))

  final def getAgents: Iterator[PersistenceAgent] =
    shards.iterator

  final def rootAgent: PersistenceAgent =
    shards(0)
}
