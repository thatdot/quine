package com.thatdot.quine.persistor

import java.util.concurrent.ConcurrentHashMap

import scala.jdk.CollectionConverters._

import com.thatdot.quine.graph.NamespaceId
import com.thatdot.quine.model.QuineId
import com.thatdot.quine.util.Log._

/** Persistence agent that multiplexes nodes across multiple underlying persistence agents,
  * creating new agents the first time new partition keys are seen.
  *
  * @note use [[ShardedPersistor]] instead if there is a finite small number of possible partitions
  *
  * @param makePartition how to make a new persistence agent from its partition key
  * @param rootPartition root partition
  * @param partitionFunction function to pick the partition
  * @param namespace a constraint that all PersistenceAgents referenced through this class must be within this namespace
  */
final class SplitPersistor[K](
  makePartition: K => PersistenceAgent,
  rootPartition: K,
  partitionFunction: QuineId => K,
  val persistenceConfig: PersistenceConfig,
  val namespace: NamespaceId = None,
  val parts: ConcurrentHashMap[K, PersistenceAgent] = new ConcurrentHashMap[K, PersistenceAgent](),
)(implicit val logConfig: LogConfig)
    extends PartitionedPersistenceAgent {

  @inline
  def getAgent(id: QuineId): PersistenceAgent = {
    val key = partitionFunction(id)
    val value = parts.computeIfAbsent(
      key,
      (key: K) => makePartition(key), // Eta-expansion breaks inference of a _Java_ function
    )
    if (value.namespace != namespace) {
      parts.remove(key)
      throw new RuntimeException(
        s"Returned PersistenceAgent for QuineId: $id had the namespace: ${value.namespace} which did not match " +
        s"the specified namespace: $namespace and was removed.",
      )
    }
    value
  }

  def getAgents: Iterator[PersistenceAgent] =
    parts.values().iterator.asScala

  def rootAgent: PersistenceAgent = {
    val value = parts.computeIfAbsent(
      rootPartition,
      (id: K) => makePartition(id), // Eta-expansion breaks inference of a _Java_ function
    )
    if (value.namespace != namespace) {
      parts.remove(rootPartition)
      throw new RuntimeException(
        s"Returned root PersistenceAgent had the namespace: ${value.namespace} which did not match " +
        s"the specified namespace: $namespace and was removed.",
      )
    }
    value
  }
}
