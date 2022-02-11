package com.thatdot.quine.persistor

import java.util.concurrent.ConcurrentHashMap

import scala.jdk.CollectionConverters._

import com.thatdot.quine.model.QuineId

/** Persistence agent that multiplexes nodes across multiple underlying persistence agents,
  * creating new agents the first time new partition keys are seen.
  *
  * @note use [[ShardedPersistor]] instead if there is a finite small number of possible partitions
  *
  * @param makePartition how to make a new persistence agent from its partition key
  * @param rootPartition root partition
  * @param partitionFunction function to pick the partition
  */
final class SplitPersistor[K](
  makePartition: K => PersistenceAgent,
  rootPartition: K,
  partitionFunction: QuineId => K,
  val persistenceConfig: PersistenceConfig,
  val parts: ConcurrentHashMap[K, PersistenceAgent] = new ConcurrentHashMap[K, PersistenceAgent]()
) extends PartitionedPersistenceAgent {

  @inline
  def getAgent(id: QuineId): PersistenceAgent =
    parts.computeIfAbsent(
      partitionFunction(id),
      (key: K) => makePartition(key) // Eta-expansion breaks inference of a _Java_ function
    )

  def getAgents: Iterator[PersistenceAgent] =
    parts.values().iterator.asScala

  def rootAgent: PersistenceAgent =
    parts.computeIfAbsent(
      rootPartition,
      (id: K) => makePartition(id) // Eta-expansion breaks inference of a _Java_ function
    )
}
