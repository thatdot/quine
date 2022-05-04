package com.thatdot.quine.persistor

/** Configuration that describes how Quine should use PersistenceAgent.
  *
  * @param journalEnabled Enable or disable Quine journal persistence
  * @param snapshotSchedule When to save snapshots
  * @param snapshotSingleton Overwrite a single snapshot record per graph node
  * @param standingQuerySchedule when to save standing query partial results (SQv4 only - DGB is always on node sleep)
  */
final case class PersistenceConfig(
  journalEnabled: Boolean = true,
  effectOrder: EventEffectOrder = EventEffectOrder.MemoryFirst,
  snapshotSchedule: PersistenceSchedule = PersistenceSchedule.OnNodeSleep,
  snapshotSingleton: Boolean = false,
  standingQuerySchedule: PersistenceSchedule = PersistenceSchedule.OnNodeSleep
) {
  def snapshotEnabled: Boolean = snapshotSchedule != PersistenceSchedule.Never
  def snapshotOnSleep: Boolean = snapshotSchedule == PersistenceSchedule.OnNodeSleep
  def snapshotOnUpdate: Boolean = snapshotSchedule == PersistenceSchedule.OnNodeUpdate
}

sealed abstract class PersistenceSchedule

object PersistenceSchedule {
  case object Never extends PersistenceSchedule
  case object OnNodeSleep extends PersistenceSchedule
  case object OnNodeUpdate extends PersistenceSchedule
}

/** A query (part) can be conceived of as a collection of individual updates to a node. Those updates can be applied
  * individually, though sometimes multiple updates are important to consider as a unit.
  * Considering a single update, there are 3 effects to consider:
  *   1. Testing for and computing the effect, if any. Decides `hasEffect` and pins down a specific EventTime.
  *   2. Updating in-memory state. This causes queries to return updated results, and triggers standing queries.
  *   3. Saving data durably to disk through the persistor.
  *
  * There are three distinct parties participating in the update:
  *   a. the node: materialized state of the node, single process to determine updates and resolve a query
  *   b. the persistor: saves events to disk, confirming when durably stored
  *   c. the query issuer: conveys user preferences and receives the confirmation (or failure) of node updates.
  *
  * These strategies affect the order of operations for the participating parties and their respective effects.
  */
sealed trait EventEffectOrder
object EventEffectOrder {

  /** Update the node in memory before the write to disk. Retry persistor failures expecting they will eventually
    * succeed. The writing query is only completed successfully after the persistor write succeeds, but the changes are
    * visible in memory before the write to disk has completed.
    * - In the presence of persistor failure, query and standing query results can be visible for changes never
    *   saved to disk
    * - Multiple updates to the same node appear simultaneously
    *
    * Implementation notes:
    * - Order: test hasEffect, ApplyNCE, persistorFuture retried forever, complete the query.
    * - Persistor will retry indefinitely (or Int.MaxValue times), with an exponential backoff.
    */
  case object MemoryFirst extends EventEffectOrder

  /** Wait for update to disk before applying in-memory effects. Other messages to the node are deferred so that no
    * other processing can occur until memory effects are applied. Most correct, least available. Highest latency,
    * especially for multiple successive operations. Does not affect throughput if parallelism is also increased.
    * - Nodes cannot sleep until persistorFuture completes.
    *
    * Implementation notes:
    * - Order: test hasEffect, pauseMessageProcessingUntil persistorFuture, ApplyNCE, complete the query.
    * - Failed persistor will cause the query to fail; the query can be retried.
    */
  case object PersistorFirst extends EventEffectOrder
}
