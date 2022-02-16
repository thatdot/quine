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
