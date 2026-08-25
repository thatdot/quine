package com.thatdot.quine.webapp.components.streams

import com.thatdot.quine.webapp.v2api.V2ApiTypes.V2IngestInfo

/** One ingest name, collapsed across the cluster members running it.
  *
  * Clustered ingests replicate the same name onto multiple member positions; the
  * Streams table shows one row per name (mirroring the dashboard) instead of one
  * row per member. `members` holds each member's own record, sorted by position,
  * and is always non-empty.
  */
final case class IngestGroup(name: String, members: List[V2IngestInfo]) {

  /** The source type of every member when they agree, or `None` when they disagree.
    * Members of one ingest run a replicated config, so disagreement is not expected.
    */
  def agreedSourceType: Option[String] = agreed(_.sourceType)

  def size: Int = members.size

  /** Cluster positions running this ingest, ascending; empty on single-node
    * deployments where members carry no position.
    */
  def memberIndices: List[Int] = members.flatMap(_.memberIdx).sorted

  /** The status of every member when they agree, or `None` when they disagree.
    * So a group only reports `Failed` when every member has failed.
    */
  def agreedStatus: Option[String] = agreed(_.status)

  private def agreed(field: V2IngestInfo => String): Option[String] =
    members.map(field).distinct match {
      case single :: Nil => Some(single)
      case _ => None
    }

  def totalIngestedCount: Long = members.map(_.stats.ingestedCount).sum

  def totalRate: Double = members.map(_.stats.rates.oneMinute).sum
}

object IngestGroup {

  /** Collapse a flat per-member ingest list into one group per name, ordered by
    * name for stable display; members within a group are ordered by position.
    */
  def fromInfos(infos: List[V2IngestInfo]): List[(String, IngestGroup)] =
    infos
      .groupBy(_.name)
      .toList
      .sortBy(_._1)
      .map { case (name, members) =>
        name -> IngestGroup(name, members.sortBy(_.memberIdx.getOrElse(-1)))
      }
}
