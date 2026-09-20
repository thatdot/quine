package com.thatdot.quine.persistor.codecs

import com.thatdot.quine.graph.{Notifiable, StandingQueryId}

/** Reading node snapshots written by an earlier Quine.
  *
  * ==How a version is recognised==
  *
  * There is no version stamp in a snapshot, and none is added here. FlatBuffers reports an absent field as absent,
  * so the presence of a field is itself the evidence of which release wrote the record: a `Subscriber` carrying
  * `queries_per_subscriber` was written by 2.2.0 or later, and one carrying only `notifiable` and
  * `related_queries` by 2.1.1. That is enough to distinguish every case the upgrade needs, so no field is added
  * for migration's sake alone -- a field written only to describe the format would cost bytes in every snapshot
  * forever to serve an upgrade each node performs once.
  *
  * ==Why these live here and not inline in the codec==
  *
  * Each step has to survive until support for the version it reads is dropped, which is longer than anyone will
  * remember why a conditional in the middle of a decode loop is shaped the way it is. Naming the step for the
  * versions it spans makes the retention question answerable: when 2.1.1 snapshots are no longer supported,
  * [[From_2_1_1_to_2_2_0]] is deleted whole and the call sites fail to compile.
  *
  * ==Adding the next one==
  *
  * Add an object named for the pair of versions it spans, put the whole decision in it, and call it from the
  * codec at the point where the older and newer shapes differ. A migration that cannot be expressed as a pure
  * function of what was decoded belongs here too, with a comment saying where the rest of it happens -- see the
  * note on the index in [[From_2_1_1_to_2_2_0]].
  */
object SnapshotMigration {

  /** A snapshot format, named for the Quine release that wrote it. */
  sealed abstract class Version(val release: String) {
    override def toString: String = release
  }

  object Version {
    case object V2_1_1 extends Version("2.1.1")
    case object V2_2_0 extends Version("2.2.0")

    /** The format this build writes. */
    val current: Version = V2_2_0
  }

  /** Which release wrote a `Subscriber`, judged by what it carries.
    *
    * @param hasQueriesPerSubscriber whether the `queries_per_subscriber` vector was present
    */
  def subscriberFormat(hasQueriesPerSubscriber: Boolean): Version =
    if (hasQueriesPerSubscriber) Version.V2_2_0 else Version.V2_1_1

  /** 2.1.1 recorded which standing queries a subscription served, but only as a union across all of its
    * subscribers. 2.2.0 records the queries per subscriber, because a subscriber is retired once none of *its own*
    * queries runs, and the union cannot answer that question.
    *
    * 2.1.1 also recorded nothing at all about which queries an index entry -- a peer's answer about a child
    * pattern -- was asked for.
    */
  object From_2_1_1_to_2_2_0 {

    /** Attribute the union to each subscriber, which is the only thing 2.1.1's record supports.
      *
      * An over-approximation: a subscriber is credited with queries it may never have depended on, so its
      * subscription outlives its need. Chosen because the opposite error loses results, and because it does not
      * persist -- the wake-time sweeps drop cancelled queries from what survives, so the set narrows towards the
      * queries that really are live. It never becomes exactly right, and that is accepted rather than corrected:
      * recovering the truth would mean asking the subscribers who they are asking for, which is a protocol change
      * for precision the over-approximation does not cost correctness on.
      *
      * @param recorded    what `queries_per_subscriber` held, empty for a 2.1.1 snapshot
      * @param subscribers who `notifiable` listed
      * @param union       what `related_queries` held
      */
    def queriesPerSubscriber(
      recorded: Map[Notifiable, Set[StandingQueryId]],
      subscribers: Set[Notifiable],
      union: Set[StandingQueryId],
    ): Map[Notifiable, Set[StandingQueryId]] =
      subscribers.foldLeft(recorded) { (acc, subscriber) =>
        if (acc.contains(subscriber)) acc else acc.updated(subscriber, union)
      }

    /** An index entry from 2.1.1 records no queries, and nothing in the snapshot can say which they were: the
      * union in `related_queries` belongs to the subscription, not to any particular ask.
      *
      * So this half of the migration is not a function of the decoded snapshot and does not happen here. The
      * entry decodes with an empty query set, and the node fills it in at its next wake from the registry: every
      * standing query whose pattern contains that child -- see `recordMissingQueryIds` in
      * `DomainNodeIndexBehavior`. An entry no running query's pattern contains is dropped there.
      */
    val indexQueriesAreFilledInAtWake: Unit = ()
  }
}
