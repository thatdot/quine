package com.thatdot.quine.webapp.dataservice

import scala.collection.mutable

/** Who is watching what, for resources whose underlying connection is shared.
  *
  * Answers the only two questions a sharing store asks: is this the first subscriber on a
  * resource (so open it), and was that the last (so release it). Kept separate from the store
  * because the store's other half needs a real WebSocket, and this half is where the mistake
  * that matters lives — releasing a resource somebody is still using cuts their stream with no
  * error anywhere.
  *
  * Mutated synchronously and deliberately not reactive: it is consulted inside command handlers,
  * where a deferred Airstream write would be read stale.
  */
final class SubscriberRegistry {

  private val watchers: mutable.Map[String, mutable.Set[String]] = mutable.Map.empty

  /** Record `subscriber`'s interest in `key`. Returns true when it is the first — the caller's
    * cue to open the underlying resource. Repeating a subscriber returns false.
    */
  def add(subscriber: String, key: String): Boolean = {
    val existing = watchers.getOrElseUpdate(key, mutable.Set.empty)
    val wasEmpty = existing.isEmpty
    existing.add(subscriber)
    wasEmpty
  }

  /** Drop `subscriber`'s interest in `key`. Returns true when nobody is left — the caller's cue
    * to release the underlying resource. Dropping an unknown subscriber returns false, so a
    * duplicate close can't release a resource still in use.
    */
  def remove(subscriber: String, key: String): Boolean =
    watchers.get(key) match {
      case None => false
      case Some(existing) =>
        val wasWatching = existing.remove(subscriber)
        val nowEmpty = existing.isEmpty
        if (nowEmpty) watchers.remove(key)
        wasWatching && nowEmpty
    }

  /** Forget everything, for a wholesale teardown where the caller releases each resource
    * itself.
    */
  def clear(): Unit = watchers.clear()

  /** Who is currently watching `key`.
    *
    * No production caller: [[add]] and [[remove]] report only the edge transitions a store acts
    * on, and this is how the tests assert the state behind them.
    */
  private[dataservice] def watching(key: String): Set[String] =
    watchers.get(key).map(_.toSet).getOrElse(Set.empty)
}
