package com.thatdot.quine.util

import java.util.{LinkedHashMap => JavaLinkedHashMap, Map => JavaMap}

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import com.thatdot.quine.util.Log._
import com.thatdot.quine.util.Log.implicits._

abstract class NanoTimeSource {
  def nanoTime(): Long
}
object SystemNanoTime extends NanoTimeSource {
  @inline
  final def nanoTime(): Long = System.nanoTime()
}
sealed abstract private[quine] class ExpiringLruSet[A] {

  /** Number of entries in the set */
  def size: Int

  /** Is an element in the set? This does not count as a "use"
    *
    * @param elem element to look for in the set
    * @return whether the element is in the set
    */
  def contains(elem: A): Boolean

  /** Iterator, ordered from first-to-expire to last-to-expire */
  def iterator: Iterator[A]

  /** Remove all entries from the set without performing expiration */
  def clear(): Unit

  /** Remove one entry from the set. If the element is not already in the set,
    * don't do anything.
    *
    * @param elem element to remove from the set
    */
  def remove(elem: A): Unit

  /** Add or update one entry to the set, putting it back at the end of the
    * expiry queue
    *
    * @param elem element to add to the set
    */
  def update(elem: A): Unit

  /** This is called from [[doExpiration]] when an element is about to be
    * evicted and can be used to save an element from eviction.
    *
    * For a simple time/size bounded LRU, this can be defined to always return
    * `true`. When building in more complex custom logic, be aware that this
    * method can end up being called very often!
    *
    * @param elem element to expire from the set
    * @return whether to really remove the element (else it is re-inserted)
    */
  def shouldExpire(elem: A): ExpiringLruSet.ExpiryDecision

  /** Action to take when an element has been evicted from the cache
    *
    * @param cause why was the element removed
    * @param elem what element was removed
    */
  def expiryListener(cause: ExpiringLruSet.RemovalCause, elem: A): Unit

  /** Expire out of the set elements that are overdue or oversized */
  def doExpiration(): Unit
}

private[quine] object ExpiringLruSet {

  /** Whether or not to expire an element from [[ExpiringLruSet]]
    *
    * @see shouldExpire
    */
  sealed abstract private[quine] class ExpiryDecision
  private[quine] object ExpiryDecision {

    /** Element should be expired */
    case object ShouldRemove extends ExpiryDecision

    /** Element should not be expired
      *
      * @note the `progressWasMade` argument distinguishes the case where the
      * very act of requesting removal has brought the element closer to removal
      * from the case where the element is no closer to removal than before.
      * This is used to avoid infinite loops in [[ExpiringLruSet.doExpiration]],
      * for instance when the cache is oversized, but none of the elements will
      * ever accept to be removed.
      *
      * @param progressWasMade has progress towards removing the element been made?
      */
    final case class RejectRemoval(progressWasMade: Boolean) extends ExpiryDecision

  }

  /** Reason for removing an element from the cache */
  sealed abstract private[quine] class RemovalCause
  private[quine] object RemovalCause {

    /** Cache was too big */
    case object Oversized extends RemovalCause

    /** Element in cache was too old */
    case object Expired extends RemovalCause
  }

  /** Doesn't store anything, so can't meaningfully sample or update */
  private[quine] class Noop[A] extends ExpiringLruSet[A] {
    def size: Int = 0
    def contains(elem: A) = false
    def iterator: Iterator[A] = Iterator.empty
    def clear(): Unit = ()
    def remove(elem: A): Unit = ()
    def update(elem: A): Unit = ()
    def shouldExpire(elem: A): ExpiryDecision.RejectRemoval = ExpiryDecision.RejectRemoval(progressWasMade = false)
    def expiryListener(cause: RemovalCause, elem: A): Unit = ()
    def doExpiration(): Unit = ()
  }

  /** Expires elements when the cache has exceeded its maximum size or the item
    * has exceeded its maximum expiry time.
    *
    * Note that both `maximumSize` and `maximumNanosAfterAccess` can be adjusted
    * at runtime (scaling them down will result in the next call to [[doExpiration]]
    * potentially triggering a lot of evictions—and calls to shouldRemove.
    *
    * @note time-based cleanup still only happens when [[doExpiration]] is called
    * @note this is not threadsafe, and therefore should only be used when managed (eg by a GraphShardActor)
    * @param initialCapacity initial capacity of the underlying map
    * @param initialMaximumSize maximum number of elements to allow before expiring
    * @param initialNanosExpiry nanoseconds after accessing before expiring
    */
  abstract class SizeAndTimeBounded[A](
    initialCapacity: Int,
    initialMaximumSize: Int,
    initialNanosExpiry: Long,
    nanoTimeSource: NanoTimeSource = SystemNanoTime
  ) extends ExpiringLruSet[A]
      with LazySafeLogging {
    private[this] var _maximumSize: Int = initialMaximumSize
    private[this] var _maximumNanosExpiry: Long = initialNanosExpiry

    /* Map from element in the set to the (system nano-)time at which it was
     * added. The iteration order matches the order in which elements will get
     * expired.
     *
     * NOTE: we are using the `LinkedHashMap` constructor variant to override
     * the ordering to be _access order_ and not the default _insertion order_.
     * This option doesn't exist on Scala's `LinkedHashMap` (at time of writing)
     */
    private[this] val linkedMap: JavaLinkedHashMap[A, Long] =
      new JavaLinkedHashMap[A, Long](
        initialCapacity,
        0.75F, // default from other `JavaLinkedHashMap` constructors
        true // use access order, not insertion order
      )

    final def maximumSize: Int = _maximumSize
    final def maximumSize_=(newSize: Int): Unit = {
      _maximumSize = newSize
      doExpiration()
    }

    final def maximumNanosExpiry: Long = _maximumNanosExpiry
    final def maximumNanosExpiry_=(newTimeoutNanos: Long): Unit = {
      _maximumNanosExpiry = newTimeoutNanos
      doExpiration()
    }

    final def iterator: Iterator[A] = linkedMap.keySet().iterator.asScala
    final def contains(elem: A): Boolean = linkedMap.containsKey(elem)

    final def size = linkedMap.size

    final def clear(): Unit = linkedMap.clear()

    final def remove(elem: A): Unit = {
      linkedMap.remove(elem)
      ()
    }

    final def update(elem: A): Unit = {
      linkedMap.put(elem, nanoTimeSource.nanoTime())
      doExpiration()
    }

    /* Core idea here is to do as many full iterations over the map as needed.
     * Each iteration can efficiently scan through entries in proposed eviction
     * order, removing them along the way.
     *
     * Unless `shouldExpire` is refusing to expire often, one iteration should
     * almost always be enough (and not even a full iteration at that).
     */
    final def doExpiration(): Unit = {
      val reinsert = mutable.ListBuffer.empty[A]
      val entrySet = linkedMap.entrySet()
      val now: Long = nanoTimeSource.nanoTime()

      // How many elements to remove due to size constraints (if negative, constraint satisfied)
      var oversizedBy: Int = linkedMap.size - maximumSize

      // Make as many passes over a non-empty `linkedMap` as necessary to reach the condition:
      //     `oversizedBy <= 0 && ! entryIsExpired`
      var progressMadeThisIteration = false
      while (linkedMap.size > 0) {
        val entryIterator = entrySet.iterator()

        while (entryIterator.hasNext) {
          val entry: JavaMap.Entry[A, Long] = entryIterator.next()
          val entryIsExpired = (now - entry.getValue) > maximumNanosExpiry
          if (oversizedBy > 0 || entryIsExpired) {
            entryIterator.remove() // Removes `entry` from `linkedMap`
            shouldExpire(entry.getKey) match {
              case ExpiryDecision.ShouldRemove =>
                oversizedBy -= 1
                expiryListener(
                  if (oversizedBy > 0) RemovalCause.Oversized else RemovalCause.Expired,
                  entry.getKey
                )
                progressMadeThisIteration = true
              case ExpiryDecision.RejectRemoval(progress) =>
                reinsert += entry.getKey
                progressMadeThisIteration = progressMadeThisIteration || progress
            }
          } else {
            // We're done with needing to remove elements!
            reinsert.foreach(linkedMap.put(_, now))
            return
          }
        }

        // Re-insert with a new time
        reinsert.foreach(linkedMap.put(_, now))
        reinsert.clear()

        if (progressMadeThisIteration) {
          progressMadeThisIteration = false
        } else {
          logger.warn(
            safe"doExpiration: halting due to lack of progress, but LRU cache is still oversized by: ${Safe(oversizedBy)}"
          )
          return
        }
      }
    }
  }
}
