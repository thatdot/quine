package com.thatdot.quine.util

import scala.concurrent.Future

import com.google.common.cache.{Cache, CacheBuilder}

// TODO consider typeclassing for extensibility into things like "resettable cache" or "persistable cache"

/** A cache capable of deduplicating identical entries. Multiple instances of the same subtype
  * of this may share resources. For example, multiple instances of a Redis-backed cache may
  * use the same Redis keyspace. It is the caller's responsibility to ensure elements have sufficient
  * entropy. Some caches may provide additional support for creating logically-namespaced instances.
  *
  * Instances may implement any expiry behavior they choose. Notably, [[DisabledCache]] implements
  * "total expiry" -- it retains nothing, and considers everything a cache miss!
  *
  * @tparam E the type of keys/elements (terms used interchangeably) to cache and deduplicate.
  */
trait DeduplicationCache[E] {

  /** Check if an element is present in the cache. This should not expire any contents of the cache
    * @return true when the element is present, false otherwise
    */
  def contains(elem: E): Future[Boolean]

  /** Insert an element into the cache. Depending on the cache implementation, this may expire one or more entries.
    * @return true if the element is new to the cache, false otherwise. Regardless of the
    *         returned value, the cache will be updated
    */
  def insert(elem: E): Future[Boolean]

  /** How many concurrent calls to `contains` or `insert` are advisable, given the cache
    * implementation. This can used as a hint to users of `contains` or `insert`
    *
    * If this is > 1, the cache must be threadsafe.
    */
  def recommendedParallelism: Int
}

/** An always-empty cache
  */
class DisabledCache[E]() extends DeduplicationCache[E] {
  def contains(elem: E): Future[Boolean] = Future.successful(false)
  def insert(elem: E): Future[Boolean] = Future.successful(true)

  val recommendedParallelism: Int = 1024
}

object DisabledCache {
  def apply[E](): DisabledCache[E] = new DisabledCache[E]()
}

/** Threadsafe implementation of [[DeduplicationCache]] backed by a [[Cache]] with perfect LRU expiry.
  */
class InMemoryDeduplicationCache[E](size: Long) extends DeduplicationCache[E] {
  val cache: Cache[E, Unit] =
    CacheBuilder
      .newBuilder()
      .asInstanceOf[CacheBuilder[E, Unit]]
      .maximumSize(size)
      .build()

  /** Check if an element is present in the cache. This will not expire any contents of the cache, but is considered a
    * "usage" for the sake of the LRU expiry scheme.
    *
    * @param elem
    * @return true when the element is present, false otherwise
    */
  def contains(elem: E): Future[Boolean] = Future.successful(
    Option(cache.getIfPresent(elem)).isDefined
  )

  /** Insert an element into the cache. If the cache already contains at least `size` elements, and this element is not
    * among them, the least-recently-used element will be dropped.
    *
    * @return true if the element was already present in the cache, false otherwise. Regardless of the
    *         returned value, the cache will be updated
    */
  def insert(elem: E): Future[Boolean] = Future.successful {
    Option(cache.asMap().put(elem, ())).isEmpty
  }

  val recommendedParallelism: Int = 256 // Very arbitrary

  /** Reset the cache
    */
  def reset(): Unit = cache.invalidateAll()
}
object InMemoryDeduplicationCache {
  def apply[E](size: Long): InMemoryDeduplicationCache[E] = new InMemoryDeduplicationCache[E](size)
}
