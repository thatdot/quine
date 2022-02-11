package com.thatdot.quine

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicLong
import java.{util => ju}

import scala.compat.ExecutionContexts
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

import com.codahale.metrics.Timer

import com.thatdot.quine.model.{
  DomainGraphBranch,
  EdgeDirection,
  GenericEdge,
  NodeLocalComparisonFunctions,
  PropertyComparisonFunctions,
  QuineId,
  QuineIdProvider
}

package object graph {

  /** Produce a `QuineId` from a series of arbitrary values. This is meant as the single canonical (user-facing) way to
    * turn values into a `QuineId` by means of consistent hashing. It can be used many places, but the intent is that
    * regardless of the interface (Cypher, API, Gremlin, etc.), the same (notional) values will produce the same `QuineId`.
    *
    * @param args Any arbitrary hashable value. Note: we expect the JVM `hashCode` does *NOT* contain sufficient bits,
    *             therefore a hashable value using Guava's implementation of 128-bit murmur3 hash is required. This is
    *             simplified to a `cypher.Value` for now.
    * @return A `QuineId` produce consistently from the input values
    */
  def idFrom(args: cypher.Value*)(implicit idProvider: QuineIdProvider): QuineId =
    idProvider.hashedQuineId(hashOfCypherValues(args))

  /** Produce a hash of cypher values as a byte array of no particular size
    */
  def hashOfCypherValues(args: Seq[cypher.Value]): Array[Byte] = Array.concat(args.map(_.hash.asBytes): _*)

  /** Conceptually, this is an estimate of how costly it would be to sleep a certain node.
    *
    * Cost refers both to the time it would take to sleep the node (serializing properties + edges)
    * and to the time wasted rewaking a node that was just put to sleep.
    */
  private[quine] type CostToSleep = AtomicLong

  /** A 0-indexed integer defining the position of a cluster member in the cluster */
  type MemberIdx = Int

  type Notifiable = Either[QuineId, StandingQueryId]

  // In short: the assumed edge must be the LAST edge tested; not the first requirement to be removed.
  private[quine] type AssumedDomainEdge = Option[(GenericEdge, DomainGraphBranch[model.Test])]
  private[quine] type LastNotification = Option[Boolean]

  /* DelayedInit on the object creation will keep objects nested inside from being instantiated until their first use.
   * Multithreaded deserialization was creating a race condition in nested object creation. Somehow this lead to a deadlock.
   * https://issues.scala-lang.org/browse/SI-3007
   */
  private[quine] def initializeNestedObjects(): Unit = synchronized {
    EdgeDirection
    EdgeDirection.Outgoing
    EdgeDirection.Incoming
    EdgeDirection.Undirected
    NodeLocalComparisonFunctions
    PropertyComparisonFunctions
    ()
  }

  implicit class FutureRecoverWith[T](f: Future[T]) {
    /* NB: it is important that the message be call by name, since we want to avoid actually
     *     computing the message until we are sure there is actually a failure to report
     */
    def recoveryMessage[U >: T](message: => String)(implicit ec: ExecutionContext): Future[U] =
      f.recoverWith {
        case e: QuineRuntimeFutureException => Future.failed(e)
        case e: Throwable =>
          Future.failed(new QuineRuntimeFutureException(message, e))
      }
  }

  implicit class ByteBufferOps(private val bb: ByteBuffer) extends AnyVal {
    def remainingBytes: Array[Byte] = {
      val remainder = Array.ofDim[Byte](bb.remaining())
      bb.get(remainder)
      remainder
    }
  }

  implicit final class TimeFuture(private val timer: Timer) extends AnyVal {

    /** Time how long a future takes to complete (success or failure is not differentiated)
      *
      * @param future what to time
      * @param timer how to do the timing
      * @return the future value
      */
    def time[T](future: => Future[T]): Future[T] = {
      val ctx = timer.time()
      val theFuture =
        try future
        catch {
          case NonFatal(err) =>
            ctx.stop()
            throw err
        }
      theFuture.onComplete(_ => ctx.stop())(ExecutionContexts.parasitic)
      theFuture
    }
  }

  /** Make an LRU cache with the specified capacity (not thread-safe) */
  def createLruCache[A](capacity: Int): ju.LinkedHashMap[A, None.type] =
    new java.util.LinkedHashMap[A, None.type](capacity, 1F, true) {
      override def removeEldestEntry(eldest: java.util.Map.Entry[A, None.type]) =
        this.size() >= capacity
    }

}
