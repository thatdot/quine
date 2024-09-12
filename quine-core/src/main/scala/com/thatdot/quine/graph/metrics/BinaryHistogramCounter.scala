package com.thatdot.quine.graph.metrics

import com.codahale.metrics.{Counter, MetricRegistry, NoopMetricRegistry}

import com.thatdot.quine.util.Log.{Safe, SafeLoggableInterpolator, StrictSafeLogging}

/** Histogram where elements can be added or removed
  *
  * Hard-codes buckets for the following intervals:
  *
  *   - `[1, 8)`
  *   - `[8, 128)`
  *   - `[128, 2048)`
  *   - `[2048, 16384)`
  *   - `[16384, +Infinity)`
  */
class BinaryHistogramCounter(
  bucket1to8: Counter,
  bucket8to128: Counter,
  bucket128to2048: Counter,
  bucket2048to16384: Counter,
  bucket16384toInfinity: Counter,
) extends StrictSafeLogging {

  /** Returns the counter that tracks how many instances of the provided `count` value exist.
    * Use only with great care -- for most of a resource's lifecycle, `increment` and `decrement` should be used.
    * Used to configure the histogram to track (or stop tracking) a value at a potentially-nonzero value,
    * for example, the number of properties on a node, which may be nonzero when the node is slept to persistence.
    */
  def bucketContaining(count: Int): Counter =
    if (count == 0) BinaryHistogramCounter.noopCounter
    else if (count < 0) {
      // This should never be hit, and indicates a bug.
      logger.info(
        safe"Negative count ${Safe(count.toString)} cannot be used with a binary histogram counter. Delegating to no-op counter instead.",
      )
      BinaryHistogramCounter.noopCounter
    } else if (count < 8) bucket1to8
    else if (count < 128) bucket8to128
    else if (count < 2048) bucket128to2048
    else if (count < 16384) bucket2048to16384
    else bucket16384toInfinity

  /** Adds a count to the appropriate bucket, managing transitions between buckets.
    * @param previousCount the _previous_ value of the count being incremented
    */
  def increment(previousCount: Int): Unit =
    previousCount + 1 match {
      case 1 =>
        bucket1to8.inc()

      case 8 =>
        bucket1to8.dec()
        bucket8to128.inc()

      case 128 =>
        bucket8to128.dec()
        bucket128to2048.inc()

      case 2048 =>
        bucket128to2048.dec()
        bucket2048to16384.inc()

      case 16384 =>
        bucket2048to16384.dec()
        bucket16384toInfinity.inc()

      case _ => ()
    }

  /** Subtracts a count from the appropriate bucket, managing transitions between buckets.
    * @param previousCount the _previous_ value of the count being incremented
    */
  def decrement(previousCount: Int): Unit =
    previousCount match {
      case 1 =>
        bucket1to8.dec()

      case 8 =>
        bucket1to8.inc()
        bucket8to128.dec()

      case 128 =>
        bucket8to128.inc()
        bucket128to2048.dec()

      case 2048 =>
        bucket128to2048.inc()
        bucket2048to16384.dec()

      case 16384 =>
        bucket2048to16384.inc()
        bucket16384toInfinity.dec()

      case _ => ()
    }
}

object BinaryHistogramCounter {
  val noopCounter: Counter = new NoopMetricRegistry().counter("unused-counter-name")

  def apply(
    registry: MetricRegistry,
    name: String,
  ): BinaryHistogramCounter =
    new BinaryHistogramCounter(
      registry.counter(MetricRegistry.name(name, "1-7")),
      registry.counter(MetricRegistry.name(name, "8-127")),
      registry.counter(MetricRegistry.name(name, "128-2047")),
      registry.counter(MetricRegistry.name(name, "2048-16383")),
      registry.counter(MetricRegistry.name(name, "16384-infinity")),
    )
}
