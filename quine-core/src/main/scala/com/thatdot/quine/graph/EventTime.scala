package com.thatdot.quine.graph

import akka.event.LoggingAdapter

import com.typesafe.scalalogging.LazyLogging

import com.thatdot.quine.model.Milliseconds

/** Timestamp for providing a strict total ordering on events observed along one clock. See
  * [[ActorClock]] for a concrete example of such a clock.
  *
  * There are three components to the timestamp:
  *
  *   - '''Most significant 42 bits store milliseconds since Jan 1 1970 UTC.''' This part should be
  *     mostly synchronized across the distributed Quine system, and this is important for being
  *     able to query a historical timestamp and get a mostly consistent response even when results
  *     are distributed across different nodes and machines. This is enough bits to represent
  *     timestamps until 2109 (see `java.time.Instant.EPOCH.plusMillis(1L << 42)` for a more
  *     precise max timestamp).
  *
  *   - '''Middle 14 bits store an actor-local timestamp sequence number.''' This is an artifical
  *     counter for disambiguating times that fall in the same millisecond but which are otherwise
  *     logically different (eg. the node processes multiple messages in a millisecond, and events
  *     that occurred due to different messages get a logically different timestamp thanks to this
  *     sequence number).
  *
  *   - '''Least significant 8 bits store an actor-local event sequence number.''' This is an
  *     artifical counter that makes it possible to give a strict total order to events that
  *     occurred at the same logical time. This makes it possible to give every event in the node
  *     journal a unique key, which enables more efficient persistence layer implementations.
  *
  * @param eventTime an actor local time that can provide a strict total order over events
  */
final case class EventTime private (eventTime: Long) extends AnyVal with Ordered[EventTime] {
  override def compare(that: EventTime): Int =
    java.lang.Long.compareUnsigned(eventTime, that.eventTime)

  import EventTime._

  /** @return an actor-local logical moment in time (millis and timestamp sequence number */
  def logicalTime: Long = eventTime >>> TimestampSequenceOffset

  /** @return millisecond timestamp (since Jan 1 1970 UTC) */
  def milliseconds: Milliseconds = Milliseconds(eventTime >>> MillisOffset)

  /** @return millisecond timestamp (since Jan 1 1970 UTC) as a [[Long]] */
  def millis: Long = eventTime >>> MillisOffset

  /** @return sequence number to order logical different times in the same millisecond */
  def timestampSequence: Long = (eventTime & TimestampSequenceMask) >>> TimestampSequenceOffset

  /** @return sequence number to order events that occur at the same logical time */
  def eventSequence: Long = eventTime & EventSequenceMask

  /** @return time and event sequence numbers (the non-millisecond part of the time) */
  def timestampAndEventSequence: Long = eventTime & (EventSequenceMask | TimestampSequenceMask)

  /** @param logOpt an optional logger. If specified, this will be used to report overflow warnings to the operator
    * @return the next smallest event time
    *
    * @note this is supposed to almost always have the same logical time, but if the event sequence
    * number overflows, it'll increment the logical time too.
    */
  def nextEventTime(logOpt: Option[LoggingAdapter]): EventTime = {
    val nextTime = new EventTime(eventTime + 1L)
    logOpt.foreach { log =>
      if (nextTime.millis != millis) {
        log.warning(
          """Too many operations on this node caused nextEventTime to overflow
            |milliseconds from: {} to: {}. Historical queries for the overflowed
            |millisecond may not reflect all updates.""".stripMargin.replace('\n', ' '),
          millis,
          nextTime.millis
        )
      }
      if (nextTime.timestampSequence != timestampSequence) {
        log.warning(
          "Too many operations on this node caused nextEventTime to overflow timestampSequence from: {} to: {}",
          timestampSequence,
          nextTime.timestampSequence
        )
      }
    }
    nextTime
  }

  /** @return the largest event time that is still in this same millisecond as this event time
    *
    * @note if the timestamp and event sequence are already the max, the output will match the input
    */
  def largestEventTimeInThisMillisecond: EventTime =
    new EventTime(eventTime | TimestampSequenceMask | EventSequenceMask)

  /** Advance time forward
    *
    * @param mustAdvanceLogicalTime must logical time advance? (has anything interesting happened?)
    * @param newMillis new millisecond component
    * @return new event time
    */
  def tick(mustAdvanceLogicalTime: Boolean, newMillis: Long = System.currentTimeMillis()): EventTime = {
    // If real-world time has changed, reset the logical time sequence counter to 0
    val newTimeSequence =
      if (newMillis != millis) 0L
      else if (!mustAdvanceLogicalTime) timestampSequence
      else timestampSequence + 1
    EventTime(newMillis, timestampSequence = newTimeSequence, eventSequence = 0L)
  }
}
object EventTime extends LazyLogging {

  final private val EventSequenceOffset: Int = 0
  final private val EventSequenceBits: Int = 8
  final private[graph] val EventSequenceMask: Long = 0x00000000000000FFL
  final private val EventSequenceMax: Long = 1L << EventSequenceBits

  final private val TimestampSequenceOffset: Int = EventSequenceOffset + EventSequenceBits
  final private val TimestampSequenceBits: Int = 14
  final private[graph] val TimestampSequenceMask: Long = 0x00000000003FFF00L
  final private val TimestampSequenceMax: Long = 1L << TimestampSequenceBits

  final private val MillisOffset: Int = TimestampSequenceOffset + TimestampSequenceBits
  final private val MillisBits: Int = 42
  final private val MillisMax: Long = 1L << MillisBits

  /** Create a new actor event timestamp
    *
    * @note the behavior when `eventSequence` or `timestampSequence` are too large is intentionally
    * to overflow into `timestampSequence` and `milliseconds` respectively. If `milliseconds`
    * overflows (which is much less likely), it just gets cropped.
    *
    * @param milliseconds milliseconds timestamp (since Jan 1 1970 UTC)
    * @param timestampSequence sequence number to order logical different times
    * @param eventSequence sequence number used to order events with the same logical time
    */
  final def apply(
    milliseconds: Long,
    timestampSequence: Long = 0L,
    eventSequence: Long = 0L
  ): EventTime = {
    val time = new EventTime(
      (milliseconds << MillisOffset) +
      (timestampSequence << TimestampSequenceOffset) +
      eventSequence
    )

    // Warn on various overflows
    if (milliseconds < 0L || MillisMax <= milliseconds) {
      logger.error(s"Milliseconds $milliseconds in $time needs to be between 0 and $MillisMax")
    }
    if (timestampSequence < 0L || TimestampSequenceMax <= timestampSequence) {
      logger.warn(s"Timestamp sequence number $timestampSequence in $time overflowed")
    }
    if (eventSequence < 0L || EventSequenceMax <= eventSequence) {
      logger.warn(s"Event sequence number $eventSequence in $time overflowed")
    }

    time
  }

  /** Wrap a [[Long]] known to have the right bit-wise structure into an actor timestamp */
  @inline
  final def fromRaw(eventTime: Long): EventTime = new EventTime(eventTime)

  final def fromMillis(millis: Milliseconds): EventTime = apply(millis.millis, 0L, 0L)

  val MinValue: EventTime = EventTime.fromRaw(0L)

  val MaxValue: EventTime = EventTime.fromRaw(-1)
}
