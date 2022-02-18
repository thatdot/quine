package com.thatdot.quine.model

/** Moment in time (represented as milliseconds since Jan 1 1970 UTC)
  *
  * These timestamps are designed to be somewhat agnostic to the machine that
  * produced them (they'll be synchronized as much as the computers system
  * clocks are). This "mostly synchronized" property makes it possible to query
  * a historical time at a [[Millisecond]] timestamp and get a reasonable
  * response even when the results are distributed across multiple machines.
  */
final case class Milliseconds(millis: Long) extends AnyVal with Ordered[Milliseconds] {
  override def compare(that: Milliseconds): Int = millis.compare(that.millis)
}

object Milliseconds {

  @inline
  final def currentTime(): Milliseconds = Milliseconds(System.currentTimeMillis())
}
