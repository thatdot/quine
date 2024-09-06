package com.thatdot.quine.app.ingest2.source

import scala.util.{Failure, Try}

import com.typesafe.scalalogging.LazyLogging

/** Error handler defined for errors that affect only a single record. This is intended to handle errors in
  * a configurable way distinct from stream-level errors, where the entire stream fails - e.g. handling
  * a single corrupt record rather than a failure in the stream communication.
  */
trait SingleRecordFailureErrorHandler {
  def handleError[A, Frame](processRecordAttempt: (Try[A], Frame)): Unit =
    processRecordAttempt match {
      case (Failure(e), frame) => onError(e, frame)
      case _ => ()
    }

  def onError[Frame](e: Throwable, frame: Frame): Unit
}

/** For a single record, simply log and move along... */
object LogOnSingleRecordFailure extends SingleRecordFailureErrorHandler with LazyLogging {
  def onError[Frame](e: Throwable, frame: Frame): Unit =
    logger.warn(s"error decoding: $frame: ${e.getMessage}")
}

object NoOpOnSingleRecordFailure extends SingleRecordFailureErrorHandler with LazyLogging {
  def onError[Frame](e: Throwable, frame: Frame): Unit = ()
}
