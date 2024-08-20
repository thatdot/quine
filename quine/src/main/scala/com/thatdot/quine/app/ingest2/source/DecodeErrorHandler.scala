package com.thatdot.quine.app.ingest2.source

import scala.util.{Failure, Try}

import com.typesafe.scalalogging.LazyLogging

/** Handle in-stream errors.
  *
  * We do not filter error values since we will need them later in the stream
  * for ack .
  */
trait DecodeErrorHandler {
  def handleError[A, Frame](decodeAttempt: (Try[A], Frame)): Unit =
    decodeAttempt match {
      case (Failure(e), frame) => onError(e, frame)
      case _ => ()
    }

  def onError[Frame](e: Throwable, frame: Frame): Unit
}

//TODO DecodeErrorHandler should have a logging type as an option, a dead-letter type as an option...
object LoggingDecodeErrorHandler extends DecodeErrorHandler with LazyLogging {
  def onError[Frame](e: Throwable, frame: Frame): Unit =
    logger.warn(s"error decoding: $frame: ${e.getMessage}")

}
