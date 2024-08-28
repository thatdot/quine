package com.thatdot.quine.util

import com.thatdot.quine.graph.cypher.CypherException
import com.thatdot.quine.graph.messaging.ExactlyOnceTimeoutException
import com.thatdot.quine.graph.{GraphNotReadyException, QuineRuntimeFutureException, ShardNotAvailableException}
import com.thatdot.quine.persistor.WrappedPersistorException

// Represents either a BaseError or a generic Error
sealed trait AnyError {
  def toThrowable: Throwable
}

// A base for a finite set of enumerable errors.
// Excludes GenericError for cases where we want to be able to know the exact error
sealed trait BaseError extends AnyError

// The base for all errors that originate in Quine
sealed trait QuineError extends BaseError

// The base for all errors that originate outside of Quine
sealed trait ExternalError extends BaseError

object AnyError {
  def fromThrowable(e: Throwable): AnyError = BaseError
    .fromThrowable(e)
    .getOrElse(
      GenericError(e.getClass.getName, e.getMessage, e.getStackTrace, Option(e.getCause).map(fromThrowable))
    )

  final case class GenericError(
    exceptionType: String,
    message: String,
    stack: Array[StackTraceElement],
    cause: Option[AnyError]
  ) extends AnyError {
    def toThrowable: Throwable = {
      val e = new Throwable(message, cause.map(_.toThrowable).orNull)
      e.setStackTrace(stack.toArray)
      e
    }
  }
}

object BaseError {
  def fromThrowable(e: Throwable): Option[BaseError] = QuineError.fromThrowable(e) match {
    case Some(value) => Some(value)
    case None => ExternalError.fromThrowable(e)
  }
}
object ExternalError {
  def fromThrowable(e: Throwable): Option[ExternalError] = e match {
    case e: org.apache.pekko.stream.RemoteStreamRefActorTerminatedException =>
      Some(RemoteStreamRefActorTerminatedError(e))
    case e: org.apache.pekko.stream.StreamRefSubscriptionTimeoutException => Some(StreamRefSubscriptionTimeoutError(e))
    case e: org.apache.pekko.stream.InvalidSequenceNumberException => Some(InvalidSequenceNumberError(e))
    case _ => None
  }

  final case class RemoteStreamRefActorTerminatedError(
    toThrowable: org.apache.pekko.stream.RemoteStreamRefActorTerminatedException
  ) extends ExternalError
  final case class StreamRefSubscriptionTimeoutError(
    toThrowable: org.apache.pekko.stream.StreamRefSubscriptionTimeoutException
  ) extends ExternalError
  final case class InvalidSequenceNumberError(toThrowable: org.apache.pekko.stream.InvalidSequenceNumberException)
      extends ExternalError
}

object QuineError {
  def fromThrowable(e: Throwable): Option[QuineError] = e match {
    case e: ExactlyOnceTimeoutException => Some(ExactlyOnceTimeoutError(e))
    case e: CypherException => Some(CypherError(e))
    case e: QuineRuntimeFutureException => Some(QuineRuntimeFutureError(e))
    case e: GraphNotReadyException => Some(GraphNotReadyError(e))
    case e: ShardNotAvailableException => Some(ShardNotAvailableError(e))
    case e: WrappedPersistorException => Some(WrappedPersisterError(e))
    case _ => None
  }

  final case class CypherError(toThrowable: CypherException) extends QuineError
  final case class QuineRuntimeFutureError(toThrowable: QuineRuntimeFutureException) extends QuineError
  final case class GraphNotReadyError(toThrowable: GraphNotReadyException) extends QuineError
  final case class ShardNotAvailableError(toThrowable: ShardNotAvailableException) extends QuineError
  final case class ExactlyOnceTimeoutError(toThrowable: ExactlyOnceTimeoutException) extends QuineError
  final case class WrappedPersisterError(toThrowable: WrappedPersistorException) extends QuineError
}
