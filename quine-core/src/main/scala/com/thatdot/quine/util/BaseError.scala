package com.thatdot.quine.util

import scala.util.control.NoStackTrace

import com.thatdot.quine.exceptions.{
  DuplicateIngestException,
  FileIngestSecurityException,
  KafkaValidationException,
  NamespaceNotFoundException,
  ShardIterationException,
}
import com.thatdot.quine.graph.cypher.CypherException
import com.thatdot.quine.graph.messaging.ExactlyOnceTimeoutException
import com.thatdot.quine.graph.{GraphNotReadyException, QuineRuntimeFutureException, ShardNotAvailableException}
import com.thatdot.quine.persistor.WrappedPersistorException

// Represents either a BaseError or a generic Error
sealed trait AnyError extends Throwable {}

// A base for a finite set of enumerable errors.
// Excludes GenericError for cases where we want to be able to know the exact error
// Any new class that extends QuineError or External Error should be added to the corresponding fromThrowable
// and the pickler
sealed trait BaseError extends AnyError

// The base for all errors that originate in Quine
trait QuineError extends BaseError with NoStackTrace

// The base for all errors that originate outside of Quine
trait ExternalError extends BaseError {
  def ofError: Throwable

  override def fillInStackTrace(): Throwable = {
    ofError.fillInStackTrace()
    this
  }

  override def getStackTrace: Array[StackTraceElement] = ofError.getStackTrace
}

object AnyError {
  def fromThrowable(e: Throwable): AnyError = BaseError
    .fromThrowable(e)
    .getOrElse(
      GenericError(e.getClass.getName, e.getMessage, e.getStackTrace, Option(e.getCause).map(fromThrowable)),
    )

  final case class GenericError(
    exceptionType: String,
    message: String,
    stack: Array[StackTraceElement],
    cause: Option[AnyError],
  ) extends Throwable(message, cause.orNull)
      with AnyError {
    override def fillInStackTrace(): Throwable = this
    override def getStackTrace: Array[StackTraceElement] = stack
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
    ofError: org.apache.pekko.stream.RemoteStreamRefActorTerminatedException,
  ) extends ExternalError
  final case class StreamRefSubscriptionTimeoutError(
    ofError: org.apache.pekko.stream.StreamRefSubscriptionTimeoutException,
  ) extends ExternalError
  final case class InvalidSequenceNumberError(ofError: org.apache.pekko.stream.InvalidSequenceNumberException)
      extends ExternalError
}

object QuineError {
  def fromThrowable(e: Throwable): Option[QuineError] = e match {
    case e: ExactlyOnceTimeoutException => Some(e)
    case e: CypherException => Some(e)
    case e: QuineRuntimeFutureException => Some(e)
    case e: GraphNotReadyException => Some(e)
    case e: ShardNotAvailableException => Some(e)
    case e: WrappedPersistorException => Some(e)
    case e: NamespaceNotFoundException => Some(e)
    case e: DuplicateIngestException => Some(e)
    case e: ShardIterationException => Some(e)
    case e: KafkaValidationException => Some(e)
    case e: FileIngestSecurityException => Some(e)
    case _ => None
  }
}
