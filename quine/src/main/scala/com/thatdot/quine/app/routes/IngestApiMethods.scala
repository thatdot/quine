package com.thatdot.quine.app.routes

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Failure
import scala.util.control.NoStackTrace

import org.apache.pekko.stream.{Materializer, StreamDetachedException}

import com.thatdot.quine.app.routes.IngestApiEntities.PauseOperationException
import com.thatdot.quine.graph.{BaseGraph, NamespaceId}
import com.thatdot.quine.routes.{
  IngestStreamConfiguration,
  IngestStreamInfo,
  IngestStreamInfoWithName,
  IngestStreamStatus
}
import com.thatdot.quine.util.SwitchMode

object IngestApiEntities {

  case class PauseOperationException(statusMsg: String) extends Exception with NoStackTrace

  object PauseOperationException {
    object Completed extends PauseOperationException("completed")
    object Terminated extends PauseOperationException("terminated")
    object Failed extends PauseOperationException("failed")
  }
}
trait IngestApiMethods {
  val graph: BaseGraph
  implicit def materializer: Materializer

  def stream2Info(conf: IngestStreamWithControl[IngestStreamConfiguration]): Future[IngestStreamInfo] =
    conf.status.map { status =>
      IngestStreamInfo(
        status,
        conf.terminated().value collect { case Failure(exception) => exception.toString },
        conf.settings,
        conf.metrics.toEndpointResponse
      )
    }(graph.shardDispatcherEC)

  val quineApp: IngestStreamState

  def setIngestStreamPauseState(
    name: String,
    namespace: NamespaceId,
    newState: SwitchMode
  ): Future[Option[IngestStreamInfoWithName]] =
    quineApp.getIngestStream(name, namespace) match {
      case None => Future.successful(None)
      case Some(ingest: IngestStreamWithControl[IngestStreamConfiguration]) =>
        ingest.initialStatus match {
          case IngestStreamStatus.Completed => Future.failed(PauseOperationException.Completed)
          case IngestStreamStatus.Terminated => Future.failed(PauseOperationException.Terminated)
          case IngestStreamStatus.Failed => Future.failed(PauseOperationException.Failed)
          case _ =>
            val flippedValve = ingest.valve().flatMap(_.flip(newState))(graph.nodeDispatcherEC)
            val ingestStatus = flippedValve.flatMap { _ =>
              // HACK: set the ingest's "initial status" to "Paused". `stream2Info` will use this as the stream status
              // when the valve is closed but the stream is not terminated. However, this assignment is not threadsafe,
              // and this directly violates the semantics of `initialStatus`. This should be fixed in a future refactor.
              ingest.initialStatus = IngestStreamStatus.Paused
              stream2Info(ingest)
            }(graph.nodeDispatcherEC)
            ingestStatus.map(status => Some(status.withName(name)))(ExecutionContext.parasitic)
        }
    }

  def mkPauseOperationError[ERROR_TYPE](
    operation: String,
    toError: String => ERROR_TYPE
  ): PartialFunction[Throwable, Either[ERROR_TYPE, Nothing]] = {
    case _: StreamDetachedException =>
      // A StreamDetachedException always occurs when the ingest has failed
      Left(toError(s"Cannot $operation a failed ingest."))
    case e: PauseOperationException =>
      Left(toError(s"Cannot $operation a ${e.statusMsg} ingest."))
  }
}
