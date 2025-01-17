package com.thatdot.quine.app.routes

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

import org.apache.pekko.Done
import org.apache.pekko.http.scaladsl.server.Directives._
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.stream.Materializer
import org.apache.pekko.util.Timeout

import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.quine.app.ingest.util.KafkaSettingsValidator
import com.thatdot.quine.exceptions.NamespaceNotFoundException
import com.thatdot.quine.graph.NamespaceId
import com.thatdot.quine.routes._
import com.thatdot.quine.util.SwitchMode

/** The Pekko HTTP implementation of [[IngestRoutes]] */
trait IngestRoutesImpl
    extends IngestRoutes
    with com.thatdot.quine.app.routes.exts.PekkoQuineEndpoints
    with IngestApiMethods
    with endpoints4s.pekkohttp.server.Endpoints
    with com.thatdot.quine.app.routes.exts.circe.JsonEntitiesFromSchemas {

  implicit def timeout: Timeout
  implicit def materializer: Materializer

  val quineApp: IngestStreamState

  /** Try to register a new ingest stream.
    * The Either represents a bad request on the Left, and the inner Option represents Some(success) or that the
    * namespace was not found (404).
    */
  implicit protected def logConfig: LogConfig
  private val ingestStreamStartRoute: Route = {
    val http404: Either[ClientErrors, Option[Nothing]] = Right(None)
    def http400(errors: ClientErrors): Either[ClientErrors, Option[Nothing]] = Left(errors)
    def httpSuccess[A](a: A): Either[ClientErrors, Option[A]] = Right(Some(a))
    def addSettings(
      name: String,
      intoNamespace: NamespaceId,
      settings: IngestStreamConfiguration,
    ): Either[ClientErrors, Option[Unit]] =
      quineApp.addIngestStream(
        name,
        settings,
        intoNamespace,
        previousStatus = None, // this ingest is being created, not restored, so it has no previous status
        shouldResumeRestoredIngests = false,
        timeout,
        memberIdx = None,
      ) match {
        case Success(false) =>
          http400(
            endpoints4s.Invalid(
              s"Cannot create ingest stream `$name` (a stream with this name already exists)",
            ),
          )
        case Success(true) => httpSuccess(())
        case Failure(_: NamespaceNotFoundException) => http404
        case Failure(err) => http400(endpoints4s.Invalid(s"Failed to create ingest stream `$name`: ${err.getMessage}"))
      }

    ingestStreamStart.implementedBy {
      case (ingestName, namespaceParam, settings: KafkaIngest) =>
        graph.requiredGraphIsReady()
        val namespace = namespaceFromParam(namespaceParam)
        KafkaSettingsValidator.validateInput(
          settings.kafkaProperties,
          settings.groupId,
          settings.offsetCommitting,
        ) match {
          case Some(errors) =>
            http400(
              endpoints4s.Invalid(
                s"Cannot create ingest stream `$ingestName`: ${errors.toList.mkString(",")}",
              ),
            )
          case None => addSettings(ingestName, namespace, settings)
        }
      case (ingestName, namespaceParam, settings) =>
        graph.requiredGraphIsReady()
        val namespace = namespaceFromParam(namespaceParam)
        addSettings(ingestName, namespace, settings)
    }
  }

  /** Try to stop an ingest stream */
  private val ingestStreamStopRoute = ingestStreamStop.implementedByAsync { case (ingestName, namespaceParam) =>
    graph.requiredGraphIsReadyFuture {
      quineApp.removeIngestStream(ingestName, namespaceFromParam(namespaceParam)) match {
        case None => Future.successful(None)
        case Some(
              control @ IngestStreamWithControl(
                settings,
                metrics,
                valve @ _,
                terminated,
                close,
                initialStatus @ _,
                optWs @ _,
              ),
            ) =>
          val finalStatus = control.status.map { previousStatus =>
            import IngestStreamStatus._
            previousStatus match {
              // in these cases, the ingest was healthy and runnable/running
              case Running | Paused | Restored => Terminated
              // in these cases, the ingest was not running/runnable
              case Completed | Failed | Terminated => previousStatus
            }
          }(ExecutionContext.parasitic)

          val terminationMessage: Future[Option[String]] = {
            // start terminating the ingest
            close()
            // future will return when termination finishes
            terminated()
              .flatMap(t =>
                t
                  .map({ case Done => None })(graph.shardDispatcherEC)
                  .recover({ case e =>
                    Some(e.toString)
                  })(graph.shardDispatcherEC),
              )(graph.shardDispatcherEC)
          }

          finalStatus
            .zip(terminationMessage)
            .map { case (newStatus, message) =>
              Some(
                IngestStreamInfoWithName(
                  ingestName,
                  newStatus,
                  message,
                  settings,
                  metrics.toEndpointResponse,
                ),
              )
            }(graph.shardDispatcherEC)
      }
    }
  }

  /** Query out a particular ingest stream */
  private val ingestStreamLookupRoute = ingestStreamLookup.implementedByAsync { case (ingestName, namespaceParam) =>
    graph.requiredGraphIsReadyFuture {
      quineApp.getIngestStream(ingestName, namespaceFromParam(namespaceParam)) match {
        case None => Future.successful(None)
        case Some(stream) => stream2Info(stream).map(s => Some(s.withName(ingestName)))(graph.shardDispatcherEC)
      }
    }
  }

  /** List out all of the currently active ingest streams */
  private val ingestStreamListRoute = ingestStreamList.implementedByAsync { namespaceParam =>
    graph.requiredGraphIsReadyFuture {
      Future
        .traverse(
          quineApp.getIngestStreams(namespaceFromParam(namespaceParam)).toList,
        ) { case (name, ingest) =>
          stream2Info(ingest).map(name -> _)(graph.shardDispatcherEC)
        }(implicitly, graph.shardDispatcherEC)
        .map(_.toMap)(graph.shardDispatcherEC)
    }
  }

  private val ingestStreamPauseRoute = ingestStreamPause.implementedByAsync { case (ingestName, namespaceParam) =>
    graph.requiredGraphIsReadyFuture {
      setIngestStreamPauseState(ingestName, namespaceFromParam(namespaceParam), SwitchMode.Close)
        .map(Right(_))(ExecutionContext.parasitic)
        .recover(mkPauseOperationError("pause", endpoints4s.Invalid(_)))(ExecutionContext.parasitic)
    }
  }

  private val ingestStreamUnpauseRoute = ingestStreamUnpause.implementedByAsync { case (ingestName, namespaceParam) =>
    graph.requiredGraphIsReadyFuture {
      setIngestStreamPauseState(ingestName, namespaceFromParam(namespaceParam), SwitchMode.Open)
        .map(Right(_))(ExecutionContext.parasitic)
        .recover(mkPauseOperationError("resume", endpoints4s.Invalid(_)))(ExecutionContext.parasitic)
    }
  }

  final val ingestRoutes: Route = {
    ingestStreamStartRoute ~
    ingestStreamStopRoute ~
    ingestStreamLookupRoute ~
    ingestStreamListRoute ~
    ingestStreamPauseRoute ~
    ingestStreamUnpauseRoute
  }
}
