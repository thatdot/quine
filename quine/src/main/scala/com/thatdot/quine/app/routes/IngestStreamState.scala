package com.thatdot.quine.app.routes
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

import org.apache.pekko.Done
import org.apache.pekko.stream.Materializer
import org.apache.pekko.util.Timeout

import cats.data.Validated.{invalidNel, validNel}
import cats.data.ValidatedNel

import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.quine.app.model.ingest.QuineIngestSource
import com.thatdot.quine.app.model.ingest2.V2IngestEntities.{
  QuineIngestConfiguration => V2IngestConfiguration,
  QuineIngestStreamWithStatus,
  Transformation,
}
import com.thatdot.quine.app.model.ingest2.source.{DecodedSource, QuineValueIngestQuery}
import com.thatdot.quine.app.model.ingest2.sources.WebSocketFileUploadSource
import com.thatdot.quine.app.model.ingest2.{V1ToV2, V2IngestEntities}
import com.thatdot.quine.app.model.transformation.polyglot
import com.thatdot.quine.app.model.transformation.polyglot.langauges.JavaScriptTransformation
import com.thatdot.quine.app.util.QuineLoggables._
import com.thatdot.quine.exceptions.{DuplicateIngestException, NamespaceNotFoundException}
import com.thatdot.quine.graph.{CypherOpsGraph, MemberIdx, NamespaceId, defaultNamespaceId, namespaceToString}
import com.thatdot.quine.routes._
import com.thatdot.quine.serialization.{AvroSchemaCache, ProtobufSchemaCache}
import com.thatdot.quine.util.{BaseError, SwitchMode}

/** Store ingests allowing for either v1 or v2 types. */
case class UnifiedIngestConfiguration(config: Either[V2IngestConfiguration, IngestStreamConfiguration]) {
  def asV1Config: IngestStreamConfiguration = config match {
    case Left(v2) => v2.asV1IngestStreamConfiguration
    case Right(v1) => v1
  }
}

trait IngestStreamState {
  type IngestName = String
  @volatile
  protected var ingestStreams: Map[NamespaceId, Map[IngestName, IngestStreamWithControl[UnifiedIngestConfiguration]]] =
    Map(defaultNamespaceId -> Map.empty)

  def defaultExecutionContext: ExecutionContext
  implicit def materializer: Materializer

  /** Add a new ingest stream to the running application.
    *
    * @param name                        Name of the stream to add
    * @param settings                    Configuration for the stream
    * @param intoNamespace               Namespace into which the stream should ingest data
    * @param previousStatus              Some previous status of the stream, if it was restored from persistence.
    *                                    None for new ingests
    * @param shouldResumeRestoredIngests If restoring an ingest, should the ingest be resumed? When `previousStatus`
    *                                    is None, this has no effect.
    * @param timeout                     How long to allow for the attempt to persist the stream to the metadata table
    *                                    (when shouldSaveMetadata = true). Has no effect if !shouldSaveMetadata
    * @param shouldSaveMetadata          Whether the application should persist this stream to the metadata table.
    *                                    This should be false when restoring from persistence (i.e., from the metadata
    *                                    table) and true otherwise.
    * @param memberIdx                   The cluster member index on which this ingest is being created
    * @return Success(true) when the operation was successful, or a Failure otherwise
    */
  def addIngestStream(
    name: String,
    settings: IngestStreamConfiguration,
    intoNamespace: NamespaceId,
    previousStatus: Option[IngestStreamStatus],
    shouldResumeRestoredIngests: Boolean,
    timeout: Timeout,
    shouldSaveMetadata: Boolean = true,
    memberIdx: Option[MemberIdx] = None,
  ): Try[Boolean]

  /** Create ingest stream using updated V2 Ingest api.
    */
  def addV2IngestStream(
    name: String,
    settings: V2IngestConfiguration,
    intoNamespace: NamespaceId,
    timeout: Timeout,
    memberIdx: MemberIdx,
  )(implicit logConfig: LogConfig): Future[Either[Seq[String], Unit]]

  /** Create an ingest stream on this member.
    */
  def createV2IngestStream(
    name: String,
    settings: V2IngestConfiguration,
    intoNamespace: NamespaceId,
    timeout: Timeout,
  )(implicit logConfig: LogConfig): ValidatedNel[BaseError, Unit]

  /** Restore a previously created ingest
    *
    * @param name                        Name of the stream to add
    * @param settings                    Configuration for the stream
    * @param intoNamespace               Namespace into which the stream should ingest data
    * @param previousStatus              Some previous status of the stream, if it was restored from persistence.
    * @param shouldResumeRestoredIngests If restoring an ingest, should the ingest be resumed? When `previousStatus`
    *                                    is None, this has no effect.
    * @param timeout                     How long to allow for the attempt to persist the stream to the metadata table
    *                                    (when shouldSaveMetadata = true). Has no effect if !shouldSaveMetadata
    * @param thisMemberIdx               This cluster member's index in case the graph is still initializing.
    * @return Success when the operation was successful, or a Failure otherwise
    */
  def restoreV2IngestStream(
    name: String,
    settings: V2IngestConfiguration,
    intoNamespace: NamespaceId,
    previousStatus: Option[IngestStreamStatus],
    shouldResumeRestoredIngests: Boolean,
    timeout: Timeout,
    thisMemberIdx: MemberIdx,
  )(implicit logConfig: LogConfig): ValidatedNel[BaseError, Unit]

  protected def determineSwitchModeAndStatus(
    previousStatus: Option[IngestStreamStatus],
    shouldResumeRestoredIngests: Boolean,
  ): (SwitchMode, IngestStreamStatus) =
    previousStatus match {
      case None =>
        // This is a freshly-created ingest, so there is no status to restore
        SwitchMode.Open -> IngestStreamStatus.Running
      case Some(lastKnownStatus) =>
        val newStatus = IngestStreamStatus.decideRestoredStatus(lastKnownStatus, shouldResumeRestoredIngests)
        val switchMode = newStatus.position match {
          case ValvePosition.Open => SwitchMode.Open
          case ValvePosition.Closed => SwitchMode.Close
        }
        switchMode -> newStatus
    }

  /** Attempt to create a [[QuineIngestSource]] from configuration and
    * stream components.
    *
    * If created, the existing ingestSource will exist in the
    * ingestStreams state map.
    *
    *  This method must be called within a synchronized since it makes
    *  changes to the shared saved state of the ingest map (and,eventually, persistence).
    *
    * Fails
    * - if the namespace doesn't exist in the state map
    * - if the named source already exists.
    */
  def createV2IngestSource(
    name: String,
    settings: V2IngestConfiguration,
    intoNamespace: NamespaceId,
    previousStatus: Option[IngestStreamStatus], // previousStatus is None if stream was not restored at all
    shouldResumeRestoredIngests: Boolean,
    metrics: IngestMetrics,
    meter: IngestMeter,
    graph: CypherOpsGraph,
  )(implicit
    protobufCache: ProtobufSchemaCache,
    avroCache: AvroSchemaCache,
    logConfig: LogConfig,
  ): ValidatedNel[BaseError, QuineIngestSource] =
    ingestStreams.get(intoNamespace) match {
      // TODO Note for review comparison: v1 version fails silently here.
      // TODO Also, shouldn't this just add the namespace if it's not found?
      case None => invalidNel(NamespaceNotFoundException(intoNamespace))
      // Ingest already exists.
      case Some(ingests) if ingests.contains(name) =>
        invalidNel(DuplicateIngestException(name, Some(namespaceToString(intoNamespace))))
      case Some(ingests) =>
        val (initialValveSwitchMode, initialStatus) =
          determineSwitchModeAndStatus(previousStatus, shouldResumeRestoredIngests)

        val decodedSourceNel: ValidatedNel[BaseError, DecodedSource] =
          DecodedSource.apply(name, settings, meter, graph.system)(
            protobufCache,
            avroCache,
            logConfig,
          )

        val validatedTransformation: ValidatedNel[BaseError, Option[polyglot.Transformation]] =
          settings.transformation.fold(
            validNel(Option.empty): ValidatedNel[BaseError, Option[polyglot.Transformation]],
          ) { case Transformation.JavaScript(function) =>
            JavaScriptTransformation.makeInstance(function) match {
              case Left(err) => invalidNel(err)
              case Right(value) => validNel(Some(value))
            }
          }

        validatedTransformation.andThen { transformation =>
          decodedSourceNel.map { (s: DecodedSource) =>

            val errorOutputs =
              s.getDeadLetterQueues(settings.onRecordError.deadLetterQueueSettings)(protobufCache, graph.system)

            val quineIngestSource: QuineIngestSource = s.toQuineIngestSource(
              name,
              QuineValueIngestQuery.apply(settings, graph, intoNamespace),
              transformation,
              graph,
              initialValveSwitchMode,
              settings.parallelism,
              settings.maxPerSecond,
              onDecodeError = errorOutputs,
              retrySettings = settings.onRecordError.retrySettings,
              logRecordError = settings.onRecordError.logRecord,
              onStreamErrorHandler = settings.onStreamError,
            )

            val streamDefWithControl: IngestStreamWithControl[UnifiedIngestConfiguration] =
              IngestStreamWithControl(
                UnifiedIngestConfiguration(Left(settings)),
                metrics,
                quineIngestSource,
                initialStatus,
              )

            // For V2 WebSocket file upload, extract and store the packaging in optWsV2
            s match {
              case wsUpload: WebSocketFileUploadSource =>
                streamDefWithControl.optWsV2 = Some(wsUpload.decodingHub)
              case _ => // Other source types don't need special handling
            }

            val newNamespaceIngests = ingests + (name -> streamDefWithControl)
            //TODO this is blocking in QuineEnterpriseApp
            ingestStreams += intoNamespace -> newNamespaceIngests

            quineIngestSource
          }
        }
    }

  def getIngestStream(
    name: String,
    namespace: NamespaceId,
  )(implicit logConfig: LogConfig): Option[IngestStreamWithControl[IngestStreamConfiguration]] =
    getIngestStreamFromState(name, namespace).map(isc => isc.copy(settings = isc.settings.asV1Config))

  def getV2IngestStream(
    name: String,
    namespace: NamespaceId,
    memberIdx: MemberIdx,
  )(implicit logConfig: LogConfig): Future[Option[V2IngestEntities.IngestStreamInfoWithName]]

  /** Get the unified ingest stream stored in memory. The value returned here will _not_ be a copy.
    * Note: Once v1 and v2 ingests are no longer both supported, distinguishing this method from
    * [[getIngestStream]] should no longer be necessary.
    */
  def getIngestStreamFromState(
    name: String,
    namespace: NamespaceId,
  ): Option[IngestStreamWithControl[UnifiedIngestConfiguration]] =
    ingestStreams.getOrElse(namespace, Map.empty).get(name)

  def getIngestStreams(namespace: NamespaceId): Map[String, IngestStreamWithControl[IngestStreamConfiguration]]

  def getV2IngestStreams(
    namespace: NamespaceId,
    memberIdx: MemberIdx,
  ): Future[Map[String, V2IngestEntities.IngestStreamInfo]]

  protected def getIngestStreamsFromState(
    namespace: NamespaceId,
  ): Map[IngestName, IngestStreamWithControl[UnifiedIngestConfiguration]] =
    ingestStreams
      .getOrElse(namespace, Map.empty)

  protected def getIngestStreamsWithStatus(
    namespace: NamespaceId,
  ): Future[Map[String, Either[IngestStreamWithStatus, QuineIngestStreamWithStatus]]]

  def removeIngestStream(
    name: String,
    namespace: NamespaceId,
  ): Option[IngestStreamWithControl[IngestStreamConfiguration]]

  def removeV2IngestStream(
    name: String,
    namespace: NamespaceId,
    memberIdx: MemberIdx,
  ): Future[Option[V2IngestEntities.IngestStreamInfoWithName]]

  def pauseV2IngestStream(
    name: String,
    namespace: NamespaceId,
    memberIdx: MemberIdx,
  ): Future[Option[V2IngestEntities.IngestStreamInfoWithName]]

  def unpauseV2IngestStream(
    name: String,
    namespace: NamespaceId,
    memberIdx: MemberIdx,
  ): Future[Option[V2IngestEntities.IngestStreamInfoWithName]]

  /** Close the ingest stream and return a future that completes when the stream terminates, including an error message
    * if any.
    */
  def terminateIngestStream(stream: IngestStreamWithControl[_]): Future[Option[String]] = {
    stream.close()
    stream
      .terminated()
      .flatMap { innerFuture =>
        innerFuture
          .map { case Done => None }(ExecutionContext.parasitic)
          .recover(e => Some(e.toString))(ExecutionContext.parasitic)
      }(ExecutionContext.parasitic)
  }

  protected def setIngestStreamPauseState(
    name: String,
    namespace: NamespaceId,
    newState: SwitchMode,
  )(implicit logConfig: LogConfig): Future[Option[V2IngestEntities.IngestStreamInfoWithName]] =
    getIngestStreamFromState(name, namespace) match {
      case None => Future.successful(None)
      case Some(ingest: IngestStreamWithControl[UnifiedIngestConfiguration]) =>
        ingest.initialStatus match {
          case IngestStreamStatus.Completed =>
            Future.failed(IngestApiEntities.PauseOperationException.Completed)
          case IngestStreamStatus.Terminated =>
            Future.failed(IngestApiEntities.PauseOperationException.Terminated)
          case IngestStreamStatus.Failed =>
            Future.failed(IngestApiEntities.PauseOperationException.Failed)
          case _ =>
            val flippedValve = ingest.valve().flatMap(_.flip(newState))(defaultExecutionContext)
            val ingestStatus = flippedValve.flatMap { _ =>
              // HACK: set the ingest's "initial status" to "Paused". `stream2Info` will use this as the stream status
              // when the valve is closed but the stream is not terminated. However, this assignment is not threadsafe,
              // and this directly violates the semantics of `initialStatus`. This should be fixed in a future refactor.
              ingest.initialStatus = IngestStreamStatus.Paused
              streamToInternalModel(ingest.copy(settings = V2IngestEntities.IngestSource(ingest.settings)))
            }(defaultExecutionContext)
            ingestStatus.map(status => Some(status.withName(name)))(ExecutionContext.parasitic)
        }
    }

  protected def streamToInternalModel(
    stream: IngestStreamWithControl[V2IngestEntities.IngestSource],
  ): Future[V2IngestEntities.IngestStreamInfo] =
    stream.status
      .map { status =>
        V2IngestEntities.IngestStreamInfo(
          V1ToV2(status),
          stream
            .terminated()
            .value
            .collect { case Success(innerFuture) =>
              innerFuture.value.flatMap {
                case Success(_) => None
                case Failure(exception) => Some(exception.getMessage)
              }
            }
            .flatten,
          stream.settings,
          V1ToV2(stream.metrics.toEndpointResponse),
        )
      }(defaultExecutionContext)

  protected def unifiedIngestStreamToInternalModel(
    conf: IngestStreamWithControl[UnifiedIngestConfiguration],
  )(implicit logConfig: LogConfig): Future[Option[V2IngestEntities.IngestStreamInfo]] = conf match {
    case IngestStreamWithControl(
          UnifiedIngestConfiguration(Left(v2Config: V2IngestConfiguration)),
          metrics,
          valve,
          terminated,
          close,
          initialStatus,
          optWs,
          optWsV2,
        ) =>
      val ingestV2 = IngestStreamWithControl[V2IngestEntities.IngestSource](
        v2Config.source,
        metrics,
        valve,
        terminated,
        close,
        initialStatus,
        optWs,
        optWsV2,
      )
      streamToInternalModel(ingestV2).map(Some(_))(ExecutionContext.parasitic)
    case _ => Future.successful(None)
  }

  protected def determineFinalStatus(statusAtTermination: IngestStreamStatus): IngestStreamStatus = {
    import com.thatdot.quine.routes.IngestStreamStatus._
    statusAtTermination match {
      // in these cases, the ingest was healthy and runnable/running
      case Running | Paused | Restored => Terminated
      // in these cases, the ingest was not running/runnable
      case Completed | Failed | Terminated => statusAtTermination
    }
  }
}
