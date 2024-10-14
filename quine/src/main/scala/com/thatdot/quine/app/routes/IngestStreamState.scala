package com.thatdot.quine.app.routes
import scala.concurrent.Future
import scala.util.{Failure, Try}

import org.apache.pekko.util.Timeout

import com.thatdot.quine.app.NamespaceNotFoundException
import com.thatdot.quine.app.ingest.QuineIngestSource
import com.thatdot.quine.app.ingest2.source.{DecodedSource, QuineValueIngestQuery}
import com.thatdot.quine.app.serialization.{AvroSchemaCache, ProtobufSchemaCache}
import com.thatdot.quine.app.v2api.endpoints.V2IngestEntities.{
  QuineIngestConfiguration => V2IngestConfiguration,
  QuineIngestStreamWithStatus,
}
import com.thatdot.quine.graph.{CypherOpsGraph, MemberIdx, NamespaceId, defaultNamespaceId, namespaceToString}
import com.thatdot.quine.routes._
import com.thatdot.quine.util.Log._
import com.thatdot.quine.util.SwitchMode

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

  /** Add an ingest stream to the running application. The ingest may be new or restored from persistence.
    *
    * TODO these two concerns should be separated into two methods, or at least two signatures -- there are too many
    * dependencies between parameters.
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
    * @param useV2Ingest                 While this method adds V1 ingest streams, this flag is a _temporary_ addition that
    *                                     uses the v2 code path to create ingests. Since this is created from the V1 config
    *                                     [[IngestStreamConfiguration]] there are some V2 constructs and variations that
    *                                     are unavailable.
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

  /** Create ingest stream using updated V2 Ingest api. */
  def addV2IngestStream(
    name: String,
    settings: V2IngestConfiguration,
    intoNamespace: NamespaceId,
    previousStatus: Option[IngestStreamStatus], // previousStatus is None if stream was not restored at all
    shouldResumeRestoredIngests: Boolean,
    timeout: Timeout,
    shouldSaveMetadata: Boolean = true,
    memberIdx: Option[MemberIdx],
  )(implicit logConfig: LogConfig): Try[Boolean]

  private def determineSwitchModeAndStatus(
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
  ): Try[QuineIngestSource] =
    ingestStreams.get(intoNamespace) match {
      // TODO Note for review comparison: v1 version fails silently here.
      // TODO Also, shouldn't this just add the namespace if it's not found?
      case None => Failure(NamespaceNotFoundException(intoNamespace))
      case Some(ingests) if ingests.contains(name) =>
        Failure(
          new Exception(s"Ingest $name already exists in namespace ${namespaceToString(intoNamespace)}"),
        ) // Ingest already exists.
      case Some(ingests) =>
        Try {
          val (initialValveSwitchMode, initialStatus) =
            determineSwitchModeAndStatus(previousStatus, shouldResumeRestoredIngests)

          //TODO should return  ValidatedNel[IngestName, DecodedSource]
          val decodedSource: DecodedSource = DecodedSource.apply(name, settings, meter, graph.system)(
            protobufCache,
            avroCache,
            logConfig,
          )
          val quineIngestSource: QuineIngestSource = decodedSource.toQuineIngestSource(
            name,
            QuineValueIngestQuery.apply(settings, graph, intoNamespace),
            graph,
            initialValveSwitchMode,
            settings.parallelism,
            settings.maxPerSecond,
          )

          val streamDefWithControl: IngestStreamWithControl[UnifiedIngestConfiguration] =
            IngestStreamWithControl(
              UnifiedIngestConfiguration(Left(settings)),
              metrics,
              quineIngestSource,
              initialStatus,
            )

          val newNamespaceIngests = ingests + (name -> streamDefWithControl)
          //TODO this is blocking in QuineEnterpriseApp
          ingestStreams += intoNamespace -> newNamespaceIngests

          quineIngestSource
        }
    }

  def getIngestStream(
    name: String,
    namespace: NamespaceId,
  )(implicit logConfig: LogConfig): Option[IngestStreamWithControl[IngestStreamConfiguration]] =
    getIngestStreamFromState(name, namespace).map(isc => isc.copy(settings = isc.settings.asV1Config))

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
}
