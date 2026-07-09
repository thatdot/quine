package com.thatdot.quine.app.v2api.endpoints

import scala.concurrent.{ExecutionContext, Future}

import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.syntax.EncoderOps
import io.circe.{Decoder, Encoder, Json}
import shapeless.{:+:, CNil, Coproduct}
import sttp.model.StatusCode
import sttp.tapir.Schema.annotations.{description, title}
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.server.ServerEndpoint.Full
import sttp.tapir.{Endpoint, Schema, emptyOutputAs, statusCode}

import com.thatdot.api.v2.ErrorResponse.{ServerError, ServiceUnavailable}
import com.thatdot.api.v2.ErrorResponseHelpers.serverError
import com.thatdot.api.v2.codec.ThirdPartyCodecs.jdk.{instantDecoder, instantEncoder}
import com.thatdot.api.v2.schema.ThirdPartySchemas.jdk.instantSchema
import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.app.util.StringOps
import com.thatdot.quine.app.v2api.definitions._
import com.thatdot.quine.app.v2api.endpoints.V2AdministrationEndpointEntities._
import com.thatdot.quine.graph.NamespaceId
import com.thatdot.quine.routes._
import com.thatdot.quine.routes.exts.NamespaceParameter

/** Objects mapping to endpoints4s-annotated objects appearing in [[com.thatdot.quine.routes.AdministrationRoutes]].
  *  These objects require parallel Tapir annotations.
  */
object V2AdministrationEndpointEntities {

  import shapeless._

  import StringOps.syntax._

  // ── Backpressure API types ──

  @title("Host info")
  @description("Identifies the Quine host serving this snapshot.")
  final case class HostInfo(
    @description("Quine version string.") version: String,
    @description("Webserver bind address.") address: String,
    @description("Webserver port.") port: Int,
    @description("OS process ID.") pid: Long,
  )
  object HostInfo {
    implicit val encoder: Encoder[HostInfo] = deriveEncoder
    implicit val decoder: Decoder[HostInfo] = deriveDecoder
  }

  @title("Cluster member")
  @description("A member of the Quine cluster.")
  final case class ClusterMember(
    @description("Cluster-assigned member index.") memberIdx: Int,
    @description("Member address.") address: String,
    @description("Member cluster port.") port: Int,
  )
  object ClusterMember {
    implicit val encoder: Encoder[ClusterMember] = deriveEncoder
    implicit val decoder: Decoder[ClusterMember] = deriveDecoder
  }

  @title("Cluster hot spare")
  @description("A hot spare host in the Quine cluster.")
  final case class ClusterHotSpare(
    @description("Hot spare address.") address: String,
    @description("Hot spare cluster port.") port: Int,
  )
  object ClusterHotSpare {
    implicit val encoder: Encoder[ClusterHotSpare] = deriveEncoder
    implicit val decoder: Decoder[ClusterHotSpare] = deriveDecoder
  }

  @title("Cluster info")
  @description("Cluster topology and health. Absent in OSS mode.")
  final case class ClusterInfo(
    @description("Cluster name.") name: String,
    @description("Whether the cluster is fully operational.") fullyUp: Boolean,
    @description("Configured target number of cluster members.") targetSize: Int,
    @description("Identity of the member serving this API response.") thisMember: ClusterMember,
    @description("All active cluster members.") clusterMembers: Seq[ClusterMember],
    @description("Hot spare hosts available for failover.") hotSpares: Seq[ClusterHotSpare],
    @description("Timestamp when this cluster measurement was taken.") measurementTime: java.time.Instant,
  )
  object ClusterInfo {
    implicit val encoder: Encoder[ClusterInfo] = deriveEncoder
    implicit val decoder: Decoder[ClusterInfo] = deriveDecoder
  }

  @title("Global valve state")
  @description("State of the global ingest valve.")
  final case class GlobalValve(
    @description("Whether the valve is open (true) or closed (false).") isOpen: Boolean,
    @description("Number of active close requests (reference count). Zero means open.") closedCount: Int,
    @description("Number of open-to-closed transitions in the last 60 seconds.") oneMinuteClosures: Int,
  )
  object GlobalValve {
    implicit val encoder: Encoder[GlobalValve] = deriveEncoder
    implicit val decoder: Decoder[GlobalValve] = deriveDecoder
  }

  @title("Ingest stages")
  @description(
    "Per-stage backpressure state for an ingest pipeline. Each value is FLOWING, CONSTRAINED, or BACKPRESSURED.",
  )
  final case class IngestStages(
    @description("Source stage.") source: String,
    @description("Pre-graph-write stage.") preGraphWrite: String,
    @description("Post-graph-write (ACK) stage. Only present for checkpointed sources.") postGraphWrite: Option[String],
  )
  object IngestStages {
    implicit val encoder: Encoder[IngestStages] = deriveEncoder
    implicit val decoder: Decoder[IngestStages] = deriveDecoder
  }

  @title("Ingest snapshot")
  @description("Backpressure and throughput state for a single ingest stream.")
  final case class IngestSnapshot(
    @description("Ingest stream name.") name: String,
    @description("Namespace.") namespace: String,
    @description("Source type (e.g. 'NumberIterator', 'Kafka', 'ServerSentEvent').") sourceType: String,
    @description("Ingest status: RUNNING, PAUSED, COMPLETED, TERMINATED, FAILED, RESTORED.") status: String,
    @description("Configured rate limit (events/sec). Null means unlimited.") rateLimit: Option[Int],
    @description("Current throughput in events/sec (1-minute EWMA).") rate: Double,
    @description("Total items ingested.") totalCount: Long,
    @description("Per-stage backpressure state.") stages: IngestStages,
  )
  object IngestSnapshot {
    implicit val encoder: Encoder[IngestSnapshot] = deriveEncoder
    implicit val decoder: Decoder[IngestSnapshot] = deriveDecoder
  }

  @title("SQ queue snapshot")
  @description("Standing query result queue fill level and throughput.")
  final case class SqQueue(
    @description("Current number of results buffered.") bufferCount: Int,
    @description("Queue depth at which the global valve closes.") backpressureThreshold: Int,
    @description("Maximum queue capacity before results are dropped.") maxSize: Int,
    @description("Fill ratio relative to threshold (0.0 to 1.0). At 1.0 the valve closes.") thresholdRatio: Double,
    @description("Fill ratio relative to max capacity (0.0 to 1.0).") capacityRatio: Double,
    @description("Total results produced into the queue.") totalProduced: Long,
    @description("Total cancellations produced into the queue.") totalCancellations: Long,
    @description("Total results dropped due to overflow.") totalDropped: Long,
    @description("Total results consumed from the queue.") totalConsumed: Long,
    @description("Production rate (events/sec, 1-minute EWMA).") productionRate: Double,
    @description("Consumption rate (events/sec, 1-minute EWMA).") consumptionRate: Double,
  )
  object SqQueue {
    implicit val encoder: Encoder[SqQueue] = deriveEncoder
    implicit val decoder: Decoder[SqQueue] = deriveDecoder
  }

  @title("Output destination snapshot")
  @description("Backpressure state of a single output destination.")
  final case class DestinationSnapshot(
    @description("Destination type (e.g. 'StandardOut', 'Drop', 'HttpEndpoint', 'Kafka').") `type`: String,
    @description("Backpressure state: FLOWING, CONSTRAINED, or BACKPRESSURED.") state: String,
  )
  object DestinationSnapshot {
    implicit val encoder: Encoder[DestinationSnapshot] = deriveEncoder
    implicit val decoder: Decoder[DestinationSnapshot] = deriveDecoder
  }

  @title("SQ output snapshot")
  @description("Throughput and backpressure state for a single standing query output.")
  final case class SqOutputSnapshot(
    @description("Output name.") name: String,
    @description("Current throughput in events/sec (1-minute EWMA).") rate: Double,
    @description("Total items processed.") totalCount: Long,
    @description("Whether an enrichment query or pre-enrichment transformation is configured.") hasEnrichment: Boolean,
    @description(
      "Enrichment stage state: FLOWING, CONSTRAINED, BACKPRESSURED, or null if no enrichment.",
    ) enrichmentState: Option[String],
    @description("Per-destination backpressure state.") destinations: Seq[DestinationSnapshot],
  )
  object SqOutputSnapshot {
    implicit val encoder: Encoder[SqOutputSnapshot] = deriveEncoder
    implicit val decoder: Decoder[SqOutputSnapshot] = deriveDecoder
  }

  @title("Standing query snapshot")
  @description("Queue state and output throughput for a single standing query.")
  final case class StandingQuerySnapshot(
    @description("Standing query name.") name: String,
    @description("Namespace.") namespace: String,
    @description("Result queue state.") queue: SqQueue,
    @description("Per-output throughput and backpressure.") outputs: Seq[SqOutputSnapshot],
  )
  object StandingQuerySnapshot {
    implicit val encoder: Encoder[StandingQuerySnapshot] = deriveEncoder
    implicit val decoder: Decoder[StandingQuerySnapshot] = deriveDecoder
  }

  @title("Persistor snapshot")
  @description("Persistor type and latency.")
  final case class PersistorSnapshot(
    @description("Store type (e.g. 'rocks-db', 'cassandra', 'empty').") `type`: String,
    @description("Mean write latency in milliseconds.") writeLatencyMs: Double,
    @description("Mean read latency in milliseconds.") readLatencyMs: Double,
  )
  object PersistorSnapshot {
    implicit val encoder: Encoder[PersistorSnapshot] = deriveEncoder
    implicit val decoder: Decoder[PersistorSnapshot] = deriveDecoder
  }

  @title("Backpressure snapshot")
  @description("Point-in-time backpressure and throughput state across all active pipelines on this host.")
  final case class BackpressureSnapshot(
    @description("Timestamp of this snapshot.") timestamp: java.time.Instant,
    @description("Identity of the Quine host serving this snapshot.") host: HostInfo,
    @description("Cluster topology and health. Absent in OSS mode.") cluster: Option[ClusterInfo],
    @description("State of the global ingest valve.") globalValve: GlobalValve,
    @description("Per-ingest backpressure and throughput.") ingests: Seq[IngestSnapshot],
    @description("Per-standing-query queue state and output throughput.") standingQueries: Seq[StandingQuerySnapshot],
    @description("Persistor type and latency.") persistor: PersistorSnapshot,
  )
  object BackpressureSnapshot {
    implicit val encoder: Encoder[BackpressureSnapshot] = deriveEncoder
    implicit val decoder: Decoder[BackpressureSnapshot] = deriveDecoder
  }

  @title("Graph hash code")
  case class TGraphHashCode(
    @description("Hash value derived from the state of the graph (nodes, properties, and edges).")
    value: String,
    @description("RFC 3339 timestamp at which the graph hash was computed.")
    atTime: java.time.Instant,
  )
  object TGraphHashCode {
    implicit val encoder: Encoder[TGraphHashCode] = deriveEncoder
    implicit val decoder: Decoder[TGraphHashCode] = deriveDecoder
  }

  @title("System Build Information")
  @description("Information collected when this version of the system was compiled.")
  final case class TQuineInfo(
    @description("Quine version.") version: String,
    @description("Current build git commit.") gitCommit: Option[String],
    @description("Current build commit date.") gitCommitDate: Option[String],
    @description("Java compilation version.") javaVersion: String,
    @description("Java runtime version.") javaRuntimeVersion: String,
    @description("Java number of cores available.") javaAvailableProcessors: Int,
    @description("Java max head size in bytes.") javaMaxMemory: Long,
    @description("Persistence data format version.") persistenceWriteVersion: String,
    @description("Quine Type.") quineType: String,
  )
  object TQuineInfo {
    implicit val encoder: Encoder[TQuineInfo] = deriveEncoder
    implicit val decoder: Decoder[TQuineInfo] = deriveDecoder
  }

  @title("Metrics Counter")
  @description("Counters record a single shared count, and give that count a name.")
  final case class TCounter(
    @description("Name of the metric being reported.") name: String,
    @description("The value tracked by this counter.") count: Long,
  )
  object TCounter {
    implicit val encoder: Encoder[TCounter] = deriveEncoder
    implicit val decoder: Decoder[TCounter] = deriveDecoder
  }

  @title("Metrics Numeric Gauge")
  @description("Gauges provide a single point-in-time measurement, and give that measurement a name.")
  final case class TNumericGauge(
    @description("Name of the metric being reported.") name: String,
    @description("The latest measurement recorded by this gauge.") value: Double,
  )
  object TNumericGauge {
    implicit val encoder: Encoder[TNumericGauge] = deriveEncoder
    implicit val decoder: Decoder[TNumericGauge] = deriveDecoder
  }

  @title("Metrics Timer Summary")
  @description(
    """A rough cumulative histogram of times recorded by a timer, as well as the average rate at which
      |that timer is used to take new measurements. All times in milliseconds.""".asOneLine,
  )
  final case class TTimerSummary(
    @description("Name of the metric being reported.") name: String,
    // standard metrics
    @description("Fastest recorded time.") min: Double,
    @description("Slowest recorded time.") max: Double,
    @description("Median recorded time.") median: Double,
    @description("Average recorded time.") mean: Double,
    @description("First-quartile time.") q1: Double,
    @description("Third-quartile time.") q3: Double,
    @description("Average per-second rate of new events over the last one minute.") oneMinuteRate: Double,
    @description("90th percentile time.") `90`: Double,
    @description("99th percentile time.") `99`: Double,
    // pareto principle thresholds
    @description("80th percentile time.") `80`: Double,
    @description("20th percentile time.") `20`: Double,
    @description("10th percentile time.") `10`: Double,
  )
  object TTimerSummary {
    implicit val encoder: Encoder[TTimerSummary] = deriveEncoder
    implicit val decoder: Decoder[TTimerSummary] = deriveDecoder
  }

  @title("Metrics Report")
  @description(
    """A selection of metrics registered by Quine, its libraries, and the JVM. Reported metrics may change
      |based on which ingests and Standing Queries have been running since Quine startup, as well as the JVM distribution
      |running Quine and the packaged version of any dependencies.""".asOneLine,
  )
  final case class TMetricsReport(
    @description("A UTC Instant at which the returned metrics were collected.") atTime: java.time.Instant,
    @description("General-purpose counters for single numerical values.")
    counters: Seq[TCounter],
    @description(
      """Timers which measure how long an operation takes and how often that operation was timed, in milliseconds.
        |These are measured with wall time, and hence may be skewed by other system events outside our control like GC
        |pauses or system load.""".asOneLine,
    )
    timers: Seq[TTimerSummary],
    @description("Gauges which report an instantaneously-sampled reading of a particular metric.")
    gauges: Seq[TNumericGauge],
  )
  object TMetricsReport {
    implicit val encoder: Encoder[TMetricsReport] = deriveEncoder
    implicit val decoder: Decoder[TMetricsReport] = deriveDecoder
  }

  @title("Shard In-Memory Limits")
  final case class TShardInMemoryLimit(
    @description("Number of in-memory nodes past which shards will try to shut down nodes.") softLimit: Int,
    @description("Number of in-memory nodes past which shards will not load in new nodes.") hardLimit: Int,
  )
  object TShardInMemoryLimit {
    implicit val encoder: Encoder[TShardInMemoryLimit] = deriveEncoder
    implicit val decoder: Decoder[TShardInMemoryLimit] = deriveDecoder
  }

  private val genCounter = Generic[Counter]
  private val genTCounter = Generic[TCounter]
  private val genTimer = Generic[TimerSummary]
  private val genTTimer = Generic[TTimerSummary]
  private val genGauge = Generic[NumericGauge]
  private val genTGauge = Generic[TNumericGauge]

  def metricsReportFromV1Metrics(metricsReport: MetricsReport): TMetricsReport =
    TMetricsReport(
      metricsReport.atTime,
      metricsReport.counters.map(c => genTCounter.from(genCounter.to(c))),
      metricsReport.timers.map(t => genTTimer.from(genTimer.to(t))),
      metricsReport.gauges.map(g => genTGauge.from(genGauge.to(g))),
    )

}

trait V2QuineAdministrationEndpoints extends V2QuineEndpointDefinitions with GraphScopedEndpoints with StringOps {

  implicit lazy val graphHashCodeSchema: Schema[TGraphHashCode] =
    Schema
      .derived[TGraphHashCode]
      .description("Graph Hash Code")
      .encodedExample(TGraphHashCode(1000L.toString, java.time.Instant.parse("2026-04-27T15:30:00Z")).asJson)

  val exampleShardMap: Map[Int, TShardInMemoryLimit] = (0 to 3).map(_ -> TShardInMemoryLimit(10000, 75000)).toMap

  implicit lazy val shardInMemoryLimitMSchema: Schema[Map[Int, TShardInMemoryLimit]] = Schema
    .schemaForMap[Int, TShardInMemoryLimit](_.toString)
    .description("A map of shard IDs to shard in-memory node limits")
    .encodedExample(exampleShardMap.asJson)

  implicit lazy val hostInfoSchema: Schema[HostInfo] = Schema.derived
  implicit lazy val clusterMemberSchema: Schema[ClusterMember] = Schema.derived
  implicit lazy val clusterHotSpareSchema: Schema[ClusterHotSpare] = Schema.derived
  implicit lazy val clusterInfoSchema: Schema[ClusterInfo] = Schema.derived
  implicit lazy val globalValveSchema: Schema[GlobalValve] = Schema.derived
  implicit lazy val ingestStagesSchema: Schema[IngestStages] = Schema.derived
  implicit lazy val ingestSnapshotSchema: Schema[IngestSnapshot] = Schema.derived
  implicit lazy val sqQueueSchema: Schema[SqQueue] = Schema.derived
  implicit lazy val destinationSnapshotSchema: Schema[DestinationSnapshot] = Schema.derived
  implicit lazy val sqOutputSnapshotSchema: Schema[SqOutputSnapshot] = Schema.derived
  implicit lazy val standingQuerySnapshotSchema: Schema[StandingQuerySnapshot] = Schema.derived
  implicit lazy val persistorSnapshotSchema: Schema[PersistorSnapshot] = Schema.derived
  implicit lazy val backpressureSnapshotSchema: Schema[BackpressureSnapshot] = Schema.derived
  implicit lazy val tQuineInfoSchema: Schema[TQuineInfo] = Schema.derived
  implicit lazy val tCounterSchema: Schema[TCounter] = Schema.derived
  implicit lazy val tNumericGaugeSchema: Schema[TNumericGauge] = Schema.derived
  implicit lazy val tTimerSummarySchema: Schema[TTimerSummary] = Schema.derived
  implicit lazy val tMetricsReportSchema: Schema[TMetricsReport] = Schema.derived
  implicit lazy val tShardInMemoryLimitSchema: Schema[TShardInMemoryLimit] = Schema.derived

  def adminBase(path: String): EndpointBase = rawEndpoint("system")
    .in(path)
    .tag("System Administration")
    .errorOut(serverError())

  protected[endpoints] val systemInfo: Endpoint[Unit, Unit, ServerError, TQuineInfo, Any] =
    adminBase("systemInfo")
      .name("get-system-info")
      .summary("System Information")
      .description(
        "Returns a JSON object containing information about how Quine was built and system runtime information.",
      )
      .get
      .out(statusCode(StatusCode.Ok))
      .out(jsonBody[TQuineInfo])

  protected[endpoints] val systemInfoLogic: Unit => Future[Either[ServerError, TQuineInfo]] =
    _ => recoverServerError(Future.successful(appMethods.buildInfo))(inp => inp)

  private val systemInfoServerEndpoint: Full[
    Unit,
    Unit,
    Unit,
    ServerError,
    TQuineInfo,
    Any,
    Future,
  ] = systemInfo.serverLogic[Future](systemInfoLogic)

  protected[endpoints] val configE: Endpoint[Unit, Unit, ServerError, Json, Any] =
    adminBase("config")
      .name("get-config")
      .summary("Running Configuration")
      .description(
        """Fetch the full configuration of the running system.
          |"Full" means that this every option value is specified including all specified config files,
          |command line options, and default values.""".asOneLine + "\n\n" +
        """This does <em>not</em> include external options, for example, the Pekko HTTP option
          |`org.apache.pekko.http.server.request-timeout` can be used to adjust the web server request timeout of this
          |REST API, but it won't show up in the response of this endpoint.""".asOneLine,
      )
      .out(statusCode(StatusCode.Ok))
      .out(jsonBody[Json])

  protected[endpoints] val configLogic: Unit => Future[Either[ServerError, Json]] = _ =>
    recoverServerError(Future.successful(appMethods.config.loadedConfigJson))((inp: Json) => inp)

  private val configServerEndpoint: Full[Unit, Unit, Unit, ServerError, Json, Any, Future] =
    configE.serverLogic[Future](configLogic)

  protected[endpoints] val graphHashCode
    : Endpoint[Unit, (NamespaceId, Option[AtTime]), ServerError, TGraphHashCode, Any] =
    graphScopedEndpoint("hashCode")
      .tag("System Administration")
      .errorOut(serverError())
      .description(
        "Generate a hash of the state of the graph at the provided timestamp.\n\n" +
        """This is done by materializing readonly/historical versions of all nodes at a particular timestamp and
          |generating a checksum based on their (serialized) properties and edges.""".asOneLine + "\n" +
        "The timestamp defaults to the server's current clock time if not provided.\n\n" +
        """Because this relies on historical nodes, results may be inconsistent if running on a configuration with
          |journals disabled.""".asOneLine,
      )
      .name("get-graph-hashcode")
      .summary("Graph Hashcode")
      .in(atTimeParameter)
      .get
      .out(statusCode(StatusCode.Ok))
      .out(jsonBody[TGraphHashCode])

  protected[endpoints] val graphHashCodeLogic: ((NamespaceId, Option[AtTime])) => Future[
    Either[ServerError, TGraphHashCode],
  ] = { case (namespaceId, atime) =>
    recoverServerError(appMethods.graphHashCode(atime, namespaceId))((inp: TGraphHashCode) => inp)
  }

  private val graphHashCodeServerEndpoint: Full[
    Unit,
    Unit,
    (NamespaceId, Option[AtTime]),
    ServerError,
    TGraphHashCode,
    Any,
    Future,
  ] = graphHashCode.serverLogic[Future](graphHashCodeLogic)

  protected[endpoints] val liveness: Endpoint[Unit, Unit, ServerError, Unit, Any] =
    adminBase("liveness")
      .name("get-liveness")
      .summary("Process Liveness")
      .description(
        """This is a basic no-op endpoint for use when checking if the system is hung or responsive.
          |The intended use is for a process manager to restart the process if the app is hung (non-responsive).
          |It does not otherwise indicate readiness to handle data requests or system health.
          |Returns a 204 response.""".asOneLine,
      )
      .get
      .out(statusCode(StatusCode.NoContent).description("System is live").and(emptyOutputAs(())))

  protected[endpoints] val livenessLogic: Unit => Future[Either[ServerError, Unit]] = _ =>
    recoverServerError(Future.successful(()))(_ => ())

  val livenessServerEndpoint: Full[
    Unit,
    Unit,
    Unit,
    ServerError,
    Unit,
    Any,
    Future,
  ] = liveness.serverLogic[Future](livenessLogic)

  implicit val ex: ExecutionContext = ExecutionContext.parasitic

  protected[endpoints] val readiness: Endpoint[
    Unit,
    Unit,
    Either[ServerError, ServiceUnavailable],
    Unit,
    Any,
  ] =
    adminBase("readiness")
      .name("get-readiness")
      .summary("Process Readiness")
      .description(
        """This indicates whether the system is fully up and ready to service user requests.
          |The intended use is for a load balancer to use this to know when the instance is
          |up ready and start routing user requests to it.""".asOneLine,
      )
      .get
      .out(statusCode(StatusCode.NoContent).description("System is ready to serve requests"))
      .out(emptyOutputAs(()))
      .errorOutEither {
        statusCode(StatusCode.ServiceUnavailable).and {
          jsonBody[ServiceUnavailable]
            .description("System is not ready")
        }
      }

  protected[endpoints] val readinessLogic: Unit => Future[Either[Either[ServerError, ServiceUnavailable], Unit]] =
    _ =>
      recoverServerErrorEither(
        Future
          .successful(
            Either.cond(
              appMethods.isReady,
              (),
              Coproduct[ServiceUnavailable :+: CNil](ServiceUnavailable("System is not ready")),
            ),
          ),
      )(identity)

  val readinessServerEndpoint: Full[
    Unit,
    Unit,
    Unit,
    Either[ServerError, ServiceUnavailable],
    Unit,
    Any,
    Future,
  ] = readiness.serverLogic[Future](readinessLogic)

  protected[endpoints] val gracefulShutdown: Endpoint[Unit, Unit, ServerError, Unit, Any] =
    rawEndpoint("system:shutdown")
      .tag("System Administration")
      .errorOut(serverError())
      .name("initiate-shutdown")
      .summary("Graceful Shutdown")
      .description(
        """Initiate a graceful graph shutdown. Final shutdown may take a little longer.
          |`202` indicates a shutdown has been successfully initiated.""".asOneLine,
      )
      .post
      .out(statusCode(StatusCode.Accepted).description("Shutdown initiated"))
  protected[endpoints] val gracefulShutdownLogic: Unit => Future[Either[ServerError, Unit]] = _ =>
    recoverServerError(appMethods.performShutdown())(_ => ())

  private val gracefulShutdownServerEndpoint: Full[Unit, Unit, Unit, ServerError, Unit, Any, Future] =
    gracefulShutdown.serverLogic[Future](gracefulShutdownLogic)

  protected[endpoints] val metadata: Endpoint[Unit, Unit, ServerError, Map[String, String], Any] =
    adminBase("metadata")
      .name("get-metadata")
      .summary("Persisted Metadata")
      .attribute(Visibility.attributeKey, Visibility.Hidden)
      .get
      .out(statusCode(StatusCode.Ok))
      .out(jsonBody[Map[String, String]])

  protected[endpoints] val metadataLogic: Unit => Future[Either[ServerError, Map[String, String]]] =
    _ => recoverServerError(appMethods.metaData)((inp: Map[String, String]) => inp: Map[String, String])

  private val metadataServerEndpoint: Full[Unit, Unit, Unit, ServerError, Map[String, String], Any, Future] =
    metadata.serverLogic[Future](metadataLogic)

  protected[endpoints] val metrics: Endpoint[Unit, Option[Int], ServerError, TMetricsReport, Any] =
    adminBase("metrics")
      .name("get-metrics")
      .summary("Metrics")
      .in(memberIdxParameter)
      .description(
        """Returns a JSON object containing metrics data used in the Quine
          |[Monitoring](https://quine.io/core-concepts/operational-considerations/#monitoring)
          |dashboard. The selection of metrics is based on current configuration and execution environment, and is
          |subject to change. A few metrics of note include:""".asOneLine +
        """
          |
          |Counters
          |
          | - `quine.node.edge-counts.*`: Histogram-style summaries of edges per node
          | - `quine.node.property-counts.*`: Histogram-style summaries of properties per node
          | - `quine.shard.*.sleep-counters`: Count of nodes managed by a shard that have gone through various lifecycle
          |   states. These can be used to estimate the number of awake nodes.
          |
          |Timers
          |
          | - `persistor.get-journal`: Time taken to read and deserialize a single node's relevant journal
          | - `persistor.persist-event`: Time taken to serialize and persist one message's worth of on-node events
          | - `persistor.get-latest-snapshot`: Time taken to read (but not deserialize) a single node snapshot
          |
          | Gauges
          | - `memory.heap.*`: JVM heap usage
          | - `memory.total`: JVM combined memory usage
          | - `shared.valve.ingest`: Number of current requests to slow ingest for another part of Quine to catch up
          | - `dgn-reg.count`: Number of in-memory registered DomainGraphNodes""".stripMargin,
      )
      .get
      .out(statusCode(StatusCode.Ok))
      .out(jsonBody[TMetricsReport])

  protected[endpoints] val metricsLogic: Option[Int] => Future[Either[ServerError, TMetricsReport]] = maybeMemberIdx =>
    recoverServerError(appMethods.metrics(maybeMemberIdx).map(metricsReportFromV1Metrics))((inp: TMetricsReport) => inp)

  private val metricsServerEndpoint: Full[Unit, Unit, Option[Int], ServerError, TMetricsReport, Any, Future] =
    metrics.serverLogic[Future](metricsLogic)

  protected[endpoints] val getShardSizes: Endpoint[
    Unit,
    Option[Int],
    ServerError,
    Map[Int, TShardInMemoryLimit],
    Any,
  ] = adminBase("shardSizeLimits").get
    .name("get-shard-sizes")
    .summary("Get Shard Sizes")
    .in(memberIdxParameter)
    .description("Get the in-memory node limits for all shards.")
    .out(statusCode(StatusCode.Ok))
    .out(jsonBody[Map[Int, TShardInMemoryLimit]])

  protected[endpoints] val getShardSizesLogic
    : Option[Int] => Future[Either[ServerError, Map[Int, TShardInMemoryLimit]]] =
    maybeMemberIdx =>
      recoverServerError(
        appMethods
          .shardSizes(Map.empty, maybeMemberIdx)
          .map(_.view.mapValues(v => TShardInMemoryLimit(v.softLimit, v.hardLimit)).toMap)(ExecutionContext.parasitic),
      )((inp: Map[Int, TShardInMemoryLimit]) => inp)

  private val getShardSizesServerEndpoint: Full[
    Unit,
    Unit,
    Option[Int],
    ServerError,
    Map[Int, TShardInMemoryLimit],
    Any,
    Future,
  ] = getShardSizes.serverLogic[Future](getShardSizesLogic)

  protected[endpoints] val updateShardSizes: Endpoint[
    Unit,
    Map[Int, TShardInMemoryLimit],
    ServerError,
    Map[Int, TShardInMemoryLimit],
    Any,
  ] = adminBase("shardSizeLimits").patch
    .name("update-shard-sizes")
    .summary("Update Shard Sizes")
    .description(
      """Update the in-memory node limits. Shards not mentioned in the request are unaffected.
        |
        |Returns the updated in-memory node settings for all shards.""".stripMargin,
    )
    .in(jsonOrYamlBody[Map[Int, TShardInMemoryLimit]](Some(exampleShardMap)))
    .out(statusCode(StatusCode.Ok))
    .out(jsonBody[Map[Int, TShardInMemoryLimit]])

  protected[endpoints] val updateShardSizesLogic: Map[Int, TShardInMemoryLimit] => Future[
    Either[ServerError, Map[Int, TShardInMemoryLimit]],
  ] = resizes =>
    recoverServerError(
      appMethods
        .shardSizes(resizes.view.mapValues(v => ShardInMemoryLimit(v.softLimit, v.hardLimit)).toMap, None)
        .map(_.view.mapValues(v => TShardInMemoryLimit(v.softLimit, v.hardLimit)).toMap)(ExecutionContext.parasitic),
    )((inp: Map[Int, TShardInMemoryLimit]) => inp)

  private val updateShardSizesServerEndpoint: Full[
    Unit,
    Unit,
    Map[Int, TShardInMemoryLimit],
    ServerError,
    Map[Int, TShardInMemoryLimit],
    Any,
    Future,
  ] = updateShardSizes.serverLogic[Future](updateShardSizesLogic)

  protected[endpoints] val requestNodeSleep
    : Endpoint[Unit, (QuineId, Option[NamespaceParameter]), ServerError, Unit, Any] =
    adminBase("nodes").post
      .name("sleep-node")
      .summary("Sleep Node")
      .description(
        """Attempt to put the specified node to sleep.
          |
          |This behavior is not guaranteed. Activity on the node will supersede this request.""".stripMargin,
      )
      .attribute(Visibility.attributeKey, Visibility.Hidden)
      .in(CustomMethod.colonVerbPath[QuineId]("nodeIdSegment", "requestSleep"))
      .in(namespaceParameter)
      .out(statusCode(StatusCode.Accepted))
  protected[endpoints] val requestNodeSleepLogic
    : ((QuineId, Option[NamespaceParameter])) => Future[Either[ServerError, Unit]] = { case (nodeId, namespace) =>
    recoverServerError(appMethods.requestNodeSleep(nodeId, namespaceFromParam(namespace)))(_ => ())
  }

  private val requestNodeSleepServerEndpoint: Full[
    Unit,
    Unit,
    (QuineId, Option[NamespaceParameter]),
    ServerError,
    Unit,
    Any,
    Future,
  ] = requestNodeSleep.serverLogic[Future](requestNodeSleepLogic)

  protected[endpoints] val backpressure: Endpoint[Unit, Unit, ServerError, Seq[BackpressureSnapshot], Any] =
    adminBase("backpressure").get
      .name("get-backpressure")
      .summary("Get Backpressure Snapshots")
      .description(
        "Returns a list of point-in-time backpressure snapshots. Currently returns a single snapshot for this host. " +
        "Future versions may return snapshots from multiple cluster members for cluster-wide aggregation.",
      )
      .out(statusCode(StatusCode.Ok))
      .out(jsonBody[Seq[BackpressureSnapshot]])

  protected[endpoints] val backpressureLogic: Unit => Future[Either[ServerError, Seq[BackpressureSnapshot]]] = _ =>
    recoverServerError(appMethods.backpressureSnapshot().map(Seq(_))(ExecutionContext.parasitic))(identity)

  private val backpressureServerEndpoint: Full[Unit, Unit, Unit, ServerError, Seq[BackpressureSnapshot], Any, Future] =
    backpressure.serverLogic[Future](backpressureLogic)

  val adminEndpoints: List[ServerEndpoint[Any, Future]] = List(
    systemInfoServerEndpoint,
    configServerEndpoint,
    graphHashCodeServerEndpoint,
    livenessServerEndpoint,
    metadataServerEndpoint,
    metricsServerEndpoint,
    readinessServerEndpoint,
    requestNodeSleepServerEndpoint,
    getShardSizesServerEndpoint,
    updateShardSizesServerEndpoint,
    gracefulShutdownServerEndpoint,
    backpressureServerEndpoint,
  )

}
