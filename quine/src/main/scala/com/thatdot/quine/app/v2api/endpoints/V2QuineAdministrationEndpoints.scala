package com.thatdot.quine.app.v2api.endpoints

import scala.concurrent.{ExecutionContext, Future}

import io.circe.generic.extras.semiauto.{deriveConfiguredDecoder, deriveConfiguredEncoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.syntax.EncoderOps
import io.circe.{Decoder, Encoder}
import shapeless.{:+:, CNil, Coproduct}
import sttp.model.StatusCode
import sttp.tapir.Schema.annotations.{description, title}
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.server.ServerEndpoint.Full
import sttp.tapir.{Endpoint, Schema, emptyOutputAs, statusCode}

import com.thatdot.api.v2.ErrorResponse.{ServerError, ServiceUnavailable}
import com.thatdot.api.v2.ErrorResponseHelpers.serverError
import com.thatdot.api.v2.TypeDiscriminatorConfig.instances.circeConfig
import com.thatdot.api.v2.codec.ThirdPartyCodecs.jdk.{instantDecoder, instantEncoder}
import com.thatdot.api.v2.schema.ThirdPartySchemas.jdk.instantSchema
import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.app.config.BaseConfig
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
    @description(
      "Cluster position of the host this snapshot describes. Absent in OSS mode, and on a " +
      "host that is not currently positioned in the cluster (e.g. a hot spare).",
    ) memberIdx: Option[Int],
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
    @description(
      "Position of this destination within its output's destination list. Stable across hosts and " +
      "polls, and the only thing distinguishing two destinations of the same type — so it is the key " +
      "to join a destination against its counterpart on another cluster member.",
    ) index: Int,
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
    @description(
      "Every graph (namespace) on this host, including those with no active ingests or " +
      "standing queries — so idle graphs still appear in the diagram.",
    ) namespaces: Seq[String],
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

  @title("Persistor Summary")
  final case class PersistorSummary(
    @description("Persistor type, e.g. \"rocksdb\", \"cassandra\", \"clickhouse\".") persistorType: String,
    @description("Whether this persistor stores data locally on this instance.") isLocal: Boolean,
  )
  object PersistorSummary {
    implicit val encoder: Encoder[PersistorSummary] = deriveEncoder
    implicit val decoder: Decoder[PersistorSummary] = deriveDecoder
  }

  @title("Webserver Summary")
  final case class WebserverSummary(
    @description("Bind address of the REST API webserver.") address: String,
    @description("Bind port of the REST API webserver.") port: Int,
    @description("Whether the REST API webserver is enabled.") enabled: Boolean,
    @description("Whether the REST API webserver is serving over TLS.") useTls: Boolean,
  )
  object WebserverSummary {
    implicit val encoder: Encoder[WebserverSummary] = deriveEncoder
    implicit val decoder: Decoder[WebserverSummary] = deriveDecoder
  }

  @title("Cluster Summary")
  @description("Cluster formation settings. Only present on Quine Enterprise.")
  final case class ClusterSummary(
    @description("Configured name of the cluster.") name: String,
    @description("Configured target size of the cluster.") targetSize: Int,
    @description("Configured number of shards per cluster member.") shardsPerMember: Int,
  )
  object ClusterSummary {
    implicit val encoder: Encoder[ClusterSummary] = deriveEncoder
    implicit val decoder: Decoder[ClusterSummary] = deriveDecoder
  }

  @title("Bolt Summary")
  @description("Bolt protocol endpoint settings. Only present on Quine Enterprise.")
  final case class BoltSummary(
    @description("Whether the Bolt endpoint is enabled.") enabled: Boolean,
    @description("Bind address of the Bolt endpoint.") address: String,
    @description("Bind port of the Bolt endpoint.") port: Int,
    @description("Configured Bolt encryption level.") encryption: String,
  )
  object BoltSummary {
    implicit val encoder: Encoder[BoltSummary] = deriveEncoder
    implicit val decoder: Decoder[BoltSummary] = deriveDecoder
  }

  @title("Running System Configuration")
  @description(
    """A filtered, non-sensitive view of the running configuration: only fields relevant to monitoring and
      |operations are included. Credentials and other secrets are never included, regardless of what backs
      |them in the underlying configuration.""".asOneLine,
  )
  final case class SystemConfigView(
    @description("Summary of the configured persistor.") persistor: PersistorSummary,
    @description("Summary of the REST API webserver bind config, if this instance has one.")
    webserver: Option[WebserverSummary],
    @description("Configured shard count, for products that expose it.") shardCount: Option[Int],
    @description("Number of in-memory nodes past which shards will try to shut down nodes.")
    inMemorySoftNodeLimit: Option[Int],
    @description("Number of in-memory nodes past which shards will not load in new nodes.")
    inMemoryHardNodeLimit: Option[Int],
    @description("Types of the configured metrics reporters, e.g. \"jmx\", \"influxdb\".")
    metricsReporterTypes: List[String],
    @description("Default API version used by this instance.") defaultApiVersion: String,
    @description("Cluster formation summary. Only present on Quine Enterprise.") cluster: Option[ClusterSummary],
    @description("Bolt endpoint summary. Only present on Quine Enterprise.") bolt: Option[BoltSummary],
  )
  object SystemConfigView {
    implicit val encoder: Encoder[SystemConfigView] = deriveEncoder
    implicit val decoder: Decoder[SystemConfigView] = deriveDecoder

    /** V2 wire shape of [[com.thatdot.quine.app.config.SystemConfigSummary]], which decides what config
      * data is safe to expose.
      */
    def fromConfig(config: BaseConfig): SystemConfigView = {
      val summary = config.systemConfigSummary
      SystemConfigView(
        persistor = PersistorSummary(summary.persistor.persistorType, summary.persistor.isLocal),
        webserver = summary.webserver.map(w => WebserverSummary(w.address, w.port, w.enabled, w.useTls)),
        shardCount = summary.shardCount,
        inMemorySoftNodeLimit = summary.inMemorySoftNodeLimit,
        inMemoryHardNodeLimit = summary.inMemoryHardNodeLimit,
        metricsReporterTypes = summary.metricsReporterTypes,
        defaultApiVersion = summary.defaultApiVersion,
        cluster = summary.cluster.map(c => ClusterSummary(c.name, c.targetSize, c.shardsPerMember)),
        bolt = summary.bolt.map(b => BoltSummary(b.enabled, b.address, b.port, b.encryption)),
      )
    }
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
    // This type is the body of the `PATCH shardSizeLimits` request, so its decoder must use the
    // shared strict config to reject unknown/misspelled fields rather than silently ignoring them.
    implicit val encoder: Encoder[TShardInMemoryLimit] = deriveConfiguredEncoder
    implicit val decoder: Decoder[TShardInMemoryLimit] = deriveConfiguredDecoder
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
  implicit lazy val persistorSummarySchema: Schema[PersistorSummary] = Schema.derived
  implicit lazy val webserverSummarySchema: Schema[WebserverSummary] = Schema.derived
  implicit lazy val clusterSummarySchema: Schema[ClusterSummary] = Schema.derived
  implicit lazy val boltSummarySchema: Schema[BoltSummary] = Schema.derived
  implicit lazy val systemConfigViewSchema: Schema[SystemConfigView] = Schema.derived

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

  protected[endpoints] val configE: Endpoint[Unit, Unit, ServerError, SystemConfigView, Any] =
    adminBase("config")
      .name("get-config")
      .summary("Running Configuration")
      .description(
        """Fetch a filtered view of the running system's configuration, containing only fields relevant to
          |monitoring and operations (e.g. persistor type, webserver bind info, shard/memory limits).""".asOneLine +
        "\n\n" +
        """Credentials and other secrets are never included, regardless of what backs them in the underlying
          |configuration. This does <em>not</em> return the full configuration -- for that, use the config file(s)
          |and command line options this instance was started with directly.""".asOneLine,
      )
      .out(statusCode(StatusCode.Ok))
      .out(jsonBody[SystemConfigView])

  protected[endpoints] val configLogic: Unit => Future[Either[ServerError, SystemConfigView]] = _ =>
    recoverServerError(Future.successful(SystemConfigView.fromConfig(appMethods.config)))(identity)

  private val configServerEndpoint: Full[Unit, Unit, Unit, ServerError, SystemConfigView, Any, Future] =
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
        "Returns a list of point-in-time backpressure snapshots, one per reachable cluster member " +
        "(a single-entry list on an unclustered/OSS instance).",
      )
      .out(statusCode(StatusCode.Ok))
      .out(jsonBody[Seq[BackpressureSnapshot]])

  protected[endpoints] val backpressureLogic: Unit => Future[Either[ServerError, Seq[BackpressureSnapshot]]] = _ =>
    recoverServerError(appMethods.backpressureSnapshot())(identity)

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
