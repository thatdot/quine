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
import sttp.tapir.{Endpoint, Schema, emptyOutputAs, path, statusCode}

import com.thatdot.api.v2.ErrorResponse.{ServerError, ServiceUnavailable}
import com.thatdot.api.v2.ErrorResponseHelpers.serverError
import com.thatdot.api.v2.SuccessEnvelope
import com.thatdot.api.v2.schema.V2ApiConfiguration
import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.app.util.StringOps
import com.thatdot.quine.app.v2api.definitions._
import com.thatdot.quine.app.v2api.endpoints.V2AdministrationEndpointEntities._
import com.thatdot.quine.routes._

/** Objects mapping to endpoints4s-annotated objects appearing in [[com.thatdot.quine.routes.AdministrationRoutes]].
  *  These objects require parallel Tapir annotations.
  */
object V2AdministrationEndpointEntities {

  import shapeless._

  import StringOps.syntax._

  @title("Graph hash code")
  case class TGraphHashCode(
    @description("Hash value derived from the state of the graph (nodes, properties, and edges).")
    value: String,
    @description("Time value used to derive the graph hash code.")
    atTime: Long,
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

trait V2QuineAdministrationEndpoints extends V2QuineEndpointDefinitions with V2ApiConfiguration with StringOps {

  implicit lazy val graphHashCodeSchema: Schema[TGraphHashCode] =
    Schema
      .derived[TGraphHashCode]
      .description("Graph Hash Code")
      .encodedExample(TGraphHashCode(1000L.toString, 12345L).asJson)

  val exampleShardMap: Map[Int, TShardInMemoryLimit] = (0 to 3).map(_ -> TShardInMemoryLimit(10000, 75000)).toMap

  implicit lazy val shardInMemoryLimitMSchema: Schema[Map[Int, TShardInMemoryLimit]] = Schema
    .schemaForMap[Int, TShardInMemoryLimit](_.toString)
    .description("A map of shard IDs to shard in-memory node limits")
    .encodedExample(exampleShardMap.asJson)

  implicit lazy val tQuineInfoSchema: Schema[TQuineInfo] = Schema.derived[TQuineInfo]
  implicit lazy val tCounterSchema: Schema[TCounter] = Schema.derived[TCounter]
  implicit lazy val tNumericGaugeSchema: Schema[TNumericGauge] = Schema.derived[TNumericGauge]
  implicit lazy val tTimerSummarySchema: Schema[TTimerSummary] = Schema.derived[TTimerSummary]
  implicit lazy val tMetricsReportSchema: Schema[TMetricsReport] = Schema.derived[TMetricsReport]
  implicit lazy val tShardInMemoryLimitSchema: Schema[TShardInMemoryLimit] = Schema.derived[TShardInMemoryLimit]

  def adminBase(path: String): EndpointBase = rawEndpoint("admin")
    .in(path)
    .tag("Administration")
    .errorOut(serverError())

  protected[endpoints] val systemInfo: Endpoint[Unit, Unit, ServerError, SuccessEnvelope.Ok[TQuineInfo], Any] =
    adminBase("system-info")
      .name("get-system-info")
      .summary("System Information")
      .description(
        "Returns a JSON object containing information about how Quine was built and system runtime information.",
      )
      .get
      .out(statusCode(StatusCode.Ok))
      .out(jsonBody[SuccessEnvelope.Ok[TQuineInfo]])

  protected[endpoints] val systemInfoLogic: Unit => Future[Either[ServerError, SuccessEnvelope.Ok[TQuineInfo]]] =
    _ => recoverServerError(Future.successful(appMethods.buildInfo))(inp => SuccessEnvelope.Ok(inp))

  private val systemInfoServerEndpoint: Full[
    Unit,
    Unit,
    Unit,
    ServerError,
    SuccessEnvelope.Ok[TQuineInfo],
    Any,
    Future,
  ] = systemInfo.serverLogic[Future](systemInfoLogic)

  protected[endpoints] val configE: Endpoint[Unit, Unit, ServerError, SuccessEnvelope.Ok[Json], Any] =
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
      .out(jsonBody[SuccessEnvelope.Ok[Json]])

  protected[endpoints] val configLogic: Unit => Future[Either[ServerError, SuccessEnvelope.Ok[Json]]] = _ =>
    recoverServerError(Future.successful(appMethods.config.loadedConfigJson))((inp: Json) => SuccessEnvelope.Ok(inp))

  private val configServerEndpoint: Full[Unit, Unit, Unit, ServerError, SuccessEnvelope.Ok[Json], Any, Future] =
    configE.serverLogic[Future](configLogic)

  protected[endpoints] val graphHashCode
    : Endpoint[Unit, (Option[AtTime], Option[String]), ServerError, SuccessEnvelope.Ok[TGraphHashCode], Any] =
    adminBase("graph-hash-code")
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
      .in(namespaceParameter)
      .get
      .out(statusCode(StatusCode.Ok))
      .out(jsonBody[SuccessEnvelope.Ok[TGraphHashCode]])

  protected[endpoints] val graphHashCodeLogic
    : ((Option[AtTime], Option[String])) => Future[Either[ServerError, SuccessEnvelope.Ok[TGraphHashCode]]] = {
    case (atime, ns: Option[String]) =>
      recoverServerError(appMethods.graphHashCode(atime, namespaceFromParam(ns)))((inp: TGraphHashCode) =>
        SuccessEnvelope.Ok(inp),
      )
  }

  private val graphHashCodeServerEndpoint: Full[
    Unit,
    Unit,
    (Option[AtTime], Option[String]),
    ServerError,
    SuccessEnvelope.Ok[TGraphHashCode],
    Any,
    Future,
  ] = graphHashCode.serverLogic[Future](graphHashCodeLogic)

  protected[endpoints] val liveness: Endpoint[Unit, Unit, ServerError, SuccessEnvelope.NoContent.type, Any] =
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
      .out(statusCode(StatusCode.NoContent).description("System is live").and(emptyOutputAs(SuccessEnvelope.NoContent)))

  protected[endpoints] val livenessLogic: Unit => Future[Either[ServerError, SuccessEnvelope.NoContent.type]] = _ =>
    recoverServerError(Future.successful(()))(_ => SuccessEnvelope.NoContent)

  val livenessServerEndpoint: Full[
    Unit,
    Unit,
    Unit,
    ServerError,
    SuccessEnvelope.NoContent.type,
    Any,
    Future,
  ] = liveness.serverLogic[Future](livenessLogic)

  implicit val ex: ExecutionContext = ExecutionContext.parasitic

  protected[endpoints] val readiness: Endpoint[
    Unit,
    Unit,
    Either[ServerError, ServiceUnavailable],
    SuccessEnvelope.NoContent.type,
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
      .out(emptyOutputAs(SuccessEnvelope.NoContent))
      .errorOutEither {
        statusCode(StatusCode.ServiceUnavailable).and {
          jsonBody[ServiceUnavailable]
            .description("System is not ready")
        }
      }

  protected[endpoints] val readinessLogic
    : Unit => Future[Either[Either[ServerError, ServiceUnavailable], SuccessEnvelope.NoContent.type]] =
    _ =>
      recoverServerErrorEither(
        Future
          .successful(
            Either.cond(
              appMethods.isReady,
              SuccessEnvelope.NoContent,
              Coproduct[ServiceUnavailable :+: CNil](ServiceUnavailable("System is not ready")),
            ),
          ),
      )(identity)

  val readinessServerEndpoint: Full[
    Unit,
    Unit,
    Unit,
    Either[ServerError, ServiceUnavailable],
    SuccessEnvelope.NoContent.type,
    Any,
    Future,
  ] = readiness.serverLogic[Future](readinessLogic)

  protected[endpoints] val gracefulShutdown: Endpoint[Unit, Unit, ServerError, SuccessEnvelope.Accepted, Any] =
    adminBase("shutdown")
      .name("initiate-shutdown")
      .summary("Graceful Shutdown")
      .description(
        """Initiate a graceful graph shutdown. Final shutdown may take a little longer.
          |`202` indicates a shutdown has been successfully initiated.""".asOneLine,
      )
      .post
      .out(statusCode(StatusCode.Accepted).description("Shutdown initiated"))
      .out(jsonBody[SuccessEnvelope.Accepted])

  protected[endpoints] val gracefulShutdownLogic: Unit => Future[Either[ServerError, SuccessEnvelope.Accepted]] = _ =>
    recoverServerError(appMethods.performShutdown())(_ => SuccessEnvelope.Accepted())

  private val gracefulShutdownServerEndpoint
    : Full[Unit, Unit, Unit, ServerError, SuccessEnvelope.Accepted, Any, Future] =
    gracefulShutdown.serverLogic[Future](gracefulShutdownLogic)

  protected[endpoints] val metadata: Endpoint[Unit, Unit, ServerError, SuccessEnvelope.Ok[Map[String, String]], Any] =
    adminBase("metadata")
      .name("get-metadata")
      .summary("Persisted Metadata")
      .attribute(Visibility.attributeKey, Visibility.Hidden)
      .get
      .out(statusCode(StatusCode.Ok))
      .out(jsonBody[SuccessEnvelope.Ok[Map[String, String]]])

  protected[endpoints] val metadataLogic: Unit => Future[Either[ServerError, SuccessEnvelope.Ok[Map[String, String]]]] =
    _ =>
      recoverServerError(appMethods.metaData)((inp: Map[String, String]) =>
        SuccessEnvelope.Ok(inp): SuccessEnvelope.Ok[Map[String, String]],
      )

  private val metadataServerEndpoint
    : Full[Unit, Unit, Unit, ServerError, SuccessEnvelope.Ok[Map[String, String]], Any, Future] =
    metadata.serverLogic[Future](metadataLogic)

  protected[endpoints] val metrics: Endpoint[Unit, Option[Int], ServerError, SuccessEnvelope.Ok[TMetricsReport], Any] =
    adminBase("metrics")
      .name("get-metrics")
      .summary("Metrics")
      .in(memberIdxParameter)
      .description(
        """Returns a JSON object containing metrics data used in the Quine
          |[Monitoring](https://docs.quine.io/core-concepts/operational-considerations.html#monitoring)
          |dashboard. The selection of metrics is based on current configuration and execution environment, and is
          |subject to change. A few metrics of note include:""".asOneLine +
        """
          |
          |Counters
          |
          | - `node.edge-counts.*`: Histogram-style summaries of edges per node
          | - `node.property-counts.*`: Histogram-style summaries of properties per node
          | - `shard.*.sleep-counters`: Count of nodes managed by a shard that have gone through various lifecycle
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
      .out(jsonBody[SuccessEnvelope.Ok[TMetricsReport]])

  protected[endpoints] val metricsLogic
    : Option[Int] => Future[Either[ServerError, SuccessEnvelope.Ok[TMetricsReport]]] = maybeMemberIdx =>
    recoverServerError(appMethods.metrics(maybeMemberIdx).map(metricsReportFromV1Metrics))((inp: TMetricsReport) =>
      SuccessEnvelope.Ok(inp),
    )

  private val metricsServerEndpoint
    : Full[Unit, Unit, Option[Int], ServerError, SuccessEnvelope.Ok[TMetricsReport], Any, Future] =
    metrics.serverLogic[Future](metricsLogic)

  protected[endpoints] val getShardSizes: Endpoint[
    Unit,
    Unit,
    ServerError,
    SuccessEnvelope.Ok[Map[Int, TShardInMemoryLimit]],
    Any,
  ] = adminBase("shards").get
    .name("get-shard-sizes")
    .summary("Get Shard Sizes")
    .description("Get the in-memory node limits for all shards.")
    .in("size-limits")
    .out(statusCode(StatusCode.Ok))
    .out(jsonBody[SuccessEnvelope.Ok[Map[Int, TShardInMemoryLimit]]])

  protected[endpoints] val getShardSizesLogic
    : Unit => Future[Either[ServerError, SuccessEnvelope.Ok[Map[Int, TShardInMemoryLimit]]]] =
    _ =>
      recoverServerError(
        appMethods
          .shardSizes(Map.empty)
          .map(_.view.mapValues(v => TShardInMemoryLimit(v.softLimit, v.hardLimit)).toMap)(ExecutionContext.parasitic),
      )((inp: Map[Int, TShardInMemoryLimit]) => SuccessEnvelope.Ok(inp))

  private val getShardSizesServerEndpoint: Full[
    Unit,
    Unit,
    Unit,
    ServerError,
    SuccessEnvelope.Ok[Map[Int, TShardInMemoryLimit]],
    Any,
    Future,
  ] = getShardSizes.serverLogic[Future](getShardSizesLogic)

  protected[endpoints] val updateShardSizes: Endpoint[
    Unit,
    Map[Int, TShardInMemoryLimit],
    ServerError,
    SuccessEnvelope.Ok[Map[Int, TShardInMemoryLimit]],
    Any,
  ] = adminBase("shards").post
    .name("update-shard-sizes")
    .summary("Update Shard Sizes")
    .description(
      """Update the in-memory node limits. Shards not mentioned in the request are unaffected.
        |
        |Returns the updated in-memory node settings for all shards.""".stripMargin,
    )
    .in("size-limits")
    .in(jsonOrYamlBody[Map[Int, TShardInMemoryLimit]](Some(exampleShardMap)))
    .out(statusCode(StatusCode.Ok))
    .out(jsonBody[SuccessEnvelope.Ok[Map[Int, TShardInMemoryLimit]]])

  protected[endpoints] val updateShardSizesLogic: Map[Int, TShardInMemoryLimit] => Future[
    Either[ServerError, SuccessEnvelope.Ok[Map[Int, TShardInMemoryLimit]]],
  ] = resizes =>
    recoverServerError(
      appMethods
        .shardSizes(resizes.view.mapValues(v => ShardInMemoryLimit(v.softLimit, v.hardLimit)).toMap)
        .map(_.view.mapValues(v => TShardInMemoryLimit(v.softLimit, v.hardLimit)).toMap)(ExecutionContext.parasitic),
    )((inp: Map[Int, TShardInMemoryLimit]) => SuccessEnvelope.Ok(inp))

  private val updateShardSizesServerEndpoint: Full[
    Unit,
    Unit,
    Map[Int, TShardInMemoryLimit],
    ServerError,
    SuccessEnvelope.Ok[Map[Int, TShardInMemoryLimit]],
    Any,
    Future,
  ] = updateShardSizes.serverLogic[Future](updateShardSizesLogic)

  protected[endpoints] val requestNodeSleep
    : Endpoint[Unit, (QuineId, Option[String]), ServerError, SuccessEnvelope.Accepted, Any] =
    adminBase("nodes").post
      .name("sleep-node")
      .summary("Sleep Node")
      .description(
        """Attempt to put the specified node to sleep.
          |
          |This behavior is not guaranteed. Activity on the node will supersede this request.""".stripMargin,
      )
      .in(path[QuineId]("nodeIdSegment"))
      .in("request-sleep")
      .in(namespaceParameter)
      .out(statusCode(StatusCode.Accepted))
      .out(jsonBody[SuccessEnvelope.Accepted])

  protected[endpoints] val requestNodeSleepLogic
    : ((QuineId, Option[String])) => Future[Either[ServerError, SuccessEnvelope.Accepted]] = {
    case (nodeId, namespace) =>
      recoverServerError(appMethods.requestNodeSleep(nodeId, namespaceFromParam(namespace)))(_ =>
        SuccessEnvelope.Accepted(),
      )
  }

  private val requestNodeSleepServerEndpoint: Full[
    Unit,
    Unit,
    (QuineId, Option[String]),
    ServerError,
    SuccessEnvelope.Accepted,
    Any,
    Future,
  ] = requestNodeSleep.serverLogic[Future](requestNodeSleepLogic)

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
  )

}
