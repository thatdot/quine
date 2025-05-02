package com.thatdot.quine.app.v2api.endpoints

import scala.concurrent.{ExecutionContext, Future}

import io.circe.Json
import io.circe.generic.extras.auto._
import io.circe.syntax.EncoderOps
import sttp.model.StatusCode
import sttp.tapir.Schema.annotations.{description, title}
import sttp.tapir.generic.auto._
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.server.ServerEndpoint.Full
import sttp.tapir.{Endpoint, Schema, emptyOutputAs, oneOfVariantFromMatchType, path, statusCode}

import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.app.v2api.definitions.SuccessEnvelope.NoContent
import com.thatdot.quine.app.v2api.definitions._
import com.thatdot.quine.app.v2api.endpoints.V2AdministrationEndpointEntities._
import com.thatdot.quine.model.Milliseconds
import com.thatdot.quine.routes._

/** Objects mapping to endpoints4s annotated objects appearing in [[com.thatdot.quine.routes.AdministrationRoutes]] . These
  * objects require parallel tapir annotations.
  *
  * Parallel Tapir objects are prefixed with "T" for disambiguation.
  */
object V2AdministrationEndpointEntities {

  import shapeless._

  @title("Graph hash code")
  case class TGraphHashCode(
    @description("Hash value derived from the state of the graph (nodes, properties, and edges)")
    value: Long,
    @description("Time value used to derive the graph hash code")
    atTime: Long,
  )

  @title("System Build Information")
  @description("Information collected when this version of the system was compiled.")
  final case class TQuineInfo(
    @title("Quine version") version: String,
    @title("Current build git commit") gitCommit: Option[String],
    @title("Current build commit date") gitCommitDate: Option[String],
    @title("Java compilation version") javaVersion: String,
    @title("Persistence data format version") persistenceWriteVersion: String,
  )

  @title("Metrics Counter")
  @description("Counters record a single shared count, and give that count a name")
  final case class TCounter(
    @description("Name of the metric being reported") name: String,
    @description("The value tracked by this counter") count: Long,
  )

  @title("Metrics Numeric Gauge")
  @description("Gauges provide a single point-in-time measurement, and give that measurement a name")
  final case class TNumericGauge(
    @description("Name of the metric being reported") name: String,
    @description("The latest measurement recorded by this gauge") value: Double,
  )

  @title("Metrics Timer Summary")
  @description(
    """A rough cumulative histogram of times recorded by a timer, as well as the average rate at which that timer is
      |used to take new measurements. All times in milliseconds.""".stripMargin.replace('\n', ' '),
  )
  final case class TTimerSummary(
    @description("Name of the metric being reported") name: String,
    // standard metrics
    @description("Fastest recorded time") min: Double,
    @description("Slowest recorded time") max: Double,
    @description("Median recorded time") median: Double,
    @description("Average recorded time") mean: Double,
    @description("First-quartile time") q1: Double,
    @description("Third-quartile time") q3: Double,
    @description(
      "Average per-second rate of new events over the last one minute",
    ) oneMinuteRate: Double,
    @description("90th percentile time") `90`: Double,
    @description("99th percentile time") `99`: Double,
    // pareto principle thresholds
    @description("80th percentile time") `80`: Double,
    @description("20th percentile time") `20`: Double,
    @description("10th percentile time") `10`: Double,
  )

  @title("Metrics Report")
  @description("""A selection of metrics registered by Quine, its libraries, and the JVM. Reported metrics may change
                 |based on which ingests and standing queries have been running since Quine startup, as well as the JVM distribution
                 |running Quine and the packaged version of any dependencies.""".stripMargin.replace('\n', ' '))
  final case class TMetricsReport(
    @description("A UTC Instant at which the returned metrics were collected") atTime: java.time.Instant,
    @description("General-purpose counters for single numerical values") counters: Seq[TCounter],
    @description(
      "Timers which measure how long an operation takes and how often that operation was timed, in milliseconds. " +
      "These are measured with wall time, and hence may be skewed by other system events outside our control like " +
      "GC pauses or system load.",
    ) timers: Seq[
      TTimerSummary,
    ],
    @description("Gauges which report an instantaneously-sampled reading of a particular metric") gauges: Seq[
      TNumericGauge,
    ],
  )

  @title("Shard In-Memory Limits")
  final case class TShardInMemoryLimit(
    @description("Number of in-memory nodes past which shards will try to shut down nodes") softLimit: Int,
    @description("Number of in-memory nodes past which shards will not load in new nodes") hardLimit: Int,
  )

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

trait V2AdministrationEndpoints extends V2QuineEndpointDefinitions with V2ApiConfiguration {

  implicit lazy val graphHashCodeSchema: Schema[TGraphHashCode] =
    Schema.derived[TGraphHashCode].description("Graph Hash Code").encodedExample(TGraphHashCode(1000L, 12345L).asJson)

  val exampleShardMap: Map[Int, TShardInMemoryLimit] = (0 to 3).map(_ -> TShardInMemoryLimit(10000, 75000)).toMap
  implicit lazy val shardInMemoryLimitMSchema: Schema[Map[Int, TShardInMemoryLimit]] = Schema
    .schemaForMap[Int, TShardInMemoryLimit](_.toString)
    .description("A map of shard IDs to shard in-memory node limits")
    .encodedExample(exampleShardMap.asJson)

  protected def adminEndpoint(path: String): Endpoint[Unit, Unit, ErrorEnvelope[_ <: CustomError], Unit, Any] =
    rawEndpoint("admin").in(path).tag("Administration")

  protected val buildInfoEndpoint
    : Full[Unit, Unit, Unit, ErrorEnvelope[_ <: CustomError], SuccessEnvelope.Ok[TQuineInfo], Any, Future] =
    adminEndpoint("build-info")
      .name("Build Information")
      .description("Returns a JSON object containing information about how Quine was built")
      .get
      .out(statusCode(StatusCode.Ok))
      .out(jsonBody[SuccessEnvelope.Ok[TQuineInfo]])
      .serverLogic { _ =>
        runServerLogicOk(Future.successful(appMethods.buildInfo))(inp => SuccessEnvelope.Ok(inp))
      }

  protected val configEndpoint: ServerEndpoint.Full[Unit, Unit, Unit, ErrorEnvelope[
    _ <: CustomError,
  ], SuccessEnvelope.Ok[Json], Any, Future] =
    adminEndpoint("config")
      .name("Running Configuration")
      .description("""Fetch the full configuration of the running system. "Full" means that this
every option value is specified including all specified config files, command line
options, and default values.

This does _not_ include external options, for example, the
Pekko HTTP option `org.apache.pekko.http.server.request-timeout` can be used to adjust the web
server request timeout of this REST API, but it won't show up in the response of this
endpoint.
""").get
      .out(statusCode(StatusCode.Ok))
      .out(jsonBody[SuccessEnvelope.Ok[Json]])
      .serverLogic { _ =>
        runServerLogicOk(Future.successful(appMethods.config.loadedConfigJson))((inp: Json) =>
          SuccessEnvelope.Ok.apply(inp),
        )
      }

  protected val graphHashCodeEndpoint
    : ServerEndpoint.Full[Unit, Unit, (Option[Milliseconds], Option[String]), ErrorEnvelope[
      _ <: CustomError,
    ], SuccessEnvelope.Ok[TGraphHashCode], Any, Future] = adminEndpoint("graph-hash-code")
    .description("""Generate a hash of the state of the graph at the provided timestamp.
                   |
                   |This is done by materializing readonly/historical versions of all nodes at a particular timestamp and
                   |generating a checksum based on their (serialized) properties and edges.
                   |
                   |The timestamp defaults to the server's current clock time if not provided.
                   |
                   |Because this relies on historical nodes, results may be inconsistent if running on a configuration with
                   |journals disabled.""".stripMargin)
    .name("Graph Hashcode")
    .in(atTimeParameter)
    .in(namespaceParameter)
    .get
    .out(statusCode(StatusCode.Ok))
    .out(jsonBody[SuccessEnvelope.Ok[TGraphHashCode]])
    .serverLogic { case (atime, ns: Option[String]) =>
      runServerLogicOk(appMethods.graphHashCode(atime, namespaceFromParam(ns)))((inp: TGraphHashCode) =>
        SuccessEnvelope.Ok(inp),
      )
    }

  protected val livenessEndpoint: Full[Unit, Unit, Unit, ErrorEnvelope[_ <: CustomError], Unit, Any, Future] =
    adminEndpoint("liveness")
      .name("Process Liveness")
      .description("""This is a basic no-op endpoint for use when checking if the system is hung or responsive.
                     | The intended use is for a process manager to restart the process if the app is hung (non-responsive).
                     | It does not otherwise indicate readiness to handle data requests or system health.
                     | Returns a 204 response.""")
      .get
      .out(statusCode(StatusCode.NoContent).description("System is live"))
      .serverLogicSuccess(_ => Future.successful(()))

  protected val readinessEndpoint
    : Full[Unit, Unit, Unit, ErrorEnvelope[_ <: CustomError], SuccessEnvelope.NoContent.type, Any, Future] =
    adminEndpoint("readiness")
      .name("Process Readiness")
      .description("""This indicates whether the system is fully up and ready to service user requests.
The intended use is for a load balancer to use this to know when the instance is
up ready and start routing user requests to it.
""").get
      .out(statusCode(StatusCode.NoContent).description("System is ready to serve requests"))
      .out(emptyOutputAs(NoContent))
      .errorOutVariantPrepend {
        oneOfVariantFromMatchType(
          statusCode(StatusCode.ServiceUnavailable).and {
            jsonBody[ErrorEnvelope[ServiceUnavailable]]
              .description("System is not ready")
          },
        )
      }
      .serverLogic { _ =>
        runServerLogicFromEitherNoContent(
          Future.successful(
            Either.cond(
              appMethods.isReady,
              NoContent,
              ErrorEnvelope(ServiceUnavailable("System is not ready")),
            ),
          ),
        )
      }

  protected val gracefulShutdownEndpoint: ServerEndpoint.Full[Unit, Unit, Unit, ErrorEnvelope[
    _ <: CustomError,
  ], SuccessEnvelope.Accepted, Any, Future] =
    adminEndpoint("shutdown")
      .name("Graceful Shutdown")
      .description(
        "Initiate a graceful graph shutdown. Final shutdown may take a little longer. `200` indicates a shutdown has been successfully initiated.",
      )
      .post
      .out(statusCode(StatusCode.Accepted).description("Shutdown initiated"))
      .out(jsonBody[SuccessEnvelope.Accepted])
      .serverLogic { _ =>
        runServerLogicAccepted(appMethods.performShutdown())((inp: Unit) => SuccessEnvelope.Accepted())
      }

  protected val metaDataEndpoint: ServerEndpoint.Full[Unit, Unit, Unit, ErrorEnvelope[
    _ <: CustomError,
  ], SuccessEnvelope.Ok[Map[String, String]], Any, Future] =
    adminEndpoint("meta-data")
      .name("Persisted Meta-Data")
      .summary("Meta-data")
      .get
      .out(statusCode(StatusCode.Ok))
      .out(jsonBody[SuccessEnvelope.Ok[Map[String, String]]])
      .serverLogic { _ =>
        runServerLogicOk(appMethods.metaData)((inp: Map[String, String]) =>
          SuccessEnvelope.Ok(inp): SuccessEnvelope.Ok[Map[String, String]],
        )
      }

  protected val metricsEndpoint: ServerEndpoint.Full[Unit, Unit, Unit, ErrorEnvelope[
    _ <: CustomError,
  ], SuccessEnvelope.Ok[TMetricsReport], Any, Future] = adminEndpoint("metrics")
    .name("Metrics")
    .summary("Metrics Summary")
    .description(
      """Returns a JSON object containing metrics data used in the Quine
        |[Monitoring](https://docs.quine.io/core-concepts/operational-considerations.html#monitoring)
        |dashboard. The selection of metrics is based on current configuration and execution environment, and is
        |subject to change. A few metrics of note include:""".stripMargin.replace('\n', ' ') +
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
          | - `dgn-reg.count`: Number of in-memory registered DomainGraphNodes
          |""".stripMargin,
    )
    .get
    .out(statusCode(StatusCode.Ok))
    .out(jsonBody[SuccessEnvelope.Ok[TMetricsReport]])
    .serverLogic { _ =>
      runServerLogicOk(Future.successful(metricsReportFromV1Metrics(appMethods.metrics)))((inp: TMetricsReport) =>
        SuccessEnvelope.Ok(inp),
      )
    }

  //TODO shardMapLimitSchema
  protected val shardSizesEndpoint: ServerEndpoint.Full[Unit, Unit, Map[Int, TShardInMemoryLimit], ErrorEnvelope[
    _ <: CustomError,
  ], SuccessEnvelope.Ok[Map[Int, TShardInMemoryLimit]], Any, Future] =
    adminEndpoint("shards").put
      .name("Shard Sizes")
      .description("""Get and update the in-memory node limits.
                   |
                   |Sending a request containing an empty json object will return the current in-memory node settings.
                   |
                   |To apply different values, apply your edits to the returned document and sent those values in
                   |a new PUT request.
                   |""".stripMargin)
      .in("size-limits")
      .in(jsonOrYamlBody[Map[Int, TShardInMemoryLimit]](Some(exampleShardMap)))
      .out(statusCode(StatusCode.Ok))
      .out(jsonBody[SuccessEnvelope.Ok[Map[Int, TShardInMemoryLimit]]])
      .serverLogic { resizes =>
        runServerLogicOk(
          appMethods
            .shardSizes(resizes.view.mapValues(v => ShardInMemoryLimit(v.softLimit, v.hardLimit)).toMap)
            .map(f => f.view.mapValues(v => TShardInMemoryLimit(v.softLimit, v.hardLimit)).toMap)(
              ExecutionContext.parasitic,
            ),
        )((inp: Map[Int, TShardInMemoryLimit]) => SuccessEnvelope.Ok(inp))
      }

  protected val requestNodeSleepEndpoint: ServerEndpoint.Full[Unit, Unit, (QuineId, Option[String]), ErrorEnvelope[
    _ <: CustomError,
  ], SuccessEnvelope.Accepted, Any, Future] = adminEndpoint("nodes").post
    .name("Sleep Node")
    .description("""Attempt to put the specified node to sleep.
                   |
                   |This behavior is not guaranteed. Activity on the node will supersede this request""".stripMargin)
    .in(path[QuineId]("nodeIdSegment"))
    .in("request-sleep")
    .in(namespaceParameter)
    .out(statusCode(StatusCode.Accepted))
    .out(jsonBody[SuccessEnvelope.Accepted])
    .serverLogic { case (nodeId, namespace) =>
      runServerLogicAccepted(appMethods.requestNodeSleep(nodeId, namespaceFromParam(namespace)))((inp: Unit) =>
        SuccessEnvelope.Accepted(),
      )
    }

  val adminHiddenEndpoints: List[ServerEndpoint[Any, Future]] = List(metaDataEndpoint)

  val adminEndpoints: List[ServerEndpoint[Any, Future]] = List(
    buildInfoEndpoint,
    configEndpoint,
    graphHashCodeEndpoint,
    livenessEndpoint,
    metaDataEndpoint,
    metricsEndpoint,
    readinessEndpoint,
    requestNodeSleepEndpoint,
    shardSizesEndpoint,
    gracefulShutdownEndpoint,
  )

}
