package com.thatdot.quine.routes

import scala.util.Try

import endpoints4s.algebra.Tag
import endpoints4s.generic.{docs, title, unnamed}
import endpoints4s.{Valid, Validated}

/** Build information exposed to the user */
@title("System Build Information")
@docs("Information collected when this version of the system was compiled.")
final case class QuineInfo(
  @docs("version of the system") version: String,
  @docs("commit associated with this build") gitCommit: Option[String],
  @docs("date associated with the commit") gitCommitDate: Option[String],
  @docs("version of Scala used during compilation") scalaVersion: String,
  @docs("version of Java used during compilation") javaVersion: String,
  @docs("version of SBT build tool used for compilation") sbtVersion: String,
  @docs("version of the persisted data format written by this build") persistenceWriteVersion: String
)

@title("Metrics Counter")
final case class Counter(
  name: String,
  count: Long
)

@title("Metrics Numeric Gauge")
final case class NumericGauge(
  name: String,
  value: Double
)

@title("Metrics Timer Summary")
@docs("A rough cumulative histogram of times recorded by a timer. All times in milliseconds.")
final case class TimerSummary(
  name: String,
  // standard metrics
  min: Double,
  max: Double,
  median: Double,
  mean: Double,
  q1: Double,
  q3: Double,
  @docs("one-minute moving average rate at which events have occurred") oneMinuteRate: Double,
  @docs("90th percentile time; representative of heavy load") `90`: Double,
  @docs("99th percentile time; representative of worst case or peak load") `99`: Double,
  // pareto principle thresholds
  `80`: Double,
  `20`: Double,
  `10`: Double
)

object MetricsReport {
  def empty: MetricsReport =
    MetricsReport(java.time.Instant.now(), Vector.empty, Vector.empty, Vector.empty)
}

final case class MetricsReport(
  @docs("a UTC Instant at which these metrics are accurate") atTime: java.time.Instant,
  @docs("all registered counters from this instance's metrics") counters: Seq[Counter],
  @docs("all registered timers from this instance's metrics") timers: Seq[TimerSummary],
  @docs("all registered numerical gauges from this instance's metrics") gauges: Seq[NumericGauge]
)

@title("Shard In-Memory Limits")
@unnamed
final case class ShardInMemoryLimit(
  @docs("number of in-memory nodes past which shards will try to shut down nodes") softLimit: Int,
  @docs("number of in-memory nodes past which shards will not load in new nodes") hardLimit: Int
)

trait AdministrationRoutes
    extends endpoints4s.algebra.Endpoints
    with endpoints4s.algebra.JsonEntitiesFromSchemas
    with endpoints4s.generic.JsonSchemas
    with exts.QuineEndpoints
    with exts.AnySchema {

  implicit final lazy val quineInfoSchema: JsonSchema[QuineInfo] =
    genericJsonSchema[QuineInfo]
      .withExample(
        QuineInfo(
          version = "0.1",
          gitCommit = None,
          gitCommitDate = None,
          scalaVersion = "2.12.15",
          javaVersion = "OpenJDK 64-Bit Server VM 1.8.0_312 (Azul Systems, Inc.)",
          sbtVersion = "1.6.1",
          persistenceWriteVersion = "10.1.0"
        )
      )

  implicit final lazy val counterSchema: JsonSchema[Counter] = genericJsonSchema[Counter]
  implicit final lazy val timerSummarySchema: JsonSchema[TimerSummary] =
    genericJsonSchema[TimerSummary]
  implicit final lazy val numGaugeSchema: JsonSchema[NumericGauge] = genericJsonSchema[NumericGauge]
  implicit final lazy val metricsReportSchema: JsonSchema[MetricsReport] =
    genericJsonSchema[MetricsReport]

  implicit final lazy val shardInMemoryLimitSchema: JsonSchema[ShardInMemoryLimit] =
    genericRecord[ShardInMemoryLimit]

  private val api = path / "api" / "v1"
  protected val admin: Path[Unit] = api / "admin"

  protected val adminTag: Tag = Tag("Administration")
    .withDescription(Some("Operations related to the management and configuration of the system"))

  final val buildInfo: Endpoint[Unit, QuineInfo] =
    endpoint(
      request = get(admin / "build-info"),
      response = ok(jsonResponse[QuineInfo]),
      docs = EndpointDocs()
        .withSummary(Some("information about how the system was compiled"))
        .withTags(List(adminTag))
    )

  final val config: Endpoint[Unit, ujson.Value] =
    endpoint(
      request = get(admin / "config"),
      response = ok(jsonResponse[ujson.Value](anySchema(None))),
      docs = EndpointDocs()
        .withSummary(Some("current configuration settings (set at startup)"))
        .withDescription(
          Some(
            """Fetch the full configuration of the running system. "Full" means that this
              |every option value is specified including all specified config files, command line
              |options, and default values.
              |
              |This does _not_ include external options, for example, the
              |Akka HTTP option `akka.http.server.request-timeout` can be used to adjust the web
              |server request timeout of this REST API, but it won't show up in the response of this
              |endpoint.
              |""".stripMargin
          )
        )
        .withTags(List(adminTag))
    )

  final val livenessProbe: Endpoint[Unit, Boolean] =
    endpoint(
      request = get(admin / "liveness"),
      response = noContent(docs = Some("System is live"))
        .orElse(serviceUnavailable(docs = Some("System is not live")))
        .xmap(_.isLeft)(isReady => if (isReady) Left(()) else Right(())),
      docs = EndpointDocs()
        .withSummary(Some("Check whether the system is live"))
        .withDescription(
          Some(
            """This is for integrating with liveness probes when containerizing the system (eg. in
              |Kubernetes or Docker). The question being answered with the status code response is
              |whether the container is alive.
              |""".stripMargin
          )
        )
        .withTags(List(adminTag))
    )

  final val readinessProbe: Endpoint[Unit, Boolean] =
    endpoint(
      request = get(admin / "readiness"),
      response = noContent(docs = Some("System is ready"))
        .orElse(serviceUnavailable(docs = Some("System is not ready")))
        .xmap(_.isLeft)(isReady => if (isReady) Left(()) else Right(())),
      docs = EndpointDocs()
        .withSummary(Some("Check whether the system is ready"))
        .withDescription(
          Some(
            """This is for integrating with liveness probes when containerizing the system (eg. in
              |Kubernetes or Docker). The question being answered with the status code response is
              |whether the container is ready to accept user requests.
              |""".stripMargin
          )
        )
        .withTags(List(adminTag))
    )

  final val metrics: Endpoint[Unit, MetricsReport] =
    endpoint(
      request = get(admin / "metrics"),
      response = ok(jsonResponse[MetricsReport]),
      docs = EndpointDocs()
        .withSummary(Some("Gets current metrics about the running quine core."))
        .withTags(List(adminTag))
    )

  final val shutdown: Endpoint[Unit, Unit] =
    endpoint(
      request = post(admin / "shutdown", emptyRequest),
      response = accepted(docs = Some("Shutdown initiated")),
      docs = EndpointDocs()
        .withSummary(
          Some("graceful shutdown of the graph")
        )
        .withDescription(
          Some(
            "Initiate graph shutdown. Final shutdown may a little longer."
          )
        )
        .withTags(List(adminTag))
    )

  final val metaData: Endpoint[Unit, Map[String, BStr]] =
    endpoint(
      request = get(admin / "meta-data"),
      response = ok(jsonResponse[Map[String, BStr]]),
      docs = EndpointDocs()
        .withSummary(
          Some("fetch the persisted meta-data")
        )
        .withTags(List(adminTag))
    )

  final val shardSizes: Endpoint[Map[Int, ShardInMemoryLimit], Map[Int, ShardInMemoryLimit]] = {
    implicit val shardMapLimitSchema: JsonSchema[Map[Int, ShardInMemoryLimit]] = mapJsonSchema[ShardInMemoryLimit]
      .xmapPartial { (map: Map[String, ShardInMemoryLimit]) =>
        map.foldLeft[Validated[Map[Int, ShardInMemoryLimit]]](Valid(Map.empty)) { case (accV, (strKey, limit)) =>
          for {
            acc <- accV
            intKey <- Validated.fromTry(Try(strKey.toInt))
          } yield acc + (intKey -> limit)
        }
      } {
        _.map { case (intKey, limit) => intKey.toString -> limit }.toMap
      }
      .withTitle("Shard Sizes Map")
      .withDescription("A map of shard IDs to shard in-memory node limits")

    endpoint(
      request = post(admin / "shard-sizes", jsonRequestWithExample[Map[Int, ShardInMemoryLimit]](Map.empty)),
      response = ok(
        jsonResponseWithExample[Map[Int, ShardInMemoryLimit]](
          (0 to 3).map(_ -> ShardInMemoryLimit(10000, 75000)).toMap
        )
      ),
      docs = EndpointDocs()
        .withSummary(Some("update the in-memory node limits of non-historical shards"))
        .withDescription(
          Some(
            """Get the in-memory node limits non-historical shards, after updating the limits
              |specified in the request body. If you do not know what the current in-memory limits
              |are, start by querying this endpoint with and empty object, then issue a second
              |request with a modified version of the limits returned from the first request.
              |""".stripMargin
          )
        )
        .withTags(List(adminTag))
    )
  }
}
