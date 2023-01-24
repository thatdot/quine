package com.thatdot.quine.routes

import scala.util.Try

import endpoints4s.algebra.Tag
import endpoints4s.generic.{docs, title, unnamed}
import endpoints4s.{Valid, Validated}

import com.thatdot.quine.routes.exts.EndpointsWithCustomErrorText

/** Build information exposed to the user */
@title("System Build Information")
@docs("Information collected when this version of the system was compiled.")
final case class QuineInfo(
  @docs("Quine version") version: String,
  @docs("Current build git commit") gitCommit: Option[String],
  @docs("Current build commit date") gitCommitDate: Option[String],
  @docs("Java compilation version") javaVersion: String,
  @docs("Persistence data format version") persistenceWriteVersion: String
)

@title("Metrics Counter")
@unnamed
final case class Counter(
  name: String,
  count: Long
)

@title("Metrics Numeric Gauge")
@unnamed
final case class NumericGauge(
  name: String,
  value: Double
)

@title("Metrics Timer Summary")
@unnamed
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
  @docs("One-minute moving average rate at which events have occurred") oneMinuteRate: Double,
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

@title("Metrics Report")
final case class MetricsReport(
  @docs("A UTC Instant at which these metrics are accurate") atTime: java.time.Instant,
  @docs("All registered counters from this instance's metrics") counters: Seq[Counter],
  @docs("All registered timers from this instance's metrics") timers: Seq[TimerSummary],
  @docs("All registered numerical gauges from this instance's metrics") gauges: Seq[NumericGauge]
)

@title("Shard In-Memory Limits")
@unnamed
final case class ShardInMemoryLimit(
  @docs("Number of in-memory nodes past which shards will try to shut down nodes") softLimit: Int,
  @docs("Number of in-memory nodes past which shards will not load in new nodes") hardLimit: Int
)

@title("Graph hash code")
@unnamed
final case class GraphHashCode(
  @docs("Hash value derived from the state of the graph (nodes, properties, and edges)") value: Long,
  @docs("Time value used to derive the graph hash code") atTime: Long
)

trait AdministrationRoutes
    extends EndpointsWithCustomErrorText
    with endpoints4s.algebra.JsonEntitiesFromSchemas
    with endpoints4s.generic.JsonSchemas
    with exts.QuineEndpoints
    with exts.AnySchema {

  implicit final lazy val quineInfoSchema: JsonSchema[QuineInfo] =
    genericJsonSchema[QuineInfo]
      .withExample(
        QuineInfo(
          version = "0.1",
          gitCommit = Some("b416b354bd4d5d2a9fe39bc55153afd312260f29"),
          gitCommitDate = Some("2022-12-29T15:09:32-0500"),
          javaVersion = "OpenJDK 64-Bit Server VM 1.8.0_312 (Azul Systems, Inc.)",
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

  implicit final lazy val graphHashCodeSchema: JsonSchema[GraphHashCode] =
    genericJsonSchema[GraphHashCode]

  private val api = path / "api" / "v1"
  protected val admin: Path[Unit] = api / "admin"

  protected val adminTag: Tag = Tag("Administration")
    .withDescription(Some("Operations related to the management and configuration of the system"))

  final val buildInfo: Endpoint[Unit, QuineInfo] =
    endpoint(
      request = get(admin / "build-info"),
      response = ok(jsonResponse[QuineInfo]),
      docs = EndpointDocs()
        .withSummary(Some("Build Information"))
        .withDescription(Some("""Returns a JSON object containing information about how Quine was built"""))
        .withTags(List(adminTag))
    )

  final def config(configExample: ujson.Value): Endpoint[Unit, ujson.Value] =
    endpoint(
      request = get(admin / "config"),
      response = ok(jsonResponseWithExample[ujson.Value](configExample)(anySchema(None))),
      docs = EndpointDocs()
        .withSummary(Some("Running Configuration"))
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
        .withSummary(Some("Container Liveness"))
        .withDescription(
          Some(
            """This is for integrating with liveness probes when containerizing the system (eg. in
              |Kubernetes or Docker).
              |
              |A 204 response indicates that the container is alive.""".stripMargin
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
        .withSummary(Some("Container Readiness"))
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
        .withSummary(Some("Metrics Summary"))
        .withDescription(
          Some(
            """Returns a JSON object containing metrics data used in the Quine 
              |[Monitoring](https://docs.quine.io/core-concepts/operational-considerations.html#monitoring) 
              |dashboard.""".stripMargin
          )
        )
        .withTags(List(adminTag))
    )

  final val shutdown: Endpoint[Unit, Unit] =
    endpoint(
      request = post(admin / "shutdown", emptyRequest),
      response = accepted(docs = Some("Shutdown initiated")),
      docs = EndpointDocs()
        .withSummary(
          Some("Graceful Shutdown")
        )
        .withDescription(
          Some(
            "Initiate a graceful graph shutdown. Final shutdown may take a little longer."
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

    val exampleShardMap = (0 to 3).map(_ -> ShardInMemoryLimit(10000, 75000)).toMap
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
      request =
        post(admin / "shard-sizes", jsonOrYamlRequestWithExample[Map[Int, ShardInMemoryLimit]](exampleShardMap)),
      response = ok(
        jsonResponseWithExample[Map[Int, ShardInMemoryLimit]](exampleShardMap)
      ),
      docs = EndpointDocs()
        .withSummary(Some("Shard Sizes"))
        .withDescription(
          Some(
            """Get and update the in-memory node limits.
              |
              |Sending a request containing an empty json object will return the current in-memory node settings.
              |
              |To apply different values, apply your edits to the returned document and sent those values in
              |a new POST request.
              |""".stripMargin
          )
        )
        .withTags(List(adminTag))
    )
  }

  final val requestNodeSleep: Endpoint[Id, Unit] =
    endpoint(
      request = post(admin / "request-node-sleep" / nodeIdSegment, emptyRequest),
      response = accepted(emptyResponse),
      docs = EndpointDocs()
        .withSummary(Some("Sleep Node"))
        .withDescription(Some("""Attempt to put the specified node to sleep.
            |
            |This behavior is not guaranteed. Activity on the node will supercede this request""".stripMargin))
        .withTags(List(adminTag))
    )

  final val graphHashCode: Endpoint[AtTime, GraphHashCode] =
    endpoint(
      request = get(admin / "graph-hash-code" /? atTime),
      response = ok(jsonResponse[GraphHashCode]),
      docs = EndpointDocs()
        .withSummary(Some("Graph Hash Code"))
        .withDescription(
          Some("""Generate a hash of the state of the graph at the provided timestamp.
                 |
                 |This is done by materializing readonly/historical versions of all nodes at a particular timestamp and
                 |generating a checksum based on their (serialized) properties and edges.
                 |
                 |The timestamp defaults to the server's current clock time if not provided.
                 |
                 |Because this relies on historical nodes, results may be inconsistent if running on a configuration with
                 |journals disabled.""".stripMargin)
        )
        .withTags(List(adminTag))
    )
}
