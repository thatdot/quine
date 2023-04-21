package com.thatdot.quine.routes

import scala.util.Try

import endpoints4s.algebra.Tag
import endpoints4s.generic.{docs, title, unnamed}
import endpoints4s.{Valid, Validated}
import io.circe.Json

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
@docs("Counters record a single shared count, and give that count a name")
@unnamed
final case class Counter(
  @docs("Name of the metric being reported") name: String,
  @docs("The value tracked by this counter") count: Long
)

@title("Metrics Numeric Gauge")
@docs("Gauges provide a single point-in-time measurement, and give that measurement a name")
@unnamed
final case class NumericGauge(
  @docs("Name of the metric being reported") name: String,
  @docs("The latest measurement recorded by this gauge") value: Double
)

@title("Metrics Timer Summary")
@unnamed
@docs("""A rough cumulative histogram of times recorded by a timer, as well as the average rate at which that timer is
        |used to take new measurements. All times in milliseconds.""".stripMargin.replace('\n', ' '))
final case class TimerSummary(
  @docs("Name of the metric being reported") name: String,
  // standard metrics
  @docs("Fastest recorded time") min: Double,
  @docs("Slowest recorded time") max: Double,
  @docs("Median recorded time") median: Double,
  @docs("Average recorded time") mean: Double,
  @docs("First-quartile time") q1: Double,
  @docs("Third-quartile time") q3: Double,
  @docs(
    "Average per-second rate of new events over the last one minute"
  ) oneMinuteRate: Double,
  @docs("90th percentile time") `90`: Double,
  @docs("99th percentile time") `99`: Double,
  // pareto principle thresholds
  @docs("80th percentile time") `80`: Double,
  @docs("20th percentile time") `20`: Double,
  @docs("10th percentile time") `10`: Double
)

object MetricsReport {
  def empty: MetricsReport =
    MetricsReport(java.time.Instant.now(), Vector.empty, Vector.empty, Vector.empty)
}

@title("Metrics Report")
@docs("""A selection of metrics registered by Quine, its libraries, and the JVM. Reported metrics may change
    |based on which ingests and standing queries have been running since Quine startup, as well as the JVM distribution
    |running Quine and the packaged version of any dependencies.""".stripMargin.replace('\n', ' '))
final case class MetricsReport(
  @docs("A UTC Instant at which the returned metrics were collected") atTime: java.time.Instant,
  @docs("General-purpose counters for single numerical values") counters: Seq[Counter],
  @docs(
    "Timers which measure how long an operation takes and how often that operation was timed, in milliseconds. " +
    "These are measured with wall time, and hence may be skewed by other system events outside our control like " +
    "GC pauses or system load."
  ) timers: Seq[
    TimerSummary
  ],
  @docs("Gauges which report an instantaneously-sampled reading of a particular metric") gauges: Seq[NumericGauge]
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

  implicit final lazy val quineInfoSchema: Record[QuineInfo] =
    genericRecord[QuineInfo]
      .withExample(
        QuineInfo(
          version = "0.1",
          gitCommit = Some("b416b354bd4d5d2a9fe39bc55153afd312260f29"),
          gitCommitDate = Some("2022-12-29T15:09:32-0500"),
          javaVersion = "OpenJDK 64-Bit Server VM 1.8.0_312 (Azul Systems, Inc.)",
          persistenceWriteVersion = "10.1.0"
        )
      )

  implicit final lazy val counterSchema: Record[Counter] = genericRecord[Counter]
  implicit final lazy val timerSummarySchema: Record[TimerSummary] =
    genericRecord[TimerSummary]
  implicit final lazy val numGaugeSchema: Record[NumericGauge] = genericRecord[NumericGauge]
  implicit final lazy val metricsReportSchema: Record[MetricsReport] =
    genericRecord[MetricsReport]

  implicit final lazy val shardInMemoryLimitSchema: Record[ShardInMemoryLimit] =
    genericRecord[ShardInMemoryLimit]

  implicit final lazy val graphHashCodeSchema: Record[GraphHashCode] =
    genericRecord[GraphHashCode]

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

  final def config(configExample: Json): Endpoint[Unit, Json] =
    endpoint(
      request = get(admin / "config"),
      response = ok(jsonResponseWithExample[Json](configExample)(anySchema(None))),
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

  final val livenessProbe: Endpoint[Unit, Unit] =
    endpoint(
      request = get(admin / "liveness"),
      response = noContent(docs = Some("System is live")),
      docs = EndpointDocs()
        .withSummary(Some("Is the process responsive?"))
        .withDescription(
          Some(
            """This is a basic no-op endpoint for use when checking if the system is hung or responsive.
              | The intended use is for a process manager to restart the process if the app is hung (non-responsive).
              | It does not otherwise indicate readiness to handle data requests or system health.
              | Returns a 204 response.""".stripMargin
          )
        )
        .withTags(List(adminTag))
    )

  final val readinessProbe: Endpoint[Unit, Boolean] =
    endpoint(
      request = get(admin / "readiness"),
      response = noContent(docs = Some("System is ready to serve requests"))
        .orElse(serviceUnavailable(docs = Some("System is not ready")))
        .xmap(_.isLeft)(isReady => if (isReady) Left(()) else Right(())),
      docs = EndpointDocs()
        .withSummary(Some("Is the system able to handle user requests?"))
        .withDescription(
          Some(
            """This indicates whether the system is fully up and ready to service user requests.
              |The intended use is for a load balancer to use this to know when the instance is
              |up ready and start routing user requests to it.
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
                |""".stripMargin
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
      .withExample(exampleShardMap)

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
            |This behavior is not guaranteed. Activity on the node will supersede this request""".stripMargin))
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
