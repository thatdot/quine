package com.thatdot.quine.v2api

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt

import org.apache.pekko.http.scaladsl.model._
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import org.apache.pekko.testkit.TestDuration
import org.apache.pekko.util.Timeout

import io.circe.syntax.EncoderOps
import io.circe.{Encoder, Json}
import org.scalacheck.Gen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import com.thatdot.api.v2.{ResourceName, TypeDiscriminatorConfig}
import com.thatdot.quine.app.config.{FileAccessPolicy, QuineConfig, ResolutionMode}
import com.thatdot.quine.app.model.outputs2.query.standing.LocalTapBus
import com.thatdot.quine.app.v2api.definitions.ingest2.ApiIngest.IngestSource.Kinesis.IteratorType
import com.thatdot.quine.app.v2api.definitions.ingest2.ApiIngest.{Oss, RecordDecodingType}
import com.thatdot.quine.app.v2api.definitions.ingest2.{ApiIngest => Api}
import com.thatdot.quine.app.v2api.definitions.query.standing.StandingQuery.StandingQueryDefinition
import com.thatdot.quine.app.v2api.definitions.query.standing.StandingQueryPattern.Cypher
import com.thatdot.quine.app.v2api.definitions.query.standing.StandingQueryPattern.StandingQueryMode.MultipleValues
import com.thatdot.quine.app.v2api.definitions.query.standing.StandingQueryResultWorkflow
import com.thatdot.quine.app.v2api.{OssApiMethods, V2OssRoutes}
import com.thatdot.quine.app.{IngestTestGraph, QuineApp}
import com.thatdot.quine.ingest2.IngestGenerators
import com.thatdot.quine.util.TestLogging._

object EndpointValidationSupport {
  private val graph = IngestTestGraph.makeGraph("endpoint-test")
  private val quineApp =
    new QuineApp(graph, false, FileAccessPolicy(List.empty, ResolutionMode.Dynamic), new LocalTapBus)
  private val app = new OssApiMethods(graph, quineApp, QuineConfig(), Timeout(5.seconds))
  private val apiRoutes = new V2OssRoutes(app)
  implicit val ec: ExecutionContext.parasitic.type = ExecutionContext.parasitic
  lazy val routes: Route = apiRoutes.v2Routes(ingestOnly = false)

  def toJsonHttpEntity[T](t: T)(implicit encoder: Encoder[T]): RequestEntity =
    HttpEntity(MediaTypes.`application/json`, t.asJson.spaces2)

  def postRawString(uri: String, t: String): HttpRequest =
    HttpRequest(HttpMethods.POST, uri, headers = Seq(), HttpEntity(MediaTypes.`application/json`, t))

  def post[T](uri: String, t: T)(implicit
    encoder: Encoder[T],
  ): HttpRequest =
    HttpRequest(HttpMethods.POST, uri, headers = Seq(), toJsonHttpEntity(t))
}
class EndpointValidationSpec
    extends AnyFlatSpec
    with ScalaCheckDrivenPropertyChecks
    with Matchers
    with ScalatestRouteTest
    with TypeDiscriminatorConfig {

  import EndpointValidationSupport._
  import IngestGenerators.Arbs._
  import IngestGenerators.Gens.{kafka => kafkaGen, kinesis => kinesisGen}

  val baseUrl = "/api/v2"
  val graphBaseUrl: String = s"$baseUrl/graph/quine"

  "A kinesis ingest with illegal iterator type" should "fail with 400" in {
    forAll(kinesisGen, arbQuineIngestConfiguration.arbitrary) { (kinesis, ingest) =>
      val url = s"$graphBaseUrl/ingests"
      val kinesisIngest = kinesis.copy(
        iteratorType = IteratorType.AfterSequenceNumber("ignore"),
        numRetries = 3, //TODO java.lang.IllegalArgumentException: maxAttempts must be positive
        shardIds = Some(Set("ignore1", "ignore2")),
        recordDecoders =
          Seq(RecordDecodingType.Gzip, RecordDecodingType.Gzip, RecordDecodingType.Gzip, RecordDecodingType.Gzip),
      )

      val quineIngestConfiguration: Api.Oss.QuineIngestConfiguration =
        ingest.copy(source = kinesisIngest)

      // Increase timeout for check using implicit, for use when many tests are running at once and longer timeouts may be needed.
      implicit val timeout: RouteTestTimeout = RouteTestTimeout(10.seconds.dilated)
      post(url, quineIngestConfiguration) ~> routes ~> check {

        status.intValue() shouldEqual 400

        //TODO this should also inspect the output and check that validation strings are correctly generated
      }
    }
  }

  "A kinesis ingest with invalid numRetries" should "fail with 400" in {
    forAll(Gen.chooseNum(Int.MinValue, 0)) { badRetries =>
      val url = s"$graphBaseUrl/ingests"
      val kinesisIngest = Api.IngestSource.Kinesis(
        format = Api.IngestFormat.StreamingFormat.Json,
        streamName = "test-stream",
        shardIds = None,
        credentials = None,
        region = None,
        iteratorType = IteratorType.Latest,
        numRetries = badRetries,
        recordDecoders = Seq.empty,
      )
      val config = Oss.QuineIngestConfiguration(
        name = ResourceName.unsafeFromString("test-kinesis-bad-retries"),
        source = kinesisIngest,
        query = "CREATE ($that)",
      )
      implicit val timeout: RouteTestTimeout = RouteTestTimeout(10.seconds.dilated)
      post(url, config) ~> routes ~> check {
        status.intValue() shouldEqual 400
      }
    }
  }

  "A kafka ingest with unrecognized properties" should "fail with 400" in {
    forAll(kafkaGen, arbQuineIngestConfiguration.arbitrary) { (kafka, ingest) =>
      val url = s"$graphBaseUrl/ingests"
      val kafkaIngest: Api.IngestSource.Kafka = kafka.copy(kafkaProperties =
        Map(
          "Unrecognized.property.name" -> "anything",
          "bootstrap.servers" -> "this is an illegal field and should not be used",
        ),
      )
      val quineIngestConfiguration: Oss.QuineIngestConfiguration =
        ingest.copy(source = kafkaIngest)
      // tests:
      post(url, quineIngestConfiguration) ~> routes ~> check {
        status.intValue() shouldEqual 400
        //TODO this should also inspect the output and check that validation strings are correctly generated
      }
    }
  }

  "An ingest config with an unknown top-level field" should "fail with 400 and name the offending field" in {
    val url = s"$graphBaseUrl/ingests"
    val kinesisIngest = Api.IngestSource.Kinesis(
      format = Api.IngestFormat.StreamingFormat.Json,
      streamName = "test-stream",
      shardIds = None,
      credentials = None,
      region = None,
      iteratorType = IteratorType.Latest,
      numRetries = 3,
      recordDecoders = Seq.empty,
    )
    val config = Oss.QuineIngestConfiguration(
      name = ResourceName.unsafeFromString("test-unknown-field"),
      source = kinesisIngest,
      query = "CREATE ($that)",
    )
    // A valid payload with one extra, unrecognized top-level field the deserializer must reject.
    val bodyWithUnknownField =
      config.asJson.deepMerge(Json.obj("notARealField" -> Json.fromString("oops"))).noSpaces
    implicit val timeout: RouteTestTimeout = RouteTestTimeout(10.seconds.dilated)
    postRawString(url, bodyWithUnknownField) ~> routes ~> check {
      status.intValue() shouldEqual 400
      responseAs[String] should include("notARealField")
    }
  }

  "A kinesis ingest with no explicit region" should "not return 500" in {
    val url = s"$graphBaseUrl/ingests"
    val kinesisIngest = Api.IngestSource.Kinesis(
      format = Api.IngestFormat.StreamingFormat.Json,
      streamName = "test-stream",
      shardIds = None,
      credentials = None,
      region = None,
      iteratorType = IteratorType.Latest,
      numRetries = 3,
      recordDecoders = Seq.empty,
    )
    val config = Oss.QuineIngestConfiguration(
      name = ResourceName.unsafeFromString("test-kinesis-no-region"),
      source = kinesisIngest,
      query = "CREATE ($that)",
    )
    implicit val timeout: RouteTestTimeout = RouteTestTimeout(10.seconds.dilated)
    post(url, config) ~> routes ~> check {
      // 400 if no region is resolvable from the environment, 201 if one is.
      // The key invariant: this must never be a 500.
      status.intValue() should (equal(400) or equal(201))
    }
  }

  "GET /api/v2/openapi.json" should "serve the V2 spec with AIP-136 colon-verb paths rewritten" in {
    // First spec request triggers tapir's full OpenAPI generation; that exceeds the
    // default 1-second test timeout under CI load.
    implicit val timeout: RouteTestTimeout = RouteTestTimeout(10.seconds.dilated)
    HttpRequest(HttpMethods.GET, "/api/v2/openapi.json") ~> routes ~> check {
      status.intValue() shouldEqual 200
      val body = entityAs[String]
      body should include(""""/api/v2/graph/quine/cypher:query"""")
      body should include(""""/api/v2/graph/quine/ingests/{ingestName}:pause"""")
      // Synthetic marker should never appear in the served spec.
      body should not include "__colonVerb__"
    }
  }

  "GET /openapi.json" should "alias the V2 spec" in {
    implicit val timeout: RouteTestTimeout = RouteTestTimeout(10.seconds.dilated)
    HttpRequest(HttpMethods.GET, "/openapi.json") ~> routes ~> check {
      status.intValue() shouldEqual 200
      val body = entityAs[String]
      body should include(""""/api/v2/graph/quine/cypher:query"""")
    }
  }

  // Identifiers that may become path segments must not contain a colon (AIP-122).
  // A colon-bearing name would make AIP-136 custom-verb URLs (e.g. `/ingests/{name}:pause`)
  // ambiguous. The rules below verify that every entrance for a resource name —
  // URL path, custom-verb segment, JSON body — refuses colons before reaching the
  // application layer.

  "DELETE /ingests/{name} with a colon in the name" should "fail with 400" in {
    implicit val timeout: RouteTestTimeout = RouteTestTimeout(10.seconds.dilated)
    HttpRequest(HttpMethods.DELETE, s"$graphBaseUrl/ingests/foo:bar") ~> routes ~> check {
      status.intValue() shouldEqual 400
    }
  }

  "POST /ingests/{name}:pause with a colon in the name prefix" should "fail with 400" in {
    // Verifies the AIP-136 ambiguity is closed: a request like `foo:bar:pause` must not
    // silently decode to name=`foo:bar` verb=`pause`. The colon-verb codec strips the
    // trailing `:pause` and delegates the remainder to the `ResourceName` codec, which
    // rejects the colon.
    implicit val timeout: RouteTestTimeout = RouteTestTimeout(10.seconds.dilated)
    HttpRequest(HttpMethods.POST, s"$graphBaseUrl/ingests/foo:bar:pause") ~> routes ~> check {
      status.intValue() shouldEqual 400
    }
  }

  "POST /ingests with a colon in the body's name field" should "fail with 400" in {
    // `unsafeFromString` bypasses ResourceName construction validation so we can serialize a
    // known-invalid name and exercise the server-side decoder rejection.
    val config = Oss.QuineIngestConfiguration(
      name = ResourceName.unsafeFromString("bad:name"),
      source = Api.IngestSource.NumberIterator(0, None),
      query = "CREATE ($that)",
    )
    implicit val timeout: RouteTestTimeout = RouteTestTimeout(10.seconds.dilated)
    post(s"$graphBaseUrl/ingests", config) ~> routes ~> check {
      status.intValue() shouldEqual 400
    }
  }

  "DELETE /standingQueries/{name} with a colon in the name" should "fail with 400" in {
    implicit val timeout: RouteTestTimeout = RouteTestTimeout(10.seconds.dilated)
    HttpRequest(HttpMethods.DELETE, s"$graphBaseUrl/standingQueries/foo:bar") ~> routes ~> check {
      status.intValue() shouldEqual 400
    }
  }

  "POST /standingQueries with a colon in the body's name field" should "fail with 400" in {
    val sq = StandingQueryDefinition(
      name = ResourceName.unsafeFromString("bad:name"),
      pattern = Cypher("MATCH (n) RETURN n", MultipleValues),
    )
    implicit val timeout: RouteTestTimeout = RouteTestTimeout(10.seconds.dilated)
    post(s"$graphBaseUrl/standingQueries", sq) ~> routes ~> check {
      status.intValue() shouldEqual 400
    }
  }

  "POST /standingQueries/{name}/outputs with a colon in the workflow's name field" should "fail with 400" in {
    val workflow =
      StandingQueryResultWorkflow.exampleToStandardOut.copy(name = ResourceName.unsafeFromString("bad:name"))
    implicit val timeout: RouteTestTimeout = RouteTestTimeout(10.seconds.dilated)
    post(s"$graphBaseUrl/standingQueries/test-sq/outputs", workflow) ~> routes ~> check {
      status.intValue() shouldEqual 400
    }
  }

  "GET /api/v2/openapi.json" should "publish the resource-name format on path parameters" in {
    implicit val timeout: RouteTestTimeout = RouteTestTimeout(10.seconds.dilated)
    HttpRequest(HttpMethods.GET, "/api/v2/openapi.json") ~> routes ~> check {
      status.intValue() shouldEqual 200
      val body = entityAs[String]
      // ResourceName carries a Schema with format "resource-name"; that should surface
      // in the spec for every {name}-style path parameter we replaced — including the
      // verb-suffix variants, whose codec now forwards the base codec's schema rather
      // than defaulting to a bare string.
      val resourceNameParameters =
        """"name":"ingestName".*?"format":"resource-name"""".r.findAllIn(body).length
      // 3 occurrences: bare `{ingestName}` (DELETE/GET), `{ingestName}:pause`, `{ingestName}:resume`.
      withClue(s"resource-name format should appear on every ingestName parameter: $resourceNameParameters")(
        resourceNameParameters should be >= 3,
      )
    }
  }
}
