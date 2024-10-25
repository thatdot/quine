package com.thatdot.quine.v2api

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt

import org.apache.pekko.http.scaladsl.model._
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.http.scaladsl.testkit.ScalatestRouteTest
import org.apache.pekko.util.Timeout

import io.circe.Encoder
import io.circe.syntax.EncoderOps
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import com.thatdot.quine.app.config.QuineConfig
import com.thatdot.quine.app.v2api.endpoints.V2IngestEntities.QuineIngestConfiguration
import com.thatdot.quine.app.v2api.endpoints.{V2IngestEntities, V2IngestSchemas}
import com.thatdot.quine.app.v2api.{OssApiMethods, V2OssRoutes}
import com.thatdot.quine.app.{IngestTestGraph, QuineApp}
import com.thatdot.quine.ingest2.ArbitraryIngests
import com.thatdot.quine.routes.KinesisIngest.IteratorType
import com.thatdot.quine.routes.RecordDecodingType
import com.thatdot.quine.util.Log.LogConfig

object EndpointValidationSupport {
  implicit val logConfig: LogConfig = LogConfig.permissive
  private val graph = IngestTestGraph.makeGraph("endpoint-test")
  private val quineApp = new QuineApp(graph)
  private val app = new OssApiMethods(graph, quineApp, QuineConfig(), Timeout(5.seconds))
  private val apiRoutes = new V2OssRoutes(app)
  implicit val ec: ExecutionContext.parasitic.type = ExecutionContext.parasitic
  lazy val routes: Route = apiRoutes.v2Routes(ingestOnly = false)

  def toJsonHttpEntity[T](t: T)(implicit encoder: Encoder[T]): RequestEntity =
    HttpEntity(MediaTypes.`application/json`, t.asJson.spaces2)

  def postRawString[T](uri: String, t: String): HttpRequest =
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
    with ArbitraryIngests
    with V2IngestSchemas {
  import EndpointValidationSupport._
  val baseUrl = "/api/v2"

  "A kinesis ingest with illegal iterator type" should "fail with 400" in {
    val url = s"$baseUrl/ingest/foo"
    val kinesisIngest = kinesisGen.sample.get.copy(
      iteratorType = IteratorType.AfterSequenceNumber("ignore"),
      numRetries = 3, //TODO java.lang.IllegalArgumentException: maxAttempts must be positive
      shardIds = Some(Set("ignore1", "ignore2")),
      recordDecoders =
        Seq(RecordDecodingType.Gzip, RecordDecodingType.Gzip, RecordDecodingType.Gzip, RecordDecodingType.Gzip),
    )

    val quineIngestConfiguration: QuineIngestConfiguration = arbIngest.arbitrary.sample.get.copy(source = kinesisIngest)
    // tests:
    post(url, quineIngestConfiguration) ~> routes ~> check {

      status.intValue() shouldEqual 400

      //TODO this should also inspect the output and check that validation strings are correctly generated
    }

  }

  "A kafka ingest with unrecognized properties" should "fail with 400" in {
    val url = s"$baseUrl/ingest/foo"
    val kafkaIngest: V2IngestEntities.KafkaIngest = kafkaGen.sample.get.copy(kafkaProperties =
      Map(
        "Unrecognized.property.name" -> "anything",
        "bootstrap.servers" -> "this is an illegal field and should not be used",
      ),
    )

    val quineIngestConfiguration: QuineIngestConfiguration = arbIngest.arbitrary.sample.get.copy(source = kafkaIngest)
    // tests:
    post(url, quineIngestConfiguration) ~> routes ~> check {
      status.intValue() shouldEqual 400
      //TODO this should also inspect the output and check that validation strings are correctly generated
      println(s"\n\nSTATUS = $status")
    }

  }

}
