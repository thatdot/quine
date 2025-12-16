package com.thatdot.quine.outputs

import io.circe.syntax.EncoderOps
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import com.thatdot.api.v2.outputs.DestinationSteps.KafkaPropertyValue
import com.thatdot.api.v2.outputs.OutputFormat
import com.thatdot.quine.CirceCodecTestSupport
import com.thatdot.quine.app.v2api.definitions.outputs.QuineDestinationSteps
import com.thatdot.quine.app.v2api.definitions.query.standing.{
  Predicate,
  StandingQueryResultTransformation,
  StandingQueryResultWorkflow,
}

class StandingQueryOutputCodecSpec
    extends AnyFunSuite
    with Matchers
    with ScalaCheckDrivenPropertyChecks
    with ArbitraryStandingQueryOutputs
    with CirceCodecTestSupport {

  test("OutputFormat.JSON roundtrip") {
    testJsonRoundtrip[OutputFormat](OutputFormat.JSON)
  }

  test("OutputFormat.Protobuf roundtrip") {
    testJsonRoundtrip[OutputFormat](OutputFormat.Protobuf("schema.desc", "MyType"))
  }

  test("OutputFormat property-based roundtrip") {
    forAll { (format: OutputFormat) =>
      val json = format.asJson.deepDropNullValues
      val decoded = json.as[OutputFormat]
      assert(decoded == Right(format))
    }
  }

  test("Predicate.OnlyPositiveMatch roundtrip") {
    testJsonRoundtrip[Predicate](Predicate.OnlyPositiveMatch)
  }

  test("Predicate property-based roundtrip") {
    forAll { (predicate: Predicate) =>
      val json = predicate.asJson.deepDropNullValues
      val decoded = json.as[Predicate]
      assert(decoded == Right(predicate))
    }
  }

  test("StandingQueryResultTransformation.InlineData roundtrip") {
    testJsonRoundtrip[StandingQueryResultTransformation](StandingQueryResultTransformation.InlineData)
  }

  test("StandingQueryResultTransformation property-based roundtrip") {
    forAll { (transformation: StandingQueryResultTransformation) =>
      val json = transformation.asJson.deepDropNullValues
      val decoded = json.as[StandingQueryResultTransformation]
      assert(decoded == Right(transformation))
    }
  }

  test("QuineDestinationSteps.Drop roundtrip") {
    testJsonRoundtrip[QuineDestinationSteps](QuineDestinationSteps.Drop)
  }

  test("QuineDestinationSteps.StandardOut roundtrip") {
    testJsonRoundtrip[QuineDestinationSteps](QuineDestinationSteps.StandardOut)
  }

  test("QuineDestinationSteps.File roundtrip") {
    testJsonRoundtrip[QuineDestinationSteps](QuineDestinationSteps.File("/tmp/output.json"))
  }

  test("QuineDestinationSteps.HttpEndpoint roundtrip") {
    testJsonRoundtrip[QuineDestinationSteps](
      QuineDestinationSteps.HttpEndpoint(url = "http://localhost:8080/data", parallelism = 8),
    )
  }

  test("QuineDestinationSteps.Kafka roundtrip") {
    testJsonRoundtrip[QuineDestinationSteps](
      QuineDestinationSteps.Kafka(
        topic = "topic",
        bootstrapServers = "localhost:9092",
        format = OutputFormat.JSON,
        kafkaProperties = Map("key" -> KafkaPropertyValue("value")),
      ),
    )
  }

  test("QuineDestinationSteps.Kinesis roundtrip") {
    testJsonRoundtrip[QuineDestinationSteps](
      QuineDestinationSteps.Kinesis(
        credentials = None,
        region = None,
        streamName = "my-stream",
        format = OutputFormat.JSON,
        kinesisParallelism = Some(4),
        kinesisMaxBatchSize = Some(100),
        kinesisMaxRecordsPerSecond = Some(1000),
        kinesisMaxBytesPerSecond = Some(100000),
      ),
    )
  }

  test("QuineDestinationSteps.ReactiveStream roundtrip") {
    testJsonRoundtrip[QuineDestinationSteps](
      QuineDestinationSteps.ReactiveStream(address = "localhost", port = 8080, format = OutputFormat.JSON),
    )
  }

  test("QuineDestinationSteps.SNS roundtrip") {
    testJsonRoundtrip[QuineDestinationSteps](
      QuineDestinationSteps.SNS(
        credentials = None,
        region = None,
        topic = "arn:aws:sns:us-east-1:123456789:my-topic",
        format = OutputFormat.JSON,
      ),
    )
  }

  test("QuineDestinationSteps.CypherQuery roundtrip") {
    testJsonRoundtrip[QuineDestinationSteps](
      QuineDestinationSteps.CypherQuery(
        query = "MATCH (n) RETURN n",
        parallelism = 8,
      ),
    )
  }

  test("QuineDestinationSteps.Slack roundtrip") {
    testJsonRoundtrip[QuineDestinationSteps](
      QuineDestinationSteps.Slack(
        hookUrl = "https://hooks.slack.com/services/T00/B00/XXX",
      ),
    )
  }

  test("QuineDestinationSteps property-based roundtrip") {
    forAll { (dest: QuineDestinationSteps) =>
      val json = dest.asJson.deepDropNullValues
      val decoded = json.as[QuineDestinationSteps]
      assert(decoded == Right(dest), s"Failed for: $dest\nJSON: ${json.spaces2}")
    }
  }

  test("Checking for ugly QuineDestinationSteps encodings") {
    forAll { (dest: QuineDestinationSteps) =>
      val json = dest.asJson.deepDropNullValues
      val decoded = json.as[QuineDestinationSteps]
      decoded.foreach(d => assert(d == dest))
      assert(decoded.isRight)

      // kafkaProperties is allowed to be empty (it's a Map)
      val allowedEmpty: Vector[String] => Boolean = {
        case path if path.lastOption.contains("kafkaProperties") => true
        case _ => false
      }
      val ugly = checkForUglyJson(json, allowedEmpty)
      assert(ugly.isRight, ugly)
    }
  }

  test("StandingQueryResultWorkflow minimal roundtrip") {
    import cats.data.NonEmptyList
    testJsonRoundtrip[StandingQueryResultWorkflow](
      StandingQueryResultWorkflow(
        name = "test-workflow",
        filter = None,
        preEnrichmentTransformation = None,
        resultEnrichment = None,
        destinations = NonEmptyList.one(QuineDestinationSteps.StandardOut),
      ),
    )
  }

  test("StandingQueryResultWorkflow full roundtrip") {
    import cats.data.NonEmptyList
    testJsonRoundtrip[StandingQueryResultWorkflow](
      StandingQueryResultWorkflow(
        name = "full-workflow",
        filter = Some(Predicate.OnlyPositiveMatch),
        preEnrichmentTransformation = Some(StandingQueryResultTransformation.InlineData),
        resultEnrichment = Some(
          QuineDestinationSteps.CypherQuery(
            query = "MATCH (n) RETURN n",
            parallelism = 4,
          ),
        ),
        destinations = NonEmptyList.of(
          QuineDestinationSteps.StandardOut,
          QuineDestinationSteps.File("/tmp/output.json"),
        ),
      ),
    )
  }

  test("StandingQueryResultWorkflow property-based roundtrip") {
    forAll { (workflow: StandingQueryResultWorkflow) =>
      val json = workflow.asJson.deepDropNullValues
      val decoded = json.as[StandingQueryResultWorkflow]
      assert(decoded == Right(workflow), s"Failed for: $workflow\nJSON: ${json.spaces2}")
    }
  }

  test("Checking for ugly StandingQueryResultWorkflow encodings") {
    forAll { (workflow: StandingQueryResultWorkflow) =>
      val json = workflow.asJson.deepDropNullValues
      val decoded = json.as[StandingQueryResultWorkflow]
      decoded.foreach(w => assert(w == workflow))
      assert(decoded.isRight)

      val allowedEmpty: Vector[String] => Boolean = {
        case path if path.lastOption.contains("kafkaProperties") => true
        case _ => false
      }
      val ugly = checkForUglyJson(json, allowedEmpty)
      assert(ugly.isRight, ugly)
    }
  }

  test("CypherQuery decodes from minimal JSON with defaults applied") {
    forAll { cypherQuery: QuineDestinationSteps.CypherQuery =>
      // Drop fields that have defaults to simulate minimal client payloads
      val minimalJson = cypherQuery.asJson.deepDropNullValues.asObject.get
        .remove("parameter")
        .remove("parallelism")
        .remove("allowAllNodeScan")
        .remove("shouldRetry")
        .toJson
      val expectedMinimalDecoded = QuineDestinationSteps.CypherQuery(query = cypherQuery.query)

      val decoded = minimalJson
        .as[QuineDestinationSteps.CypherQuery]
        .getOrElse(fail(s"Failed to decode `minimalJson` of ${minimalJson.noSpaces}"))
      decoded shouldEqual expectedMinimalDecoded
    }
  }

  test("StandingQueryResultWorkflow decodes from minimal JSON with defaults applied") {
    forAll { workflow: StandingQueryResultWorkflow =>
      // Drop fields with defaults to simulate minimal client payloads
      val minimalJson = workflow.asJson.deepDropNullValues.asObject.get
        .remove("filter")
        .remove("preEnrichmentTransformation")
        .remove("resultEnrichment")
        .toJson
      val expectedMinimalDecoded = StandingQueryResultWorkflow(
        name = workflow.name,
        destinations = workflow.destinations,
      )

      val decoded = minimalJson
        .as[StandingQueryResultWorkflow]
        .getOrElse(fail(s"Failed to decode `minimalJson` of ${minimalJson.noSpaces}"))
      decoded shouldEqual expectedMinimalDecoded
    }
  }
}
