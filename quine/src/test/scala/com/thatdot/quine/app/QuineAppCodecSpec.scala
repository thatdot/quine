package com.thatdot.quine.app

import java.util.UUID

import io.circe.parser.parse
import io.circe.syntax.EncoderOps
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import com.thatdot.common.security.Secret
import com.thatdot.quine.graph.StandingQueryId
import com.thatdot.quine.routes.{AwsCredentials, StandingQueryResultOutputUserDef}

class QuineAppCodecSpec extends AnyWordSpec with Matchers with ScalaCheckDrivenPropertyChecks {

  import QuineAppGenerators.V2StandingQueryDataMap
  import QuineAppGenerators.Arbs.v2StandingQueryDataMap
  import com.thatdot.common.security.Secret.Unsafe._

  "sqOutputs1PersistenceCodec" should {
    "roundtrip V1 StandingQueryResultOutputUserDef with credentials correctly" in {
      val v1Codec = QuineApp.sqOutputs1PersistenceCodec

      val original: StandingQueryResultOutputUserDef = StandingQueryResultOutputUserDef.WriteToKinesis(
        credentials =
          Some(AwsCredentials(Secret("AKIAIOSFODNN7EXAMPLE"), Secret("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"))),
        region = None,
        streamName = "test-stream",
        kinesisParallelism = None,
        kinesisMaxBatchSize = None,
        kinesisMaxRecordsPerSecond = None,
        kinesisMaxBytesPerSecond = None,
      )

      val json = original.asJson(v1Codec.encoder)
      val decoded = json.as[StandingQueryResultOutputUserDef](v1Codec.decoder)

      decoded shouldBe Right(original)
    }
  }

  "sqOutputs1MapPersistenceCodec" should {
    "roundtrip V1StandingQueryDataMap with credentials correctly" in {
      val v1MapCodec = QuineApp.sqOutputs1MapPersistenceCodec

      val original: QuineApp.V1StandingQueryDataMap = Map(
        "test-query" -> (
          StandingQueryId(UUID.randomUUID()),
          Map(
            "kinesis-output" -> StandingQueryResultOutputUserDef.WriteToKinesis(
              credentials = Some(
                AwsCredentials(Secret("AKIAIOSFODNN7EXAMPLE"), Secret("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")),
              ),
              region = None,
              streamName = "test-stream",
              kinesisParallelism = None,
              kinesisMaxBatchSize = None,
              kinesisMaxRecordsPerSecond = None,
              kinesisMaxBytesPerSecond = None,
            ),
          ),
        ),
      )

      val json = original.asJson(v1MapCodec.encoder)
      val decoded = json.as[QuineApp.V1StandingQueryDataMap](v1MapCodec.decoder)

      decoded shouldBe Right(original)
    }

    "encode V1StandingQueryDataMap in expected object format" in {
      val v1MapCodec = QuineApp.sqOutputs1MapPersistenceCodec

      val sqId = StandingQueryId(UUID.fromString("3b3121c7-c4ae-4335-b792-a981ec24d4d3"))
      val data: QuineApp.V1StandingQueryDataMap = Map(
        "test-query" -> (
          sqId,
          Map(
            "stdout" -> StandingQueryResultOutputUserDef.PrintToStandardOut(),
          ),
        ),
      )

      val json = data.asJson(v1MapCodec.encoder)

      val queryEntry = json.hcursor.downField("test-query")
      queryEntry.downField("_1").downField("uuid").as[String] shouldBe Right(sqId.uuid.toString)
      queryEntry.downField("_2").downField("stdout").downField("type").as[String] shouldBe Right("PrintToStandardOut")
    }

    "decode V1StandingQueryDataMap from expected object format" in {
      val v1MapCodec = QuineApp.sqOutputs1MapPersistenceCodec

      val sqId = StandingQueryId(UUID.fromString("3b3121c7-c4ae-4335-b792-a981ec24d4d3"))

      val expectedFormatJson = parse(
        """{
          |  "test-query": {
          |    "_1": { "uuid": "3b3121c7-c4ae-4335-b792-a981ec24d4d3" },
          |    "_2": {
          |      "stdout": {
          |        "type": "PrintToStandardOut",
          |        "logLevel": "Info",
          |        "logMode": "Complete",
          |        "structure": { "type": "WithMetadata" }
          |      }
          |    }
          |  }
          |}""".stripMargin,
      ).getOrElse(fail("Failed to parse test JSON"))

      val data = expectedFormatJson
        .as[QuineApp.V1StandingQueryDataMap](v1MapCodec.decoder)
        .getOrElse(fail("Failed to decode V1StandingQueryDataMap"))
      val (decodedSqId, outputs) = data("test-query")
      decodedSqId shouldBe sqId
      outputs.keys should contain("stdout")
    }
  }

  "sqOutputs2PersistenceCodec" should {
    "roundtrip V2StandingQueryDataMap including credentials (property-based)" in {
      val v2MapCodec = QuineApp.sqOutputs2PersistenceCodec

      forAll { (data: V2StandingQueryDataMap) =>
        val json = data.asJson(v2MapCodec.encoder)
        val decoded = json.as[V2StandingQueryDataMap](v2MapCodec.decoder)

        decoded shouldBe Right(data)
      }
    }

    "preserve StandingQueryId UUIDs exactly" in {
      val v2MapCodec = QuineApp.sqOutputs2PersistenceCodec

      forAll { (data: V2StandingQueryDataMap) =>
        val json = data.asJson(v2MapCodec.encoder)
        val decoded = json.as[V2StandingQueryDataMap](v2MapCodec.decoder)

        for {
          decodedData <- decoded
          (name, (originalId, _)) <- data
          (decodedId, _) <- decodedData.get(name)
        } decodedId shouldBe originalId
      }
    }

    "encode empty map correctly" in {
      val v2MapCodec = QuineApp.sqOutputs2PersistenceCodec

      val empty: V2StandingQueryDataMap = Map.empty
      val json = empty.asJson(v2MapCodec.encoder)
      json.asObject.map(_.isEmpty) shouldBe Some(true)

      val decoded = json.as[V2StandingQueryDataMap](v2MapCodec.decoder)
      decoded shouldBe Right(empty)
    }

    "encode StandingQueryId as correct UUID string in JSON" in {
      val v2MapCodec = QuineApp.sqOutputs2PersistenceCodec

      forAll { (data: V2StandingQueryDataMap) =>
        whenever(data.nonEmpty) {
          val json = data.asJson(v2MapCodec.encoder)
          val originalIds = data.values.map { case (sqId, _) => sqId.uuid.toString }.toSet
          // V2 format: tuples are arrays, StandingQueryId is plain UUID string
          val jsonIds = for {
            obj <- json.asObject.toVector
            (_, entry) <- obj.toVector
            arr <- entry.asArray
            sqIdJson <- arr.headOption
            uuidStr <- sqIdJson.asString
          } yield uuidStr

          jsonIds should contain theSameElementsAs originalIds
        }
      }
    }

    "decode V2StandingQueryDataMap from expected array format" in {
      val v2MapCodec = QuineApp.sqOutputs2PersistenceCodec

      val sqId = StandingQueryId(UUID.fromString("3b3121c7-c4ae-4335-b792-a981ec24d4d3"))

      val expectedFormatJson = parse(
        """{
          |  "test-query": [
          |    "3b3121c7-c4ae-4335-b792-a981ec24d4d3",
          |    {
          |      "stdout": {
          |        "name": "stdout",
          |        "destinations": [
          |          { "type": "StandardOut" }
          |        ]
          |      }
          |    }
          |  ]
          |}""".stripMargin,
      ).getOrElse(fail("Failed to parse test JSON"))

      val data = expectedFormatJson
        .as[V2StandingQueryDataMap](v2MapCodec.decoder)
        .getOrElse(fail("Failed to decode V2StandingQueryDataMap"))
      val (decodedSqId, outputs) = data("test-query")
      decodedSqId shouldBe sqId
      outputs.keys should contain("stdout")
    }
  }
}
