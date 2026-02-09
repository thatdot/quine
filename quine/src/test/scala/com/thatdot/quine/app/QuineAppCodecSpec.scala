package com.thatdot.quine.app

import java.util.UUID

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

  private val sqDataMapCodec = QuineApp.sqOutputs2PersistenceCodec

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
  }

  "sqOutputs2PersistenceCodec" should {
    "roundtrip V2StandingQueryDataMap including credentials (property-based)" in {
      forAll { (data: V2StandingQueryDataMap) =>
        val json = data.asJson(sqDataMapCodec.encoder)
        val decoded = json.as[V2StandingQueryDataMap](sqDataMapCodec.decoder)

        decoded shouldBe Right(data)
      }
    }

    "preserve StandingQueryId UUIDs exactly" in {
      forAll { (data: V2StandingQueryDataMap) =>
        val json = data.asJson(sqDataMapCodec.encoder)
        val decoded = json.as[V2StandingQueryDataMap](sqDataMapCodec.decoder)

        for {
          decodedData <- decoded
          (name, (originalId, _)) <- data
          (decodedId, _) <- decodedData.get(name)
        } decodedId shouldBe originalId
      }
    }

    "encode empty map correctly" in {
      val empty: V2StandingQueryDataMap = Map.empty
      val json = empty.asJson(sqDataMapCodec.encoder)
      json.asObject.map(_.isEmpty) shouldBe Some(true)

      val decoded = json.as[V2StandingQueryDataMap](sqDataMapCodec.decoder)
      decoded shouldBe Right(empty)
    }

    "encode StandingQueryId as correct UUID string in JSON" in {
      forAll { (data: V2StandingQueryDataMap) =>
        whenever(data.nonEmpty) {
          val json = data.asJson(sqDataMapCodec.encoder)
          val originalIds = data.values.map { case (sqId, _) => sqId.uuid.toString }.toSet
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
  }
}
