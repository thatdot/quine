package com.thatdot.quine.v2api

import io.circe.syntax._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import com.thatdot.api.v2.ResourceName
import com.thatdot.quine.app.v2api.definitions.ingest2.ApiIngest._

class V2IngestEndpointCodecSpec extends AnyFunSpec with Matchers with ScalaCheckDrivenPropertyChecks {

  import V2IngestEndpointGenerators.Arbs._

  describe("IngestStreamInfoWithName memberIdx codec") {
    import java.time.Instant

    import scala.concurrent.duration.DurationInt

    import com.thatdot.api.v2.RatesSummary

    def infoWithMember(settings: Oss.QuineIngestConfiguration, memberIdx: Option[Int]): IngestStreamInfoWithName =
      IngestStreamInfoWithName(
        name = ResourceName.unsafeFromString("test-ingest"),
        status = IngestStreamStatus.Running,
        message = None,
        settings = settings,
        stats = IngestStreamStats(
          ingestedCount = 0L,
          rates = RatesSummary(0L, 0, 0, 0, 0),
          byteRates = RatesSummary(0L, 0, 0, 0, 0),
          startTime = Instant.EPOCH,
          totalRuntime = 0.seconds,
        ),
        memberIdx = memberIdx,
      )

    it("should roundtrip the optional memberIdx") {
      forAll { (settings: Oss.QuineIngestConfiguration, memberIdx: Option[Int]) =>
        val decoded = infoWithMember(settings, memberIdx).asJson
          .as[IngestStreamInfoWithName]
          .getOrElse(fail("Failed to decode IngestStreamInfoWithName"))
        decoded.memberIdx shouldBe memberIdx
      }
    }

    it("should default memberIdx to None when the field is absent (backward compatible)") {
      forAll { (settings: Oss.QuineIngestConfiguration) =>
        // A payload produced before memberIdx existed simply omits the field.
        val withoutMemberIdx = infoWithMember(settings, Some(3)).asJson.mapObject(_.remove("memberIdx"))
        val decoded =
          withoutMemberIdx.as[IngestStreamInfoWithName].getOrElse(fail("Failed to decode IngestStreamInfoWithName"))
        decoded.memberIdx shouldBe None
      }
    }
  }

  describe("OnRecordErrorHandler codec") {
    it("should roundtrip encode/decode") {
      forAll { (handler: OnRecordErrorHandler) =>
        val json = handler.asJson
        val decoded = json.as[OnRecordErrorHandler]
        decoded shouldBe Right(handler)
      }
    }
  }

  describe("OnStreamErrorHandler codec") {
    it("should roundtrip encode/decode") {
      forAll { (handler: OnStreamErrorHandler) =>
        val json = handler.asJson
        val decoded = json.as[OnStreamErrorHandler]
        decoded shouldBe Right(handler)
      }
    }

    it("should include type discriminator") {
      forAll { (handler: OnStreamErrorHandler) =>
        val json = handler.asJson
        val expectedType = handler.getClass.getSimpleName.stripSuffix("$")
        json.hcursor.downField("type").as[String] shouldBe Right(expectedType)
      }
    }
  }

  describe("IngestSource codec") {
    it("should roundtrip encode/decode NumberIterator as IngestSource") {
      forAll { (source: IngestSource) =>
        val json = source.asJson
        val decoded = json.as[IngestSource]
        decoded shouldBe Right(source)
      }
    }

    it("should include type discriminator for NumberIterator") {
      forAll { (source: IngestSource.NumberIterator) =>
        val json = source.asInstanceOf[IngestSource].asJson
        json.hcursor.downField("type").as[String] shouldBe Right("NumberIterator")
        json.hcursor.downField("startOffset").as[Long] shouldBe Right(source.startOffset)
        json.hcursor.downField("limit").as[Option[Long]] shouldBe Right(source.limit)
      }
    }
  }

  describe("Transformation codec") {
    it("should roundtrip encode/decode JavaScript transformation") {
      forAll { (transform: Transformation.JavaScript) =>
        val json = transform.asInstanceOf[Transformation].asJson
        val decoded = json.as[Transformation]
        decoded shouldBe Right(transform)
      }
    }
  }

  describe("Oss.QuineIngestConfiguration codec") {
    it("should roundtrip encode/decode") {
      forAll { (config: Oss.QuineIngestConfiguration) =>
        val json = config.asJson
        val decoded = json.as[Oss.QuineIngestConfiguration]
        decoded shouldBe Right(config)
      }
    }

    it("should encode with correct field names") {
      forAll { (config: Oss.QuineIngestConfiguration) =>
        val json = config.asJson
        json.hcursor.downField("name").as[String] shouldBe Right(config.name.value)
        json.hcursor.downField("query").as[String] shouldBe Right(config.query)
        json.hcursor.downField("parameter").as[String] shouldBe Right(config.parameter)
        json.hcursor.downField("parallelism").as[Int] shouldBe Right(config.parallelism)
        json.hcursor.downField("maxPerSecond").as[Option[Int]] shouldBe Right(config.maxPerSecond)
      }
    }

    it("should preserve all nested structures through roundtrip") {
      forAll { (config: Oss.QuineIngestConfiguration) =>
        val json = config.asJson
        val decoded = json.as[Oss.QuineIngestConfiguration].getOrElse(fail("Decode failed"))

        decoded.name shouldBe config.name
        decoded.source shouldBe config.source
        decoded.query shouldBe config.query
        decoded.parameter shouldBe config.parameter
        decoded.transformation shouldBe config.transformation
        decoded.parallelism shouldBe config.parallelism
        decoded.maxPerSecond shouldBe config.maxPerSecond
        decoded.onRecordError shouldBe config.onRecordError
        decoded.onStreamError shouldBe config.onStreamError
      }
    }

    it("should preserve DLQ Kafka secrets with preservingEncoder") {
      import com.thatdot.common.security.Secret
      import com.thatdot.api.v2.PlainLogin
      import com.thatdot.quine.app.v2api.definitions.ingest2.{
        DeadLetterQueueOutput,
        DeadLetterQueueSettings,
        OutputFormat,
      }

      val config = Oss.QuineIngestConfiguration(
        name = ResourceName.unsafeFromString("test-api-dlq-config"),
        source = IngestSource.NumberIterator(limit = None),
        query = "CREATE ($that)",
        onRecordError = OnRecordErrorHandler(
          deadLetterQueueSettings = DeadLetterQueueSettings(
            destinations = List(
              DeadLetterQueueOutput.Kafka(
                topic = "dlq-topic",
                bootstrapServers = "localhost:9092",
                sslKeystorePassword = Some(Secret("keystore-secret")),
                sslTruststorePassword = Some(Secret("truststore-secret")),
                sslKeyPassword = Some(Secret("key-secret")),
                saslJaasConfig = Some(PlainLogin("user", Secret("password"))),
                outputFormat = OutputFormat.JSON(),
              ),
            ),
          ),
        ),
      )

      import Secret.Unsafe._
      val configPreservingEncoder = Oss.QuineIngestConfiguration.preservingEncoder
      val json = configPreservingEncoder(config)
      val dlqKafka = json.hcursor
        .downField("onRecordError")
        .downField("deadLetterQueueSettings")
        .downField("destinations")
        .downArray

      dlqKafka.downField("sslKeystorePassword").as[String] shouldBe Right("keystore-secret")
      dlqKafka.downField("sslTruststorePassword").as[String] shouldBe Right("truststore-secret")
      dlqKafka.downField("sslKeyPassword").as[String] shouldBe Right("key-secret")
      dlqKafka.downField("saslJaasConfig").downField("password").as[String] shouldBe Right("password")
    }
  }
}
