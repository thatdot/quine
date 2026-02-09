package com.thatdot.quine.ingest2

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import com.thatdot.common.security.Secret
import com.thatdot.quine.app.model.ingest2.V2IngestEntities._
import com.thatdot.quine.app.model.ingest2._
import com.thatdot.quine.{routes => V1}

/** Preserving encoder tests for V2 Ingest types used in persistence.
  *
  * For default encoder tests (API responses, redaction), see [[V2IngestEntitiesCodecSpec]].
  */
class V2IngestEntitiesPreservingCodecSpec extends AnyFunSpec with Matchers with ScalaCheckDrivenPropertyChecks {

  import V2IngestEntitiesGenerators.Arbs._

  describe("SQSIngest preserving encoder") {
    import Secret.Unsafe._
    val ingestSourcePreservingEncoder = IngestSource.preservingEncoder

    it("should preserve AWS credentials for storage") {
      forAll { (sqs: SQSIngest) =>
        whenever(sqs.credentials.isDefined) {
          val json = ingestSourcePreservingEncoder(sqs)
          val credsJson = json.hcursor.downField("credentials")

          credsJson.downField("accessKeyId").as[String] shouldBe
          Right(sqs.credentials.get.accessKeyId.unsafeValue)
          credsJson.downField("secretAccessKey").as[String] shouldBe
          Right(sqs.credentials.get.secretAccessKey.unsafeValue)
        }
      }
    }

    it("should roundtrip with preserving encoder") {
      forAll { (sqs: SQSIngest) =>
        val json = ingestSourcePreservingEncoder(sqs)
        val decoded = json.as[IngestSource]
        decoded shouldBe Right(sqs)
      }
    }
  }

  describe("KinesisIngest preserving encoder") {
    import Secret.Unsafe._
    val ingestSourcePreservingEncoder = IngestSource.preservingEncoder

    it("should preserve AWS credentials for storage") {
      forAll { (kinesis: KinesisIngest) =>
        whenever(kinesis.credentials.isDefined) {
          val json = ingestSourcePreservingEncoder(kinesis)
          val credsJson = json.hcursor.downField("credentials")

          credsJson.downField("accessKeyId").as[String] shouldBe
          Right(kinesis.credentials.get.accessKeyId.unsafeValue)
          credsJson.downField("secretAccessKey").as[String] shouldBe
          Right(kinesis.credentials.get.secretAccessKey.unsafeValue)
        }
      }
    }

    it("should roundtrip with preserving encoder") {
      forAll { (kinesis: KinesisIngest) =>
        val json = ingestSourcePreservingEncoder(kinesis)
        val decoded = json.as[IngestSource]
        decoded shouldBe Right(kinesis)
      }
    }
  }

  describe("S3Ingest preserving encoder") {
    import Secret.Unsafe._
    val ingestSourcePreservingEncoder = IngestSource.preservingEncoder

    it("should preserve AWS credentials for storage") {
      forAll { (s3: S3Ingest) =>
        whenever(s3.credentials.isDefined) {
          val json = ingestSourcePreservingEncoder(s3)
          val credsJson = json.hcursor.downField("credentials")

          credsJson.downField("accessKeyId").as[String] shouldBe
          Right(s3.credentials.get.accessKeyId.unsafeValue)
          credsJson.downField("secretAccessKey").as[String] shouldBe
          Right(s3.credentials.get.secretAccessKey.unsafeValue)
        }
      }
    }

    it("should roundtrip with preserving encoder") {
      forAll { (s3: S3Ingest) =>
        val json = ingestSourcePreservingEncoder(s3)
        val decoded = json.as[IngestSource]
        decoded shouldBe Right(s3)
      }
    }
  }

  describe("IngestSource preserving encoder") {
    import Secret.Unsafe._
    val ingestSourcePreservingEncoder = IngestSource.preservingEncoder

    it("should roundtrip with preserving encoder") {
      forAll { (source: IngestSource) =>
        val json = ingestSourcePreservingEncoder(source)
        val decoded = json.as[IngestSource]
        decoded shouldBe Right(source)
      }
    }
  }

  describe("QuineIngestConfiguration preserving encoder") {
    import Secret.Unsafe._
    val configPreservingEncoder = QuineIngestConfiguration.preservingEncoder

    it("should preserve credentials in source for storage") {
      val config = QuineIngestConfiguration(
        name = "test-sqs-config",
        source = SQSIngest(
          format = StreamingFormat.JsonFormat,
          queueUrl = "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
          credentials = Some(V1.AwsCredentials(Secret("AKIAEXAMPLE"), Secret("secretkey123"))),
          region = Some(V1.AwsRegion("us-east-1")),
        ),
        query = "CREATE ($that)",
      )

      val json = configPreservingEncoder(config)
      val credsJson = json.hcursor.downField("source").downField("credentials")

      credsJson.downField("accessKeyId").as[String] shouldBe Right("AKIAEXAMPLE")
      credsJson.downField("secretAccessKey").as[String] shouldBe Right("secretkey123")
    }

    it("should roundtrip with preserving encoder") {
      forAll { (config: QuineIngestConfiguration) =>
        val json = configPreservingEncoder(config)
        val decoded = json.as[QuineIngestConfiguration]
        decoded shouldBe Right(config)
      }
    }
  }

  describe("QuineIngestStreamWithStatus preserving encoder") {
    import Secret.Unsafe._
    val streamWithStatusPreservingEncoder = QuineIngestStreamWithStatus.preservingEncoder

    it("should preserve credentials in config source for storage") {
      val config = QuineIngestConfiguration(
        name = "test-sqs-config",
        source = SQSIngest(
          format = StreamingFormat.JsonFormat,
          queueUrl = "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
          credentials = Some(V1.AwsCredentials(Secret("AKIAEXAMPLE"), Secret("secretkey123"))),
          region = Some(V1.AwsRegion("us-east-1")),
        ),
        query = "CREATE ($that)",
      )
      val ingest = QuineIngestStreamWithStatus(config, Some(V1.IngestStreamStatus.Running))

      val json = streamWithStatusPreservingEncoder(ingest)
      val credsJson = json.hcursor.downField("config").downField("source").downField("credentials")

      credsJson.downField("accessKeyId").as[String] shouldBe Right("AKIAEXAMPLE")
      credsJson.downField("secretAccessKey").as[String] shouldBe Right("secretkey123")
    }

    it("should roundtrip with preserving encoder") {
      forAll { (ingest: QuineIngestStreamWithStatus) =>
        val json = streamWithStatusPreservingEncoder(ingest)
        val decoded = json.as[QuineIngestStreamWithStatus]
        decoded shouldBe Right(ingest)
      }
    }
  }
}
