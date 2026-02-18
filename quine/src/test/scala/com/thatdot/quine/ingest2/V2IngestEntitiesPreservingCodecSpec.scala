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

  describe("KafkaIngest preserving encoder") {
    import Secret.Unsafe._
    import com.thatdot.api.v2.{PlainLogin, ScramLogin, OAuthBearerLogin}
    val ingestSourcePreservingEncoder = IngestSource.preservingEncoder

    it("should preserve sslKeystorePassword for storage") {
      val kafka = KafkaIngest(
        format = StreamingFormat.JsonFormat,
        topics = Left(Set("test-topic")),
        bootstrapServers = "localhost:9092",
        groupId = Some("test-group"),
        offsetCommitting = None,
        endingOffset = None,
        sslKeystorePassword = Some(Secret("keystore-secret-123")),
      )

      val json = ingestSourcePreservingEncoder(kafka)
      val jsonString = json.noSpaces

      json.hcursor.downField("sslKeystorePassword").as[String] shouldBe Right("keystore-secret-123")
      jsonString should not include "Secret(****)"
    }

    it("should preserve sslTruststorePassword for storage") {
      val kafka = KafkaIngest(
        format = StreamingFormat.JsonFormat,
        topics = Left(Set("test-topic")),
        bootstrapServers = "localhost:9092",
        groupId = Some("test-group"),
        offsetCommitting = None,
        endingOffset = None,
        sslTruststorePassword = Some(Secret("truststore-secret-456")),
      )

      val json = ingestSourcePreservingEncoder(kafka)
      val jsonString = json.noSpaces

      json.hcursor.downField("sslTruststorePassword").as[String] shouldBe Right("truststore-secret-456")
      jsonString should not include "Secret(****)"
    }

    it("should preserve sslKeyPassword for storage") {
      val kafka = KafkaIngest(
        format = StreamingFormat.JsonFormat,
        topics = Left(Set("test-topic")),
        bootstrapServers = "localhost:9092",
        groupId = Some("test-group"),
        offsetCommitting = None,
        endingOffset = None,
        sslKeyPassword = Some(Secret("key-secret-789")),
      )

      val json = ingestSourcePreservingEncoder(kafka)
      val jsonString = json.noSpaces

      json.hcursor.downField("sslKeyPassword").as[String] shouldBe Right("key-secret-789")
      jsonString should not include "Secret(****)"
    }

    it("should preserve saslJaasConfig PlainLogin for storage") {
      val kafka = KafkaIngest(
        format = StreamingFormat.JsonFormat,
        topics = Left(Set("test-topic")),
        bootstrapServers = "localhost:9092",
        groupId = Some("test-group"),
        offsetCommitting = None,
        endingOffset = None,
        saslJaasConfig = Some(PlainLogin("alice", Secret("plain-password"))),
      )

      val json = ingestSourcePreservingEncoder(kafka)
      val jsonString = json.noSpaces
      val jaasJson = json.hcursor.downField("saslJaasConfig")

      jaasJson.downField("username").as[String] shouldBe Right("alice")
      jaasJson.downField("password").as[String] shouldBe Right("plain-password")
      jsonString should not include "Secret(****)"
    }

    it("should preserve saslJaasConfig ScramLogin for storage") {
      val kafka = KafkaIngest(
        format = StreamingFormat.JsonFormat,
        topics = Left(Set("test-topic")),
        bootstrapServers = "localhost:9092",
        groupId = Some("test-group"),
        offsetCommitting = None,
        endingOffset = None,
        saslJaasConfig = Some(ScramLogin("bob", Secret("scram-password"))),
      )

      val json = ingestSourcePreservingEncoder(kafka)
      val jsonString = json.noSpaces
      val jaasJson = json.hcursor.downField("saslJaasConfig")

      jaasJson.downField("username").as[String] shouldBe Right("bob")
      jaasJson.downField("password").as[String] shouldBe Right("scram-password")
      jsonString should not include "Secret(****)"
    }

    it("should preserve saslJaasConfig OAuthBearerLogin for storage") {
      val kafka = KafkaIngest(
        format = StreamingFormat.JsonFormat,
        topics = Left(Set("test-topic")),
        bootstrapServers = "localhost:9092",
        groupId = Some("test-group"),
        offsetCommitting = None,
        endingOffset = None,
        saslJaasConfig = Some(OAuthBearerLogin("client-id", Secret("client-secret"), Some("scope"), None)),
      )

      val json = ingestSourcePreservingEncoder(kafka)
      val jsonString = json.noSpaces
      val jaasJson = json.hcursor.downField("saslJaasConfig")

      jaasJson.downField("clientId").as[String] shouldBe Right("client-id")
      jaasJson.downField("clientSecret").as[String] shouldBe Right("client-secret")
      jsonString should not include "Secret(****)"
    }

    it("should preserve all Kafka secrets together for storage") {
      val kafka = KafkaIngest(
        format = StreamingFormat.JsonFormat,
        topics = Left(Set("test-topic")),
        bootstrapServers = "localhost:9092",
        groupId = Some("test-group"),
        offsetCommitting = None,
        endingOffset = None,
        sslKeystorePassword = Some(Secret("ks-pass")),
        sslTruststorePassword = Some(Secret("ts-pass")),
        sslKeyPassword = Some(Secret("key-pass")),
        saslJaasConfig = Some(PlainLogin("user", Secret("sasl-pass"))),
      )

      val json = ingestSourcePreservingEncoder(kafka)
      val jsonString = json.noSpaces

      // All secrets should be preserved
      json.hcursor.downField("sslKeystorePassword").as[String] shouldBe Right("ks-pass")
      json.hcursor.downField("sslTruststorePassword").as[String] shouldBe Right("ts-pass")
      json.hcursor.downField("sslKeyPassword").as[String] shouldBe Right("key-pass")
      json.hcursor.downField("saslJaasConfig").downField("password").as[String] shouldBe Right("sasl-pass")
      // None should be redacted
      jsonString should not include "Secret(****)"
    }

    it("should roundtrip with preserving encoder") {
      val kafka = KafkaIngest(
        format = StreamingFormat.JsonFormat,
        topics = Left(Set("test-topic")),
        bootstrapServers = "localhost:9092",
        groupId = Some("test-group"),
        offsetCommitting = None,
        endingOffset = None,
        sslKeystorePassword = Some(Secret("ks-pass")),
        sslTruststorePassword = Some(Secret("ts-pass")),
        sslKeyPassword = Some(Secret("key-pass")),
        saslJaasConfig = Some(PlainLogin("user", Secret("sasl-pass"))),
      )

      val json = ingestSourcePreservingEncoder(kafka)
      val decoded = json.as[IngestSource]

      decoded shouldBe Right(kafka)
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

    it("should preserve DLQ Kafka secrets for storage") {
      import com.thatdot.quine.app.v2api.definitions.ingest2.{
        DeadLetterQueueOutput,
        DeadLetterQueueSettings,
        OutputFormat,
      }
      import com.thatdot.quine.app.v2api.definitions.ingest2.ApiIngest.OnRecordErrorHandler
      import com.thatdot.api.v2.PlainLogin

      val config = QuineIngestConfiguration(
        name = "test-kafka-dlq-config",
        source = NumberIteratorIngest(StreamingFormat.JsonFormat, limit = None),
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

    it("should preserve DLQ Kinesis AWS credentials for storage") {
      import com.thatdot.quine.app.v2api.definitions.ingest2.{
        DeadLetterQueueOutput,
        DeadLetterQueueSettings,
        OutputFormat,
      }
      import com.thatdot.quine.app.v2api.definitions.ingest2.ApiIngest.OnRecordErrorHandler
      import com.thatdot.api.v2.{AwsCredentials, AwsRegion}

      val config = QuineIngestConfiguration(
        name = "test-kinesis-dlq-config",
        source = NumberIteratorIngest(StreamingFormat.JsonFormat, limit = None),
        query = "CREATE ($that)",
        onRecordError = OnRecordErrorHandler(
          deadLetterQueueSettings = DeadLetterQueueSettings(
            destinations = List(
              DeadLetterQueueOutput.Kinesis(
                credentials = Some(AwsCredentials(Secret("AKIAEXAMPLE"), Secret("secretkey123"))),
                region = Some(AwsRegion("us-east-1")),
                streamName = "dlq-stream",
                kinesisParallelism = None,
                kinesisMaxBatchSize = None,
                kinesisMaxRecordsPerSecond = None,
                kinesisMaxBytesPerSecond = None,
                outputFormat = OutputFormat.JSON(),
              ),
            ),
          ),
        ),
      )

      val json = configPreservingEncoder(config)
      val dlqKinesis = json.hcursor
        .downField("onRecordError")
        .downField("deadLetterQueueSettings")
        .downField("destinations")
        .downArray

      val credsJson = dlqKinesis.downField("credentials")
      credsJson.downField("accessKeyId").as[String] shouldBe Right("AKIAEXAMPLE")
      credsJson.downField("secretAccessKey").as[String] shouldBe Right("secretkey123")
    }

    it("should preserve DLQ SNS AWS credentials for storage") {
      import com.thatdot.quine.app.v2api.definitions.ingest2.{
        DeadLetterQueueOutput,
        DeadLetterQueueSettings,
        OutputFormat,
      }
      import com.thatdot.quine.app.v2api.definitions.ingest2.ApiIngest.OnRecordErrorHandler
      import com.thatdot.api.v2.{AwsCredentials, AwsRegion}

      val config = QuineIngestConfiguration(
        name = "test-sns-dlq-config",
        source = NumberIteratorIngest(StreamingFormat.JsonFormat, limit = None),
        query = "CREATE ($that)",
        onRecordError = OnRecordErrorHandler(
          deadLetterQueueSettings = DeadLetterQueueSettings(
            destinations = List(
              DeadLetterQueueOutput.SNS(
                credentials = Some(AwsCredentials(Secret("AKIAEXAMPLE"), Secret("secretkey123"))),
                region = Some(AwsRegion("us-east-1")),
                topic = "arn:aws:sns:us-east-1:123456789012:dlq-topic",
                outputFormat = OutputFormat.JSON(),
              ),
            ),
          ),
        ),
      )

      val json = configPreservingEncoder(config)
      val dlqSns = json.hcursor
        .downField("onRecordError")
        .downField("deadLetterQueueSettings")
        .downField("destinations")
        .downArray

      val credsJson = dlqSns.downField("credentials")
      credsJson.downField("accessKeyId").as[String] shouldBe Right("AKIAEXAMPLE")
      credsJson.downField("secretAccessKey").as[String] shouldBe Right("secretkey123")
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
