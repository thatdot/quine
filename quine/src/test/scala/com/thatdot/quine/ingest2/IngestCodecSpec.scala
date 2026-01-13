package com.thatdot.quine.ingest2

import java.nio.charset.Charset

import io.circe.Decoder.Result
import io.circe.Json
import io.circe.syntax.EncoderOps
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import com.thatdot.api.v2.{AwsCredentials, AwsRegion}
import com.thatdot.quine.CirceCodecTestSupport
import com.thatdot.quine.app.v2api.definitions.ingest2.ApiIngest.IngestSource.Kinesis.IteratorType
import com.thatdot.quine.app.v2api.definitions.ingest2.ApiIngest.RecordDecodingType._
import com.thatdot.quine.app.v2api.definitions.ingest2.ApiIngest.{
  BillingMode,
  ClientVersionConfig,
  FileIngestMode,
  IngestFormat,
  IngestSource,
  KCLConfiguration,
  KafkaAutoOffsetReset,
  KafkaOffsetCommitting,
  KafkaSecurityProtocol,
  KinesisCheckpointSettings,
  KinesisSchedulerSourceSettings,
  MetricsDimension,
  MetricsLevel,
  OnRecordErrorHandler,
  Oss,
  RecordDecodingType,
  RecordRetrySettings,
  WebSocketClient,
}
import com.thatdot.quine.app.v2api.definitions.ingest2.{ApiIngest, DeadLetterQueueSettings, OutputFormat}
import com.thatdot.quine.app.v2api.endpoints.V2IngestApiSchemas

class IngestCodecSpec
    extends AnyFunSuite
    with Matchers
    with ScalaCheckDrivenPropertyChecks
    with V2IngestApiSchemas
    with ArbitraryIngests
    with CirceCodecTestSupport {

  // Note: These tests use the parent sealed trait type (IngestSource) because
  // explicit Circe codecs exist at the sealed trait level, not for individual subtypes.
  // The roundtrip still verifies subtype preservation via the type discriminator.

  test("num ingest json encode/decode") {
    testJsonRoundtrip[IngestSource](IngestSource.NumberIterator(2, Some(3)))
  }

  // CSV format test removed - IngestFormat lacks explicit codecs and is only used
  // as a field within IngestSource, which is already tested via the roundtrip tests.

  test("file json encode/decode") {
    testJsonRoundtrip[IngestSource](
      ApiIngest.IngestSource.File(
        format = IngestFormat.FileFormat.JsonL,
        path = "/a",
        fileIngestMode = Some(FileIngestMode.Regular),
        maximumLineSize = Some(10),
        startOffset = 10,
        limit = Some(20),
        characterEncoding = Charset.forName("UTF-16"),
        recordDecoders = Seq(Zlib),
      ),
    )
  }

  test("s3 json encode/decode") {
    testJsonRoundtrip[IngestSource](
      ApiIngest.IngestSource.S3(
        format = IngestFormat.FileFormat.JsonL,
        bucket = "bucket",
        key = "key",
        credentials = Some(AwsCredentials("A", "B")),
        maximumLineSize = Some(10),
        startOffset = 10,
        limit = Some(20),
        characterEncoding = Charset.forName("UTF-16"),
        recordDecoders = Seq(Zlib),
      ),
    )
  }

  test("stdin json encode/decode") {
    testJsonRoundtrip[IngestSource](
      ApiIngest.IngestSource.StdInput(
        format = IngestFormat.FileFormat.JsonL,
        maximumLineSize = Some(10),
        characterEncoding = Charset.forName("UTF-16"),
      ),
    )
  }

  test("websocket json encode/decode") {
    testJsonRoundtrip[IngestSource](
      ApiIngest.IngestSource.WebsocketClient(
        format = IngestFormat.StreamingFormat.Json,
        url = "url",
        initMessages = Seq("A", "B", "C"),
        keepAlive = WebSocketClient.SendMessageInterval("message", 5001),
        characterEncoding = Charset.forName("UTF-16"),
      ),
    )
  }

  test("kinesis json encode/decode") {
    testJsonRoundtrip[IngestSource](
      ApiIngest.IngestSource.Kinesis(
        format = IngestFormat.StreamingFormat.Json,
        streamName = "streamName",
        shardIds = Some(Set("A", "B", "C")),
        credentials = Some(AwsCredentials("A", "B")),
        region = Some(AwsRegion.apply("us-east-1")),
        iteratorType = IteratorType.AfterSequenceNumber("sequenceNumber"),
        numRetries = 2,
        recordDecoders = Seq(Base64, Zlib),
      ),
    )
  }

  test("sse json encode/decode") {
    testJsonRoundtrip[IngestSource](
      ApiIngest.IngestSource
        .ServerSentEvent(format = IngestFormat.StreamingFormat.Json, url = "url", recordDecoders = Seq(Base64, Zlib)),
    )
  }

  test("sqs json encode/decode") {
    testJsonRoundtrip[IngestSource](
      ApiIngest.IngestSource.SQS(
        format = IngestFormat.StreamingFormat.Json,
        queueUrl = "queueUrl",
        readParallelism = 12,
        credentials = Some(AwsCredentials("A", "B")),
        region = Some(AwsRegion.apply("us-east-1")),
        recordDecoders = Seq(Base64, Zlib),
      ),
    )
  }
  test("kafka json encode/decode") {

    val topics = Left(Set("topic1", "topic2"))
    val offsetCommitting = Some(
      KafkaOffsetCommitting.ExplicitCommit(
        maxBatch = 1001,
        maxIntervalMillis = 10001,
        parallelism = 101,
        waitForCommitConfirmation = false,
      ),
    )
    ApiIngest.IngestSource.Kafka(
      format = IngestFormat.StreamingFormat.Json,
      topics = topics,
      bootstrapServers = "bootstrapServers",
      groupId = Some("groupId"),
      securityProtocol = KafkaSecurityProtocol.Sasl_Plaintext,
      offsetCommitting = offsetCommitting,
      autoOffsetReset = KafkaAutoOffsetReset.Latest,
      kafkaProperties = Map("A" -> "B", "C" -> "D"),
      endingOffset = Some(2L),
      recordDecoders = Seq(Base64, Zlib),
    )

  }

  test("file ingest") {
    val topics = Right(Map("A" -> Set(1, 2), "B" -> Set(3, 4)))
    val offsetCommitting = Some(
      KafkaOffsetCommitting.ExplicitCommit(
        maxBatch = 1001,
        maxIntervalMillis = 10001,
        parallelism = 101,
        waitForCommitConfirmation = false,
      ),
    )
    val kafka = ApiIngest.IngestSource.Kafka(
      format = IngestFormat.StreamingFormat.Protobuf("url", "typename"),
      topics = topics,
      bootstrapServers = "bootstrapServers",
      groupId = Some("groupId"),
      securityProtocol = KafkaSecurityProtocol.Sasl_Plaintext,
      offsetCommitting = offsetCommitting,
      autoOffsetReset = KafkaAutoOffsetReset.Latest,
      kafkaProperties = Map("A" -> "B", "C" -> "D"),
      endingOffset = Some(2L),
      recordDecoders = Seq(Base64, Zlib),
    )
    testJsonRoundtrip(Oss.QuineIngestConfiguration("kafka-in", kafka, "CREATE $(that)"))

  }

  test("V2 Ingest configuration encode/decode") {
    forAll { ic: Oss.QuineIngestConfiguration =>
      val j: Json = ic.asJson.deepDropNullValues
      val r: Result[Oss.QuineIngestConfiguration] = j.as[Oss.QuineIngestConfiguration]
      //Config rehydrated from json
      r.foreach(config => assert(config == ic))
    }
  }

  test("Checking for ugly IngestSource encodings") {
    forAll { ic: IngestSource =>
      val j: Json = ic.asJson.deepDropNullValues
      val r: Result[IngestSource] = j.as[IngestSource]
      r.foreach(config => assert(config == ic))
      assert(r.isRight)
      val allowedEmpty: Vector[String] => Boolean = {
        case Vector("topics") => true
        case Vector("kafkaProperties") => true
        case _ => false
      }
      val ugly = checkForUglyJson(j, allowedEmpty)
      assert(ugly.isRight, ugly)
    }
  }

  test("OnRecordErrorHandler decodes from minimal JSON with defaults applied") {
    forAll { handler: ApiIngest.OnRecordErrorHandler =>
      // Drop fields that have defaults to simulate minimal client payloads
      val minimalJson = handler.asJson.deepDropNullValues.asObject.get
        .remove("retrySettings")
        .remove("logRecord")
        .remove("deadLetterQueueSettings")
        .toJson
      val expectedMinimalDecoded = OnRecordErrorHandler()

      val decoded = minimalJson
        .as[ApiIngest.OnRecordErrorHandler]
        .getOrElse(fail(s"Failed to decode `minimalJson` of ${minimalJson.noSpaces}"))
      decoded shouldEqual expectedMinimalDecoded
    }
  }

  test("RecordRetrySettings decodes from minimal JSON with defaults applied") {
    forAll { settings: RecordRetrySettings =>
      // Drop all fields with defaults to simulate minimal client payloads
      val minimalJson = settings.asJson.deepDropNullValues.asObject.get
        .remove("minBackoff")
        .remove("maxBackoff")
        .remove("randomFactor")
        .remove("maxRetries")
        .toJson
      val expectedMinimalDecoded = RecordRetrySettings()

      val decoded = minimalJson
        .as[RecordRetrySettings]
        .getOrElse(fail(s"Failed to decode `minimalJson` of ${minimalJson.noSpaces}"))
      decoded shouldEqual expectedMinimalDecoded
    }
  }

  test("KinesisCheckpointSettings decodes from minimal JSON with defaults applied") {
    forAll { settings: KinesisCheckpointSettings =>
      // Drop fields with defaults to simulate minimal client payloads
      val minimalJson = settings.asJson.deepDropNullValues.asObject.get
        .remove("disableCheckpointing")
        .remove("maxBatchSize")
        .remove("maxBatchWaitMillis")
        .toJson
      val expectedMinimalDecoded = KinesisCheckpointSettings()

      val decoded = minimalJson
        .as[KinesisCheckpointSettings]
        .getOrElse(fail(s"Failed to decode `minimalJson` of ${minimalJson.noSpaces}"))
      decoded shouldEqual expectedMinimalDecoded
    }
  }

  test("KinesisSchedulerSourceSettings decodes from minimal JSON with defaults applied") {
    forAll { settings: KinesisSchedulerSourceSettings =>
      // Drop fields with defaults to simulate minimal client payloads
      val minimalJson = settings.asJson.deepDropNullValues.asObject.get
        .remove("bufferSize")
        .remove("backpressureTimeoutMillis")
        .toJson
      val expectedMinimalDecoded = KinesisSchedulerSourceSettings()

      val decoded = minimalJson
        .as[KinesisSchedulerSourceSettings]
        .getOrElse(fail(s"Failed to decode `minimalJson` of ${minimalJson.noSpaces}"))
      decoded shouldEqual expectedMinimalDecoded
    }
  }

  test("KCLConfiguration decodes from minimal JSON with defaults applied") {
    forAll { config: KCLConfiguration =>
      // Drop all Option fields with defaults to simulate minimal client payloads
      val minimalJson = config.asJson.deepDropNullValues.asObject.get
        .remove("configsBuilder")
        .remove("leaseManagementConfig")
        .remove("retrievalSpecificConfig")
        .remove("processorConfig")
        .remove("coordinatorConfig")
        .remove("lifecycleConfig")
        .remove("retrievalConfig")
        .remove("metricsConfig")
        .toJson
      val expectedMinimalDecoded = KCLConfiguration()

      val decoded =
        minimalJson.as[KCLConfiguration].getOrElse(fail(s"Failed to decode `minimalJson` of ${minimalJson.noSpaces}"))
      decoded shouldEqual expectedMinimalDecoded
    }
  }

  test("DeadLetterQueueSettings decodes from minimal JSON with defaults applied") {
    forAll { settings: DeadLetterQueueSettings =>
      // Drop fields with defaults to simulate minimal client payloads
      val minimalJson = settings.asJson.deepDropNullValues.asObject.get
        .remove("destinations")
        .toJson
      val expectedMinimalDecoded = DeadLetterQueueSettings()

      val decoded = minimalJson
        .as[DeadLetterQueueSettings]
        .getOrElse(fail(s"Failed to decode `minimalJson` of ${minimalJson.noSpaces}"))
      decoded shouldEqual expectedMinimalDecoded
    }
  }

  test("OutputFormat.JSON decodes from minimal JSON with defaults applied") {
    forAll { json: OutputFormat.JSON =>
      // Drop fields with defaults to simulate minimal client payloads
      val minimalJson = json.asJson.deepDropNullValues.asObject.get
        .remove("withInfoEnvelope")
        .toJson
      val expectedMinimalDecoded = OutputFormat.JSON()

      val decoded =
        minimalJson.as[OutputFormat.JSON].getOrElse(fail(s"Failed to decode `minimalJson` of ${minimalJson.noSpaces}"))
      decoded shouldEqual expectedMinimalDecoded
    }
  }

  test("RecordDecodingType encodes with type discriminator") {
    (RecordDecodingType.Zlib: RecordDecodingType).asJson shouldEqual Json.obj("type" -> Json.fromString("Zlib"))
    (RecordDecodingType.Gzip: RecordDecodingType).asJson shouldEqual Json.obj("type" -> Json.fromString("Gzip"))
    (RecordDecodingType.Base64: RecordDecodingType).asJson shouldEqual Json.obj("type" -> Json.fromString("Base64"))
  }

  test("FileIngestMode encodes with type discriminator") {
    (FileIngestMode.Regular: FileIngestMode).asJson shouldEqual Json.obj("type" -> Json.fromString("Regular"))
    (FileIngestMode.NamedPipe: FileIngestMode).asJson shouldEqual Json.obj("type" -> Json.fromString("NamedPipe"))
  }

  test("KafkaAutoOffsetReset encodes with type discriminator") {
    (KafkaAutoOffsetReset.Latest: KafkaAutoOffsetReset).asJson shouldEqual Json.obj("type" -> Json.fromString("Latest"))
    (KafkaAutoOffsetReset.Earliest: KafkaAutoOffsetReset).asJson shouldEqual
    Json.obj("type" -> Json.fromString("Earliest"))
    (KafkaAutoOffsetReset.None: KafkaAutoOffsetReset).asJson shouldEqual Json.obj("type" -> Json.fromString("None"))
  }

  test("BillingMode encodes with type discriminator") {
    (BillingMode.PROVISIONED: BillingMode).asJson shouldEqual Json.obj("type" -> Json.fromString("PROVISIONED"))
    (BillingMode.PAY_PER_REQUEST: BillingMode).asJson shouldEqual Json.obj("type" -> Json.fromString("PAY_PER_REQUEST"))
    (BillingMode.UNKNOWN_TO_SDK_VERSION: BillingMode).asJson shouldEqual
    Json.obj("type" -> Json.fromString("UNKNOWN_TO_SDK_VERSION"))
  }

  test("MetricsLevel encodes with type discriminator") {
    (MetricsLevel.NONE: MetricsLevel).asJson shouldEqual Json.obj("type" -> Json.fromString("NONE"))
    (MetricsLevel.SUMMARY: MetricsLevel).asJson shouldEqual Json.obj("type" -> Json.fromString("SUMMARY"))
    (MetricsLevel.DETAILED: MetricsLevel).asJson shouldEqual Json.obj("type" -> Json.fromString("DETAILED"))
  }

  test("MetricsDimension encodes with type discriminator") {
    (MetricsDimension.OPERATION_DIMENSION_NAME: MetricsDimension).asJson shouldEqual
    Json.obj("type" -> Json.fromString("OPERATION_DIMENSION_NAME"))
    (MetricsDimension.SHARD_ID_DIMENSION_NAME: MetricsDimension).asJson shouldEqual
    Json.obj("type" -> Json.fromString("SHARD_ID_DIMENSION_NAME"))
    (MetricsDimension.STREAM_IDENTIFIER: MetricsDimension).asJson shouldEqual
    Json.obj("type" -> Json.fromString("STREAM_IDENTIFIER"))
    (MetricsDimension.WORKER_IDENTIFIER: MetricsDimension).asJson shouldEqual
    Json.obj("type" -> Json.fromString("WORKER_IDENTIFIER"))
  }

  test("ClientVersionConfig encodes with type discriminator") {
    (ClientVersionConfig.CLIENT_VERSION_CONFIG_COMPATIBLE_WITH_2X: ClientVersionConfig).asJson shouldEqual
    Json.obj("type" -> Json.fromString("CLIENT_VERSION_CONFIG_COMPATIBLE_WITH_2X"))
    (ClientVersionConfig.CLIENT_VERSION_CONFIG_3X: ClientVersionConfig).asJson shouldEqual
    Json.obj("type" -> Json.fromString("CLIENT_VERSION_CONFIG_3X"))
  }

}
