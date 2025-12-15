package com.thatdot.quine.ingest2

import java.nio.charset.Charset

import io.circe.Decoder.Result
import io.circe.Json
import io.circe.generic.extras.auto._
import io.circe.syntax.EncoderOps
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import com.thatdot.api.v2.{AwsCredentials, AwsRegion}
import com.thatdot.quine.CirceCodecTestSupport
import com.thatdot.quine.app.v2api.definitions.ingest2.ApiIngest
import com.thatdot.quine.app.v2api.definitions.ingest2.ApiIngest.IngestSource.Kinesis.IteratorType
import com.thatdot.quine.app.v2api.definitions.ingest2.ApiIngest.RecordDecodingType._
import com.thatdot.quine.app.v2api.definitions.ingest2.ApiIngest.{
  FileIngestMode,
  IngestFormat,
  IngestSource,
  KafkaAutoOffsetReset,
  KafkaOffsetCommitting,
  KafkaSecurityProtocol,
  OnRecordErrorHandler,
  Oss,
  WebSocketClient,
}
import com.thatdot.quine.app.v2api.endpoints.V2IngestApiSchemas

class IngestCodecSpec
    extends AnyFunSuite
    with Matchers
    with ScalaCheckDrivenPropertyChecks
    with V2IngestApiSchemas
    with ArbitraryIngests
    with CirceCodecTestSupport {

  test("num ingest json encode/decode") {
    testJsonRoundtrip(IngestSource.NumberIterator(2, Some(3)))
  }

  test("csv format json encode/decode") {
    testJsonRoundtrip(IngestFormat.FileFormat.CSV(Left(true)))
    testJsonRoundtrip(IngestFormat.FileFormat.CSV(Right(List("A", "B"))))
  }

  test("file json encode/decode") {
    testJsonRoundtrip(
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
    testJsonRoundtrip(
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
    testJsonRoundtrip(
      ApiIngest.IngestSource.StdInput(
        format = IngestFormat.FileFormat.JsonL,
        maximumLineSize = Some(10),
        characterEncoding = Charset.forName("UTF-16"),
      ),
    )
  }

  test("websocket json encode/decode") {
    testJsonRoundtrip(
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

    testJsonRoundtrip(
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
    testJsonRoundtrip(
      ApiIngest.IngestSource
        .ServerSentEvent(format = IngestFormat.StreamingFormat.Json, url = "url", recordDecoders = Seq(Base64, Zlib)),
    )
  }

  test("sqs json encode/decode") {
    testJsonRoundtrip(
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

}
