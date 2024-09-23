package com.thatdot.quine.ingest2

import java.nio.charset.Charset

import io.circe.Decoder.Result
import io.circe.generic.auto._
import io.circe.syntax.EncoderOps
import io.circe.{Decoder, Encoder, Json}
import org.scalatest.Assertion
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import com.thatdot.quine.app.v2api.endpoints.V2IngestEntities.{
  CsvIngestFormat,
  IngestConfiguration,
  ProtobufIngestFormat,
}
import com.thatdot.quine.app.v2api.endpoints.{V2IngestEntities, V2IngestSchemas}
import com.thatdot.quine.routes.FileIngestMode.Regular
import com.thatdot.quine.routes.KafkaAutoOffsetReset.Latest
import com.thatdot.quine.routes.KafkaOffsetCommitting.ExplicitCommit
import com.thatdot.quine.routes.KafkaSecurityProtocol.Sasl_Plaintext
import com.thatdot.quine.routes.KinesisIngest.IteratorType.AfterSequenceNumber
import com.thatdot.quine.routes.RecordDecodingType.{Base64, Zlib}
import com.thatdot.quine.routes.WebsocketSimpleStartupIngest.SendMessageInterval
import com.thatdot.quine.routes._
class IngestCodecTest
    extends AnyFunSuite
    with ScalaCheckDrivenPropertyChecks
    with ArbitraryIngests
    with V2IngestSchemas {

  def testJsonEncodeDecode[V: Encoder: Decoder](v: V): Assertion = {
    val json = v.asJson
    val i2: Result[V] = json.as[V]
    assert(i2 == Right(v))
  }

  def testConfigurationConvertv1v2[A <: IngestStreamConfiguration](v1: A): Unit = {
    val asV2 = V2IngestEntities.fromV1Ingest(v1)
    val j: Json = asV2.asJson
    val r: Result[V2IngestEntities.IngestConfiguration] = j.as[V2IngestEntities.IngestConfiguration]
    //Config rehydrated from json
    r.foreach(config => assert(config == asV2))
  }

  test("num ingest v1 -> v2 -> v1") {
    forAll { originalV1Ingest: NumberIteratorIngest => testConfigurationConvertv1v2(originalV1Ingest) }
  }

  test("file ingest v1 -> v2 -> v1") {
    forAll { originalV1Ingest: FileIngest => testConfigurationConvertv1v2(originalV1Ingest) }
  }

  test("S3 ingest v1 -> v2 -> v1") {
    forAll { originalV1Ingest: S3Ingest => testConfigurationConvertv1v2(originalV1Ingest) }
  }

  test("Stdin ingest v1 -> v2 -> v1") {
    forAll { originalV1Ingest: StandardInputIngest => testConfigurationConvertv1v2(originalV1Ingest) }
  }

  test("sse ingest v1 -> v2 -> v1") {
    forAll { originalV1Ingest: ServerSentEventsIngest => testConfigurationConvertv1v2(originalV1Ingest) }
  }

  test("sqs ingest v1 -> v2 -> v1") {
    forAll { originalV1Ingest: SQSIngest => testConfigurationConvertv1v2(originalV1Ingest) }
  }

  test("kinesis ingest v1 -> v2 -> v1") {
    forAll { originalV1Ingest: KinesisIngest => testConfigurationConvertv1v2(originalV1Ingest) }
  }

  test("kafka ingest v1 -> v2 -> v1") {
    forAll { originalV1Ingest: KafkaIngest => testConfigurationConvertv1v2(originalV1Ingest) }
  }

  test("num ingest json encode/decode") {
    testJsonEncodeDecode(V2IngestEntities.NumberIteratorIngest(2, Some(3)))
  }

  test("csv format json encode/decode") {
    testJsonEncodeDecode(CsvIngestFormat(Left(true)))
    testJsonEncodeDecode(CsvIngestFormat(Right(List("A", "B"))))
  }

  test("file json encode/decode") {
    testJsonEncodeDecode(
      V2IngestEntities.FileIngest("/a", Some(Regular), Some(10), 10, Some(20), Charset.forName("UTF-16"), Seq(Zlib)),
    )
  }

  test("s3 json encode/decode") {
    testJsonEncodeDecode(
      V2IngestEntities.S3Ingest(
        "bucket",
        "key",
        Some(AwsCredentials("A", "B")),
        Some(10),
        10,
        Some(20),
        Charset.forName("UTF-16"),
        Seq(Zlib),
      ),
    )
  }

  test("stdin json encode/decode") {
    testJsonEncodeDecode(V2IngestEntities.StdInputIngest(Some(10), Charset.forName("UTF-16")))
  }

  test("websocket json encode/decode") {
    testJsonEncodeDecode(
      V2IngestEntities.WebsocketIngest(
        "url",
        Seq("A", "B", "C"),
        SendMessageInterval("message", 5001),
        Charset.forName("UTF-16"),
      ),
    )
  }

  test("kinesis json encode/decode") {

    testJsonEncodeDecode(
      V2IngestEntities.KinesisIngest(
        "streamName",
        Some(Set("A", "B", "C")),
        Some(AwsCredentials("A", "B")),
        Some(AwsRegion.apply("us-east-1")),
        AfterSequenceNumber("sequenceNumber"),
        2,
        Seq(Base64, Zlib),
      ),
    )
  }

  test("sse json encode/decode") {
    testJsonEncodeDecode(V2IngestEntities.ServerSentEventIngest("url", Seq(Base64, Zlib)))
  }

  test("sqs json encode/decode") {
    testJsonEncodeDecode(
      V2IngestEntities.SQSIngest(
        "queueUrl",
        12,
        Some(AwsCredentials("A", "B")),
        Some(AwsRegion.apply("us-east-1")),
        true,
        Seq(Base64, Zlib),
      ),
    )
  }
  test("kafka json encode/decode") {

    val topics = Left(Set("topic1", "topic2"))
    val offsetCommitting = Some(
      ExplicitCommit(
        1001,
        10001,
        101,
        false,
      ),
    )
    V2IngestEntities.KafkaIngest(
      topics,
      "bootstrapServers",
      Some("groupId"),
      Sasl_Plaintext,
      offsetCommitting,
      Latest,
      Map("A" -> "B", "C" -> "D"),
      Some(2L),
      Seq(Base64, Zlib),
    )

  }

  test("file ingest") {
    val topics = Right(Map("A" -> Set(1, 2), "B" -> Set(3, 4)))
    val offsetCommitting = Some(
      ExplicitCommit(
        1001,
        10001,
        101,
        false,
      ),
    )
    val kafka = V2IngestEntities.KafkaIngest(
      topics,
      "bootstrapServers",
      Some("groupId"),
      Sasl_Plaintext,
      offsetCommitting,
      Latest,
      Map("A" -> "B", "C" -> "D"),
      Some(2L),
      Seq(Base64, Zlib),
    )
    testJsonEncodeDecode(IngestConfiguration(kafka, format = ProtobufIngestFormat("url", "typename")))

  }

  test("V2 Ingest configuration encode/decode") {
    forAll { ic: IngestConfiguration =>
      val j: Json = ic.asJson
      val r: Result[V2IngestEntities.IngestConfiguration] = j.as[V2IngestEntities.IngestConfiguration]
      //Config rehydrated from json
      r.foreach(config => assert(config == ic))
    }
  }
}
