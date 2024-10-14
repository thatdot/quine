package com.thatdot.quine.ingest2

import java.nio.charset.Charset

import cats.implicits._
import io.circe.Decoder.Result
import io.circe.generic.auto._
import io.circe.syntax.EncoderOps
import io.circe.{Decoder, Encoder, Json}
import org.scalatest.Assertion
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import com.thatdot.quine.app.v2api.endpoints.V2IngestEntities.FileFormat.{CsvFormat, JsonFormat}
import com.thatdot.quine.app.v2api.endpoints.V2IngestEntities.StreamingFormat.ProtobufFormat
import com.thatdot.quine.app.v2api.endpoints.V2IngestEntities.{IngestSource, QuineIngestConfiguration, StreamingFormat}
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
    with V2IngestSchemas
    with ArbitraryIngests {

  def testJsonEncodeDecode[V: Encoder: Decoder](v: V): Assertion = {
    val json = v.asJson
    val i2: Result[V] = json.as[V]
    assert(i2 == Right(v))
  }

  test("num ingest json encode/decode") {
    testJsonEncodeDecode(V2IngestEntities.NumberIteratorIngest(StreamingFormat.JsonFormat, 2, Some(3)))
  }

  test("csv format json encode/decode") {
    testJsonEncodeDecode(CsvFormat(Left(true)))
    testJsonEncodeDecode(CsvFormat(Right(List("A", "B"))))
  }

  test("file json encode/decode") {
    testJsonEncodeDecode(
      V2IngestEntities
        .FileIngest(JsonFormat, "/a", Some(Regular), Some(10), 10, Some(20), Charset.forName("UTF-16"), Seq(Zlib)),
    )
  }

  test("s3 json encode/decode") {
    testJsonEncodeDecode(
      V2IngestEntities.S3Ingest(
        JsonFormat,
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
    testJsonEncodeDecode(V2IngestEntities.StdInputIngest(JsonFormat, Some(10), Charset.forName("UTF-16")))
  }

  test("websocket json encode/decode") {
    testJsonEncodeDecode(
      V2IngestEntities.WebsocketIngest(
        StreamingFormat.JsonFormat,
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
        StreamingFormat.JsonFormat,
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
    testJsonEncodeDecode(V2IngestEntities.ServerSentEventIngest(StreamingFormat.JsonFormat, "url", Seq(Base64, Zlib)))
  }

  test("sqs json encode/decode") {
    testJsonEncodeDecode(
      V2IngestEntities.SQSIngest(
        StreamingFormat.JsonFormat,
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
      StreamingFormat.JsonFormat,
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
      ProtobufFormat("url", "typename"),
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
    testJsonEncodeDecode(QuineIngestConfiguration(kafka, "CREATE $(that)"))

  }

  test("V2 Ingest configuration encode/decode") {
    forAll { ic: QuineIngestConfiguration =>
      val j: Json = ic.asJson.deepDropNullValues
      val r: Result[V2IngestEntities.QuineIngestConfiguration] = j.as[V2IngestEntities.QuineIngestConfiguration]
      //Config rehydrated from json
      r.foreach(config => assert(config == ic))
    }
  }

  /** Checks to see if a json encoding produces any "ugly" values.
    * Any time a "Left" or "Right" appears as a key, we probably have an Either that was encoded wrong.
    * Any class that encodes to an empty object is also probably wrong.
    * @param json The json to recursively check
    * @param allowedToBeEmpty Since we cannot tell from the json alone whether an empty object came from
    *                         a case class or just a map, allowedToBeEmpty indicates that a value is allowed to be empty
    *                         (i.e. it came from a map rather than a case class)
    * @return Left(error) if there is an ugly value in the json otherwise Right(())
    */
  def checkForUglyJson(
    json: Json,
    allowedToBeEmpty: Vector[String] => Boolean,
    path: Vector[String] = Vector.empty,
  ): Either[String, Unit] =
    json.fold[Either[String, Unit]](
      Right(()),
      (_ => Right(())),
      (_ => Right(())),
      (_ => Right(())),
      (
        _.zipWithIndex
          .traverse { case (innerJson, index) =>
            checkForUglyJson(innerJson, allowedToBeEmpty, path.appended(index.toString))
          }
          .map(_ => ()),
      ),
      (obj => {
        val map = obj.toMap
        for {
          _ <- if (map.contains("Left")) Left(s"Json contained a left value at ${path.mkString(".")}") else Right(())
          _ <- if (map.contains("Right")) Left(s"Json contained a right value at ${path.mkString(".")}") else Right(())
          _ <-
            if (map.isEmpty && !allowedToBeEmpty(path)) Left(s"Json object was empty at ${path.mkString(".")}")
            else Right(())
          _ <- map.toList.traverse { case (k, innerJson) =>
            checkForUglyJson(innerJson, allowedToBeEmpty, path.appended(k))
          }
        } yield ()
      }),
    )

  test("Checking for ugly IngestSource encodings") {
    forAll { ic: IngestSource =>
      val j: Json = ic.asJson.deepDropNullValues
      val r: Result[IngestSource] = j.as[IngestSource]
      r.foreach(config => assert(config == ic))
      assert(r.isRight)
      val allowedEmpty: Vector[String] => Boolean = {
        case Vector("KafkaIngest", "topics") => true
        case Vector("KafkaIngest", "kafkaProperties") => true
        case _ => false
      }
      val ugly = checkForUglyJson(j, allowedEmpty)
      assert(ugly.isRight, ugly)
    }
  }

}
