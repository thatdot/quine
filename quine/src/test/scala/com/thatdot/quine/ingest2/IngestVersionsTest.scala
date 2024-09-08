package com.thatdot.quine.ingest2

import java.nio.charset.Charset

import scala.jdk.CollectionConverters.SetHasAsScala
import scala.util.Random

import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import com.thatdot.quine.app.v2api.endpoints.V2IngestEntities
import com.thatdot.quine.graph.ArbitraryInstances
import com.thatdot.quine.ingest2.IngestSourceTestSupport.randomString
import com.thatdot.quine.routes.FileIngestFormat.CypherCsv
import com.thatdot.quine.routes.FileIngestMode.{NamedPipe, Regular}
import com.thatdot.quine.routes.KafkaOffsetCommitting.ExplicitCommit
import com.thatdot.quine.routes.KafkaSecurityProtocol.{PlainText, Ssl}
import com.thatdot.quine.routes.StreamedRecordFormat.{CypherProtobuf, CypherRaw, Drop}
import com.thatdot.quine.routes._
import com.thatdot.quine.{routes => v1}
class IngestVersionsTest extends AnyFunSuite with ScalaCheckDrivenPropertyChecks with ArbitraryInstances {

  implicit val arbV1StreamedFormat: Arbitrary[StreamedRecordFormat] = Arbitrary(
    Gen.oneOf[StreamedRecordFormat](
      Gen.resultOf(StreamedRecordFormat.CypherJson),
      Gen.resultOf(CypherRaw),
      Gen.resultOf(CypherProtobuf),
      Gen.const(Drop),
    ),
  )
  implicit val arbV1FileFormat: Arbitrary[FileIngestFormat] = Arbitrary(
    Gen.oneOf[FileIngestFormat](
      Gen.resultOf(FileIngestFormat.CypherLine),
      Gen.resultOf(FileIngestFormat.CypherJson),
      Gen.const(CypherCsv(randomString(), randomString(), Left(true))),
    ),
  )

  implicit val arbF: Arbitrary[FileIngestMode] = Arbitrary(Gen.oneOf(Regular, NamedPipe))
  implicit val arbAWS: Arbitrary[Option[AwsCredentials]] = Arbitrary(
    Gen.option(Gen.const(AwsCredentials(randomString(), randomString()))),
  )
  implicit val arbReg: Arbitrary[Option[AwsRegion]] = Arbitrary(
    Gen.option(Gen.oneOf("us-west-1", "us-east-1").map(AwsRegion.apply)),
  )
  implicit val arbRec: Arbitrary[Seq[RecordDecodingType]] = Arbitrary(
    Gen.containerOf[Seq, RecordDecodingType](
      Gen.oneOf(RecordDecodingType.Gzip, RecordDecodingType.Base64, RecordDecodingType.Zlib),
    ),
  )
  implicit val optionSet: Gen[Option[Set[String]]] = Gen.option(Gen.containerOf[Set, String](Gen.asciiStr))
  implicit val optionPosInt: Gen[Option[Int]] = Gen.option(Gen.posNum[Int])

  implicit val iterType: Arbitrary[KinesisIngest.IteratorType] = Arbitrary(
    Gen.oneOf(KinesisIngest.IteratorType.Latest, KinesisIngest.IteratorType.TrimHorizon),
  )
  implicit val genOffset: Gen[KafkaOffsetCommitting] = Gen.resultOf(ExplicitCommit)

  implicit def v1KinesisGen: Gen[v1.KinesisIngest] = for {
    format: StreamedRecordFormat <- arbV1StreamedFormat.arbitrary
    shardIds: Option[Set[String]] <- optionSet
    creds: Option[AwsCredentials] <- arbAWS.arbitrary
    region: Option[AwsRegion] <- arbReg.arbitrary
    iter: KinesisIngest.IteratorType <- iterType.arbitrary
    decoders <- arbRec.arbitrary
    maxPerSec <- optionPosInt
  } yield v1.KinesisIngest(
    format,
    randomString(),
    shardIds,
    Random.nextInt(1000),
    creds,
    region,
    iter,
    Random.nextInt(1000),
    maxPerSec,
    decoders,
    checkpointSettings = None,
  )

  implicit def v1KafkaGen: Gen[v1.KafkaIngest] = for {
    format: StreamedRecordFormat <- arbV1StreamedFormat.arbitrary
    topics <- Gen.containerOf[Set, String](Gen.asciiStr)
    groupId <- Gen.option(Gen.asciiStr)
    securityProtocol <- Gen.oneOf(PlainText, Ssl)
    decoders <- arbRec.arbitrary
    offsetCommitting <- Gen.option(genOffset)
    offsetReset: KafkaAutoOffsetReset <- Gen.oneOf(
      KafkaAutoOffsetReset.Latest,
      KafkaAutoOffsetReset.Earliest,
      KafkaAutoOffsetReset.None,
    )
    endingOffset <- optionPosInt.map(i => i.map(_.toLong))
    maxPerSec <- optionPosInt
  } yield v1.KafkaIngest(
    format,
    Left(topics),
    Random.nextInt(1000),
    randomString(),
    groupId,
    securityProtocol,
    offsetCommitting,
    offsetReset,
    Map(randomString() -> randomString()),
    endingOffset,
    maxPerSec,
    decoders,
  )

  def legalEncoding: String = Gen.oneOf[String](Charset.availableCharsets().keySet().asScala).sample.getOrElse("UTF-8")

  implicit val arbFile: Arbitrary[FileIngest] =
    Arbitrary(Gen.resultOf(v1.FileIngest).map(_.copy(encoding = legalEncoding)))

  test("v1 -> v2 -> v1 file ingest should round trip") {
    forAll { originalV1Ingest: FileIngest =>
      assert(V2IngestEntities.fromV1Ingest(originalV1Ingest).asV1IngestStreamConfiguration == originalV1Ingest)
    }
  }

  implicit val arbS3: Arbitrary[S3Ingest] = Arbitrary(Gen.resultOf(v1.S3Ingest))
  test("v1 -> v2 -> v1 S3 ingest should round trip") {
    forAll { originalV1Ingest: FileIngest =>
      assert(V2IngestEntities.fromV1Ingest(originalV1Ingest).asV1IngestStreamConfiguration == originalV1Ingest)
    }
  }

  implicit val arbStdIn: Arbitrary[StandardInputIngest] = Arbitrary(Gen.resultOf(v1.StandardInputIngest))
  test("v1 -> v2 -> v1 Stdin ingest should round trip") {

    forAll { originalV1Ingest: FileIngest =>
      assert(V2IngestEntities.fromV1Ingest(originalV1Ingest).asV1IngestStreamConfiguration == originalV1Ingest)
    }

  }

  implicit val arbNum: Arbitrary[NumberIteratorIngest] = Arbitrary(Gen.resultOf(v1.NumberIteratorIngest))
  test("v1 -> v2 -> v1 num ingest should round trip") {
    forAll { originalV1Ingest: NumberIteratorIngest =>
      assert(V2IngestEntities.fromV1Ingest(originalV1Ingest).asV1IngestStreamConfiguration == originalV1Ingest)
    }
  }

  implicit val arbSse: Arbitrary[ServerSentEventsIngest] = Arbitrary(Gen.resultOf(v1.ServerSentEventsIngest))
  test("v1 -> v2 -> v1 sse ingest should round trip") {
    forAll { originalV1Ingest: ServerSentEventsIngest =>
      assert(V2IngestEntities.fromV1Ingest(originalV1Ingest).asV1IngestStreamConfiguration == originalV1Ingest)
    }
  }

  implicit val arbSQS: Arbitrary[SQSIngest] = Arbitrary(Gen.resultOf(v1.SQSIngest))
  test("v1 -> v2 -> v1 sqs ingest should round trip") {
    forAll { originalV1Ingest: SQSIngest =>
      assert(V2IngestEntities.fromV1Ingest(originalV1Ingest).asV1IngestStreamConfiguration == originalV1Ingest)
    }
  }

  implicit val arbKinesis: Arbitrary[KinesisIngest] = Arbitrary(v1KinesisGen)
  test("v1 -> v2 -> v1 kinesis ingest should round trip") {
    forAll { originalV1Ingest: KinesisIngest =>
      assert(V2IngestEntities.fromV1Ingest(originalV1Ingest).asV1IngestStreamConfiguration == originalV1Ingest)
    }
  }

  implicit val arbKafka: Arbitrary[KafkaIngest] = Arbitrary(v1KafkaGen)
  test("v1 -> v2 -> v1 kafka ingest should round trip") {
    forAll { originalV1Ingest: KafkaIngest =>
      assert(V2IngestEntities.fromV1Ingest(originalV1Ingest).asV1IngestStreamConfiguration == originalV1Ingest)
    }
  }

}
