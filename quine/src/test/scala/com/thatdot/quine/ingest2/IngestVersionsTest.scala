package com.thatdot.quine.ingest2

import java.nio.charset.Charset

import scala.jdk.CollectionConverters._
import scala.util.Random

import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import com.thatdot.quine.app.v2api.endpoints.V2IngestEntities
import com.thatdot.quine.app.v2api.endpoints.V2IngestEntities._
import com.thatdot.quine.graph.ArbitraryInstances
import com.thatdot.quine.ingest2.IngestSourceTestSupport.randomString
import com.thatdot.quine.routes.FileIngestFormat.CypherCsv
import com.thatdot.quine.routes.FileIngestMode.{NamedPipe, Regular}
import com.thatdot.quine.routes.KafkaOffsetCommitting.ExplicitCommit
import com.thatdot.quine.routes.KafkaSecurityProtocol.{PlainText, Ssl}
import com.thatdot.quine.routes.RecordDecodingType
import com.thatdot.quine.routes.StreamedRecordFormat.{CypherProtobuf, CypherRaw, Drop}
import com.thatdot.quine.util.Log.LogConfig
import com.thatdot.quine.{routes => v1}
trait ArbitraryIngests {
  def legalEncoding: String = Gen.oneOf[String](Charset.availableCharsets().keySet().asScala).sample.getOrElse("UTF-8")

  implicit val arbV1StreamedFormat: Arbitrary[v1.StreamedRecordFormat] = Arbitrary(
    Gen.oneOf[v1.StreamedRecordFormat](
      Gen.resultOf(v1.StreamedRecordFormat.CypherJson),
      Gen.resultOf(CypherRaw),
      Gen.resultOf(CypherProtobuf),
      Gen.const(Drop),
    ),
  )
  implicit val arbV1FileFormat: Arbitrary[v1.FileIngestFormat] = Arbitrary(
    Gen.oneOf[v1.FileIngestFormat](
      Gen.resultOf(v1.FileIngestFormat.CypherLine),
      Gen.resultOf(v1.FileIngestFormat.CypherJson),
      Gen.const(CypherCsv(randomString(), randomString(), Left(true))),
    ),
  )

  implicit val decoderSeqGen: Gen[Seq[RecordDecodingType]] =
    Gen.someOf(RecordDecodingType.Zlib, RecordDecodingType.Gzip, RecordDecodingType.Base64)

  implicit val arbF: Arbitrary[v1.FileIngestMode] = Arbitrary(Gen.oneOf(Regular, NamedPipe))
  implicit val arbAWS: Arbitrary[Option[v1.AwsCredentials]] = Arbitrary(
    Gen.option(Gen.const(v1.AwsCredentials(randomString(), randomString()))),
  )
  implicit val arbReg: Arbitrary[Option[v1.AwsRegion]] = Arbitrary(
    Gen.option(Gen.oneOf("us-west-1", "us-east-1").map(v1.AwsRegion.apply)),
  )
  implicit val arbRec: Arbitrary[Seq[v1.RecordDecodingType]] = Arbitrary(
    Gen.containerOf[Seq, v1.RecordDecodingType](
      Gen.oneOf(v1.RecordDecodingType.Gzip, v1.RecordDecodingType.Base64, v1.RecordDecodingType.Zlib),
    ),
  )
  implicit val optionSet: Gen[Option[Set[String]]] = Gen.option(Gen.containerOfN[Set, String](3, Gen.asciiStr))
  implicit val optionPosInt: Gen[Option[Int]] = Gen.option(Gen.posNum[Int])

  implicit val iterType: Arbitrary[v1.KinesisIngest.IteratorType] = Arbitrary(
    Gen.oneOf(v1.KinesisIngest.IteratorType.Latest, v1.KinesisIngest.IteratorType.TrimHorizon),
  )
  implicit val genOffset: Gen[v1.KafkaOffsetCommitting] = Gen.resultOf(ExplicitCommit)

  // V1 Ingests:

  implicit val v1FileGen: Gen[v1.FileIngest] = Gen.resultOf(v1.FileIngest).map(_.copy(encoding = legalEncoding))
  implicit val v1S3Gen: Gen[v1.S3Ingest] = Gen.resultOf(v1.S3Ingest).map(_.copy(encoding = legalEncoding))
  implicit val v1StdInGen: Gen[v1.StandardInputIngest] =
    Gen.resultOf(v1.StandardInputIngest).map(_.copy(encoding = legalEncoding))
  implicit val v1NumInGen: Gen[v1.NumberIteratorIngest] = Gen.resultOf(v1.NumberIteratorIngest)
  implicit val v1SseInGen: Gen[v1.ServerSentEventsIngest] =
    Gen.resultOf(v1.ServerSentEventsIngest).map(_.copy(recordDecoders = decoderSeqGen.sample.get))
  implicit val v1SqsInGen: Gen[v1.SQSIngest] = Gen.resultOf(v1.SQSIngest)

  implicit val v1KinesisGen: Gen[v1.KinesisIngest] = for {
    format: v1.StreamedRecordFormat <- arbV1StreamedFormat.arbitrary
    shardIds: Option[Set[String]] <- optionSet
    creds: Option[v1.AwsCredentials] <- arbAWS.arbitrary
    region: Option[v1.AwsRegion] <- arbReg.arbitrary
    iter: v1.KinesisIngest.IteratorType <- iterType.arbitrary
    decoders <- decoderSeqGen
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

  implicit val v1KafkaGen: Gen[v1.KafkaIngest] = for {
    format: v1.StreamedRecordFormat <- arbV1StreamedFormat.arbitrary
    topics <- Gen.containerOfN[Set, String](3, Gen.asciiStr)
    groupId <- Gen.option(Gen.asciiStr)
    securityProtocol <- Gen.oneOf(PlainText, Ssl)
    decoders <- decoderSeqGen
    offsetCommitting <- Gen.option(genOffset)
    offsetReset: v1.KafkaAutoOffsetReset <- Gen.oneOf(
      v1.KafkaAutoOffsetReset.Latest,
      v1.KafkaAutoOffsetReset.Earliest,
      v1.KafkaAutoOffsetReset.None,
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

  implicit def logConfig: LogConfig = LogConfig.testing

  implicit val arbFile: Arbitrary[v1.FileIngest] = Arbitrary(v1FileGen)
  implicit val arbS3: Arbitrary[v1.S3Ingest] = Arbitrary(v1S3Gen)
  implicit val arbStdIn: Arbitrary[v1.StandardInputIngest] = Arbitrary(v1StdInGen)
  implicit val arbNum: Arbitrary[v1.NumberIteratorIngest] = Arbitrary(v1NumInGen)
  implicit val arbSse: Arbitrary[v1.ServerSentEventsIngest] = Arbitrary(v1SseInGen)
  implicit val arbSQS: Arbitrary[v1.SQSIngest] = Arbitrary(v1SqsInGen)
  implicit val arbKinesis: Arbitrary[v1.KinesisIngest] = Arbitrary(v1KinesisGen)
  implicit val arbKafka: Arbitrary[v1.KafkaIngest] = Arbitrary(v1KafkaGen)

  // note the limitation that as configured here this can never generate a v2 ingest
  // that covers options not addressed in v1, since we're generating them all from the
  // v1 versions
  implicit val v2IngestGen: Gen[IngestConfiguration] = Gen
    .oneOf(v1FileGen, v1S3Gen, v1StdInGen, v1NumInGen, v1SseInGen, v1SqsInGen, v1KinesisGen, v1KafkaGen)
    .map(fromV1Ingest)
  implicit val arbV2Ingest: Arbitrary[IngestConfiguration] = Arbitrary(v2IngestGen)

}
class IngestVersionsTest
    extends AnyFunSuite
    with ScalaCheckDrivenPropertyChecks
    with ArbitraryInstances
    with ArbitraryIngests {

  test("v1 -> v2 -> v1 num ingest should round trip") {
    forAll { originalV1Ingest: v1.NumberIteratorIngest =>
      assert(V2IngestEntities.fromV1Ingest(originalV1Ingest).asV1IngestStreamConfiguration == originalV1Ingest)
    }
  }

  test("v1 -> v2 -> v1 file ingest should round trip") {
    forAll { originalV1Ingest: v1.FileIngest =>

      assert(V2IngestEntities.fromV1Ingest(originalV1Ingest).asV1IngestStreamConfiguration == originalV1Ingest)
    }
  }

  test("v1 -> v2 -> v1 S3 ingest should round trip") {
    forAll { originalV1Ingest: v1.S3Ingest =>
      assert(V2IngestEntities.fromV1Ingest(originalV1Ingest).asV1IngestStreamConfiguration == originalV1Ingest)
    }
  }

  test("v1 -> v2 -> v1 Stdin ingest should round trip") {

    forAll { originalV1Ingest: v1.StandardInputIngest =>
      assert(V2IngestEntities.fromV1Ingest(originalV1Ingest).asV1IngestStreamConfiguration == originalV1Ingest)
    }

  }

  test("v1 -> v2 -> v1 sse ingest should round trip") {
    forAll { originalV1Ingest: v1.ServerSentEventsIngest =>
      assert(V2IngestEntities.fromV1Ingest(originalV1Ingest).asV1IngestStreamConfiguration == originalV1Ingest)
    }
  }

  test("v1 -> v2 -> v1 sqs ingest should round trip") {
    forAll { originalV1Ingest: v1.SQSIngest =>
      assert(V2IngestEntities.fromV1Ingest(originalV1Ingest).asV1IngestStreamConfiguration == originalV1Ingest)
    }
  }

  test("v1 -> v2 -> v1 kinesis ingest should round trip") {
    forAll { originalV1Ingest: v1.KinesisIngest =>
      assert(V2IngestEntities.fromV1Ingest(originalV1Ingest).asV1IngestStreamConfiguration == originalV1Ingest)
    }
  }

  test("v1 -> v2 -> v1 kafka ingest should round trip") {
    forAll { originalV1Ingest: v1.KafkaIngest =>
      assert(V2IngestEntities.fromV1Ingest(originalV1Ingest).asV1IngestStreamConfiguration == originalV1Ingest)
    }
  }

}
