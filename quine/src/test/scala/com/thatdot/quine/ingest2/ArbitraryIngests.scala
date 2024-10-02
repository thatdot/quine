package com.thatdot.quine.ingest2

import java.nio.charset.Charset

import scala.jdk.CollectionConverters._

import org.scalacheck.{Arbitrary, Gen}

import com.thatdot.quine.app.v2api.endpoints.V2IngestEntities.FileFormat.LineFormat
import com.thatdot.quine.app.v2api.endpoints.V2IngestEntities._
import com.thatdot.quine.ingest2.IngestSourceTestSupport.randomString
import com.thatdot.quine.routes.FileIngestMode.{NamedPipe, Regular}
import com.thatdot.quine.routes.KafkaOffsetCommitting.ExplicitCommit
import com.thatdot.quine.routes.{KafkaAutoOffsetReset, KafkaSecurityProtocol, RecordDecodingType}
import com.thatdot.quine.util.Log.LogConfig
import com.thatdot.quine.{routes => v1}
trait ArbitraryIngests {
  implicit val genCharset: Gen[Charset] =
    Gen.oneOf[String](Charset.availableCharsets().keySet().asScala).map(Charset.forName)

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
  implicit val genKafkaOffset: Gen[v1.KafkaOffsetCommitting] = Gen.resultOf(ExplicitCommit)
  implicit val arbKafkaOffset: Arbitrary[v1.KafkaOffsetCommitting] = Arbitrary(genKafkaOffset)
  implicit val genSecProtocol: Gen[KafkaSecurityProtocol] = Gen.oneOf(KafkaSecurityProtocol.values)
  implicit val arbProcotol: Arbitrary[KafkaSecurityProtocol] = Arbitrary(genSecProtocol)
  implicit val genOffsetReset: Gen[KafkaAutoOffsetReset] = Gen.oneOf(KafkaAutoOffsetReset.values)
  implicit val arbOffset: Arbitrary[KafkaAutoOffsetReset] = Arbitrary(genOffsetReset)

  implicit val fileFormatGen: Gen[FileFormat] = Gen.oneOf(
    FileFormat.JsonFormat,
    LineFormat,
    FileFormat.CsvFormat(),
    FileFormat.CsvFormat(Left(true)),
    FileFormat.CsvFormat(Right(List("A", "N", "C"))),
  )
  implicit val arbFileFormat: Arbitrary[FileFormat] = Arbitrary(fileFormatGen)
  implicit val streamingFormatGen: Gen[StreamingFormat] = Gen.oneOf(
    StreamingFormat.JsonFormat,
    StreamingFormat.RawFormat,
    StreamingFormat.DropFormat,
    StreamingFormat.ProtobufFormat("url", "typeName"),
    StreamingFormat.AvroFormat("url"),
  )

  implicit val genOnRecordError: Gen[OnRecordErrorHandler] = Gen.oneOf(LogRecordErrorHandler, DeadLetterErrorHandler)
  implicit val arbOnRecordError: Arbitrary[OnRecordErrorHandler] = Arbitrary(genOnRecordError)

  implicit val genOnStreamError: Gen[OnStreamErrorHandler] = Gen.oneOf(LogStreamError, RetryStreamError(1))
  implicit val arbOnStreamError: Arbitrary[OnStreamErrorHandler] = Arbitrary(genOnStreamError)
  //
  implicit val arbStreamingFormat: Arbitrary[StreamingFormat] = Arbitrary(streamingFormatGen)
  implicit val arbCharset: Arbitrary[Charset] = Arbitrary(genCharset)
  implicit val fileGen: Gen[FileIngest] = Gen.resultOf(FileIngest)
  implicit val s3Gen: Gen[S3Ingest] = Gen.resultOf(S3Ingest)
  implicit val stdInGen: Gen[StdInputIngest] = Gen.resultOf(StdInputIngest)
  implicit val numInGen: Gen[NumberIteratorIngest] = Gen.resultOf(NumberIteratorIngest)
  implicit val sseInGen: Gen[ServerSentEventIngest] =
    Gen.resultOf(ServerSentEventIngest)
  implicit val sqsInGen: Gen[SQSIngest] = Gen.resultOf(SQSIngest)
  implicit val kinesisGen: Gen[KinesisIngest] = Gen.resultOf(KinesisIngest)
  implicit val kafkaGen: Gen[KafkaIngest] = Gen.resultOf(KafkaIngest)

  implicit def logConfig: LogConfig = LogConfig.testing

  implicit val arbStdIn: Arbitrary[StdInputIngest] = Arbitrary(stdInGen)
  implicit val arbNum: Arbitrary[NumberIteratorIngest] = Arbitrary(numInGen)
  implicit val arbSse: Arbitrary[ServerSentEventIngest] = Arbitrary(sseInGen)
  implicit val arbSQS: Arbitrary[SQSIngest] = Arbitrary(sqsInGen)
  implicit val arbKinesis: Arbitrary[KinesisIngest] = Arbitrary(kinesisGen)
  implicit val arbKafka: Arbitrary[KafkaIngest] = Arbitrary(kafkaGen)

  implicit val v2IngestSourceGen: Gen[IngestSource] =
    Gen.oneOf(fileGen, s3Gen, stdInGen, numInGen, sseInGen, sqsInGen, kinesisGen, kafkaGen)
  implicit val arbInbestSource: Arbitrary[IngestSource] = Arbitrary(v2IngestSourceGen)
  implicit val v2IngestConfigurationGen: Gen[QuineIngestConfiguration] = for {
    source <- v2IngestSourceGen
  } yield QuineIngestConfiguration(source)
  implicit val arbIngest: Arbitrary[QuineIngestConfiguration] = Arbitrary(v2IngestConfigurationGen)
}
