package com.thatdot.quine.ingest2

import java.nio.charset.Charset

import scala.jdk.CollectionConverters._

import org.scalacheck.{Arbitrary, Gen}

import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.quine.app.v2api.definitions.ApiIngest.FileFormat.LineFormat
import com.thatdot.quine.app.v2api.definitions.ApiIngest.FileIngestMode.{NamedPipe, Regular}
import com.thatdot.quine.app.v2api.definitions.ApiIngest.KafkaOffsetCommitting.ExplicitCommit
import com.thatdot.quine.app.v2api.definitions.ApiIngest.WebsocketSimpleStartupIngest.KeepaliveProtocol
import com.thatdot.quine.app.v2api.definitions.ApiIngest._
import com.thatdot.quine.ingest2.IngestSourceTestSupport.randomString
trait ArbitraryIngests {

  implicit val genCharset: Gen[Charset] =
    Gen.oneOf[String](Charset.availableCharsets().keySet().asScala).map(Charset.forName)

  implicit val keepAliveProtocolGen: Gen[KeepaliveProtocol] =
    Gen.oneOf[KeepaliveProtocol](
      Gen.posNum[Int].map(WebsocketSimpleStartupIngest.PingPongInterval(_)),
      Gen.zip(Gen.asciiPrintableStr, Gen.posNum[Int]) map { case (message, intervalMillis) =>
        WebsocketSimpleStartupIngest.SendMessageInterval(message, intervalMillis)
      },
      Gen.const(WebsocketSimpleStartupIngest.NoKeepalive),
    )

  implicit val decoderSeqGen: Gen[Seq[RecordDecodingType]] =
    Gen.someOf(RecordDecodingType.Zlib, RecordDecodingType.Gzip, RecordDecodingType.Base64)

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

  implicit val optionSet: Gen[Option[Set[String]]] = Gen.option(Gen.containerOfN[Set, String](3, Gen.asciiStr))
  implicit val optionPosInt: Gen[Option[Int]] = Gen.option(Gen.posNum[Int])

  implicit val iterType: Arbitrary[KinesisIngest.IteratorType] = Arbitrary(
    Gen.oneOf(
      Gen.const(KinesisIngest.IteratorType.Latest),
      Gen.const(KinesisIngest.IteratorType.TrimHorizon),
      Gen.numStr.map(KinesisIngest.IteratorType.AtSequenceNumber(_)),
      Gen.numStr.map(KinesisIngest.IteratorType.AfterSequenceNumber(_)),
      Gen.posNum[Long].map(KinesisIngest.IteratorType.AtTimestamp(_)),
    ),
  )

  implicit val genKafkaOffset: Gen[KafkaOffsetCommitting] = Gen.resultOf(ExplicitCommit)
  implicit val arbKafkaOffset: Arbitrary[KafkaOffsetCommitting] = Arbitrary(genKafkaOffset)
  implicit val genSecProtocol: Gen[KafkaSecurityProtocol] = Gen.oneOf(
    KafkaSecurityProtocol.PlainText,
    KafkaSecurityProtocol.Ssl,
    KafkaSecurityProtocol.Sasl_Ssl,
    KafkaSecurityProtocol.Sasl_Plaintext,
  )

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
  implicit val arbKeepAliveProtocol: Arbitrary[WebsocketSimpleStartupIngest.KeepaliveProtocol] = Arbitrary(
    keepAliveProtocolGen,
  )

  implicit val fileGen: Gen[FileIngest] = Gen.resultOf(FileIngest)
  implicit val s3Gen: Gen[S3Ingest] = Gen.resultOf(S3Ingest)
  implicit val stdInGen: Gen[StdInputIngest] = Gen.resultOf(StdInputIngest)
  implicit val numInGen: Gen[NumberIteratorIngest] = Gen.resultOf(NumberIteratorIngest)
  implicit val webSocketGen: Gen[WebsocketIngest] = Gen.resultOf(WebsocketIngest)
  implicit val sseInGen: Gen[ServerSentEventIngest] =
    Gen.resultOf(ServerSentEventIngest)
  implicit val sqsInGen: Gen[SQSIngest] = Gen.resultOf(SQSIngest)
  implicit val kinesisGen: Gen[KinesisIngest] = Gen.resultOf(KinesisIngest(_, _, _, _, _, _, _, _))
  implicit val kafkaGen: Gen[KafkaIngest] = Gen.resultOf(KafkaIngest(_, _, _, _, _, _, _, _, _, _))

  implicit def logConfig: LogConfig = LogConfig.permissive

  implicit val arbStdIn: Arbitrary[StdInputIngest] = Arbitrary(stdInGen)
  implicit val arbNum: Arbitrary[NumberIteratorIngest] = Arbitrary(numInGen)
  implicit val arbWeb: Arbitrary[WebsocketIngest] = Arbitrary(webSocketGen)
  implicit val arbSse: Arbitrary[ServerSentEventIngest] = Arbitrary(sseInGen)
  implicit val arbSQS: Arbitrary[SQSIngest] = Arbitrary(sqsInGen)
  implicit val arbKinesis: Arbitrary[KinesisIngest] = Arbitrary(kinesisGen)
  implicit val arbKafka: Arbitrary[KafkaIngest] = Arbitrary(kafkaGen)

  implicit val v2IngestSourceGen: Gen[IngestSource] =
    Gen.oneOf(fileGen, s3Gen, stdInGen, numInGen, webSocketGen, sseInGen, sqsInGen, kinesisGen, kafkaGen)
  implicit val arbInbestSource: Arbitrary[IngestSource] = Arbitrary(v2IngestSourceGen)
  implicit val v2IngestConfigurationGen: Gen[Oss.QuineIngestConfiguration] = for {
    source <- v2IngestSourceGen
  } yield Oss.QuineIngestConfiguration(source, "CREATE ($that)")

  implicit val arbIngest: Arbitrary[Oss.QuineIngestConfiguration] = Arbitrary(v2IngestConfigurationGen)
  implicit val v1IngestStreamStatusGen: Gen[IngestStreamStatus] = Gen.oneOf(
    Gen.const(IngestStreamStatus.Running),
    Gen.const(IngestStreamStatus.Paused),
    Gen.const(IngestStreamStatus.Restored),
    Gen.const(IngestStreamStatus.Completed),
    Gen.const(IngestStreamStatus.Terminated),
    Gen.const(IngestStreamStatus.Failed),
  )
  implicit val arbV1IngestStreamStatus: Arbitrary[IngestStreamStatus] = Arbitrary(v1IngestStreamStatusGen)
}
