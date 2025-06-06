package com.thatdot.quine.ingest2

import java.nio.charset.Charset

import scala.jdk.CollectionConverters._

import org.scalacheck.{Arbitrary, Gen}

import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.quine.app.v2api.definitions.ingest2.ApiIngest.FileIngestMode.{NamedPipe, Regular}
import com.thatdot.quine.app.v2api.definitions.ingest2.ApiIngest.KafkaOffsetCommitting.ExplicitCommit
import com.thatdot.quine.app.v2api.definitions.ingest2.ApiIngest.WebsocketSimpleStartupIngest.KeepaliveProtocol
import com.thatdot.quine.app.v2api.definitions.ingest2.ApiIngest._
import com.thatdot.quine.app.v2api.definitions.{AwsCredentials, AwsRegion}
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

  implicit val iterType: Arbitrary[IngestSource.Kinesis.IteratorType] = Arbitrary(
    Gen.oneOf(
      Gen.const(IngestSource.Kinesis.IteratorType.Latest),
      Gen.const(IngestSource.Kinesis.IteratorType.TrimHorizon),
      Gen.numStr.map(IngestSource.Kinesis.IteratorType.AtSequenceNumber(_)),
      Gen.numStr.map(IngestSource.Kinesis.IteratorType.AfterSequenceNumber(_)),
      Gen.posNum[Long].map(IngestSource.Kinesis.IteratorType.AtTimestamp(_)),
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

  implicit val fileFormatGen: Gen[IngestFormat.FileFormat] = Gen.oneOf(
    IngestFormat.FileFormat.Json,
    IngestFormat.FileFormat.Line,
    IngestFormat.FileFormat.Csv(),
    IngestFormat.FileFormat.Csv(Left(true)),
    IngestFormat.FileFormat.Csv(Right(List("A", "N", "C"))),
  )
  implicit val arbFileFormat: Arbitrary[IngestFormat.FileFormat] = Arbitrary(fileFormatGen)
  implicit val streamingFormatGen: Gen[IngestFormat.StreamingFormat] = Gen.oneOf(
    IngestFormat.StreamingFormat.Json,
    IngestFormat.StreamingFormat.Raw,
    IngestFormat.StreamingFormat.Drop,
    IngestFormat.StreamingFormat.Protobuf("url", "typeName"),
    IngestFormat.StreamingFormat.Avro("url"),
  )

  implicit val genOnRecordError: Gen[OnRecordErrorHandler] = Gen.oneOf(LogRecordErrorHandler, DeadLetterErrorHandler)
  implicit val arbOnRecordError: Arbitrary[OnRecordErrorHandler] = Arbitrary(genOnRecordError)

  implicit val genOnStreamError: Gen[OnStreamErrorHandler] = Gen.oneOf(LogStreamError, RetryStreamError(1))
  implicit val arbOnStreamError: Arbitrary[OnStreamErrorHandler] = Arbitrary(genOnStreamError)
  //
  implicit val arbStreamingFormat: Arbitrary[IngestFormat.StreamingFormat] = Arbitrary(streamingFormatGen)
  implicit val arbCharset: Arbitrary[Charset] = Arbitrary(genCharset)
  implicit val arbKeepAliveProtocol: Arbitrary[WebsocketSimpleStartupIngest.KeepaliveProtocol] = Arbitrary(
    keepAliveProtocolGen,
  )

  implicit val fileGen: Gen[IngestSource.File] = Gen.resultOf(IngestSource.File)
  implicit val s3Gen: Gen[IngestSource.S3] = Gen.resultOf(IngestSource.S3)
  implicit val stdInGen: Gen[IngestSource.StdInput] = Gen.resultOf(IngestSource.StdInput)
  implicit val numInGen: Gen[IngestSource.NumberIterator] = Gen.resultOf(IngestSource.NumberIterator)
  implicit val webSocketGen: Gen[IngestSource.Websocket] = Gen.resultOf(IngestSource.Websocket)
  implicit val sseInGen: Gen[IngestSource.ServerSentEvent] =
    Gen.resultOf(IngestSource.ServerSentEvent)
  implicit val sqsInGen: Gen[IngestSource.SQS] = Gen.resultOf(IngestSource.SQS)
  implicit val kinesisGen: Gen[IngestSource.Kinesis] = Gen.resultOf(IngestSource.Kinesis(_, _, _, _, _, _, _, _))
  implicit val kafkaGen: Gen[IngestSource.Kafka] = Gen.resultOf(IngestSource.Kafka(_, _, _, _, _, _, _, _, _, _))

  implicit def logConfig: LogConfig = LogConfig.permissive

  implicit val arbStdIn: Arbitrary[IngestSource.StdInput] = Arbitrary(stdInGen)
  implicit val arbNum: Arbitrary[IngestSource.NumberIterator] = Arbitrary(numInGen)
  implicit val arbWeb: Arbitrary[IngestSource.Websocket] = Arbitrary(webSocketGen)
  implicit val arbSse: Arbitrary[IngestSource.ServerSentEvent] = Arbitrary(sseInGen)
  implicit val arbSQS: Arbitrary[IngestSource.SQS] = Arbitrary(sqsInGen)
  implicit val arbKinesis: Arbitrary[IngestSource.Kinesis] = Arbitrary(kinesisGen)
  implicit val arbKafka: Arbitrary[IngestSource.Kafka] = Arbitrary(kafkaGen)

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
