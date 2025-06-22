package com.thatdot.quine.app.model.ingest2.sources

import scala.jdk.CollectionConverters.MapHasAsScala

import org.apache.pekko.stream.connectors.sqs.scaladsl.{SqsAckFlow, SqsSource => PekkoSqsSource}
import org.apache.pekko.stream.connectors.sqs.{MessageAction, SqsSourceSettings}
import org.apache.pekko.stream.scaladsl.{Flow, Source}
import org.apache.pekko.{Done, NotUsed}

import cats.data.ValidatedNel
import cats.implicits.catsSyntaxValidatedId
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.{Message, MessageAttributeValue}

import com.thatdot.data.{DataFoldableFrom, DataFolderTo}
import com.thatdot.quine.app.ShutdownSwitch
import com.thatdot.quine.app.model.ingest.serialization.ContentDecoder
import com.thatdot.quine.app.model.ingest.util.AwsOps
import com.thatdot.quine.app.model.ingest.util.AwsOps.AwsBuilderOps
import com.thatdot.quine.app.model.ingest2.source.FramedSource
import com.thatdot.quine.app.routes.IngestMeter
import com.thatdot.quine.routes.{AwsCredentials, AwsRegion}
import com.thatdot.quine.util.BaseError

case class SqsSource(
  queueURL: String,
  readParallelism: Int,
  credentialsOpt: Option[AwsCredentials],
  regionOpt: Option[AwsRegion],
  deleteReadMessages: Boolean,
  meter: IngestMeter,
  decoders: Seq[ContentDecoder] = Seq(),
) extends FramedSourceProvider {
  // Available settings: see https://pekko.apache.org/docs/pekko-connectors/current/sqs.html
  implicit val client: SqsAsyncClient = SqsAsyncClient
    .builder()
    .credentials(credentialsOpt)
    .region(regionOpt)
    .httpClient(
      NettyNioAsyncHttpClient.builder.maxConcurrency(AwsOps.httpConcurrencyPerClient).build(),
    )
    .build()

  val src: Source[Message, ShutdownSwitch] = withKillSwitches(
    PekkoSqsSource(
      queueURL,
      SqsSourceSettings()
        .withParallelRequests(readParallelism),
    ).via(metered[Message](meter, m => m.body().length)),
  )

  private def foldAttr[B](mav: MessageAttributeValue, folder: DataFolderTo[B]): B = {
    val builder = folder.mapBuilder()

    builder.add("dataType", folder.string(mav.dataType()))

    Option(mav.stringValue()).foreach(s => builder.add("stringValue", folder.string(s)))

    Option(mav.binaryValue()).foreach { binaryValue =>
      builder.add("binaryValue", folder.bytes(binaryValue.asByteArray()))
    }

    if (!mav.stringListValues().isEmpty) {
      val vecBuilder = folder.vectorBuilder()
      mav.stringListValues().forEach(s => vecBuilder.add(folder.string(s)))
      builder.add("stringListValues", vecBuilder.finish())
    }

    if (!mav.binaryListValues().isEmpty) {
      val vecBuilder = folder.vectorBuilder()
      mav.binaryListValues().forEach(bb => vecBuilder.add(folder.bytes(bb.asByteArray())))
      builder.add("binaryListValues", vecBuilder.finish())
    }

    builder.finish()
  }

  private val messageFolder: DataFoldableFrom[Message] =
    new DataFoldableFrom[Message] {

      def fold[B](value: Message, folder: DataFolderTo[B]): B = {
        val builder = folder.mapBuilder()

        builder.add("messageId", folder.string(value.messageId()))
        builder.add("receiptHandle", folder.string(value.receiptHandle()))
        builder.add("md5OfBody", folder.string(value.md5OfBody()))
        builder.add("body", folder.string(value.body()))
        builder.add("md5OfMessageAttributes", folder.string(value.md5OfMessageAttributes()))

        val attrsBuilder = folder.mapBuilder()
        value.attributes().asScala.foreach { case (k, v) =>
          attrsBuilder.add(k.name(), folder.string(v))
        }
        builder.add("attributes", attrsBuilder.finish())

        val msgAttrsBuilder = folder.mapBuilder()
        value.messageAttributes().asScala.foreach { case (name, mav) =>
          msgAttrsBuilder.add(name, foldAttr(mav, folder))
        }
        builder.add("messageAttributes", msgAttrsBuilder.finish())

        builder.finish()
      }
    }

  def framedSource: ValidatedNel[BaseError, FramedSource] = {

    def ack: Flow[Message, Done, NotUsed] = if (deleteReadMessages)
      Flow[Message].map(MessageAction.delete).via(SqsAckFlow.apply(queueURL)).map {
        //TODO MAP Result result: SqsAckResult => result.getResult.
        _ => Done
      }
    else Flow.fromFunction(_ => Done)

    def onTermination(): Unit = client.close()
    FramedSource[Message](
      src,
      meter,
      message => ContentDecoder.decode(decoders, message.body().getBytes()),
      messageFolder,
      ack,
      () => onTermination(),
    ).valid
  }

}
