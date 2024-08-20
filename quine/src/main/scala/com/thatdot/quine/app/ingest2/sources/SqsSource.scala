package com.thatdot.quine.app.ingest2.sources

import org.apache.pekko.stream.connectors.sqs.scaladsl.{SqsAckFlow, SqsSource => PekkoSqsSource}
import org.apache.pekko.stream.connectors.sqs.{MessageAction, SqsSourceSettings}
import org.apache.pekko.stream.scaladsl.{Flow, Source}
import org.apache.pekko.{Done, NotUsed}

import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.Message

import com.thatdot.quine.app.ShutdownSwitch
import com.thatdot.quine.app.ingest.serialization.ContentDecoder
import com.thatdot.quine.app.ingest.util.AwsOps
import com.thatdot.quine.app.ingest.util.AwsOps.AwsBuilderOps
import com.thatdot.quine.app.ingest2.source.{FramedSource, FramedSourceProvider}
import com.thatdot.quine.app.routes.IngestMeter
import com.thatdot.quine.routes.{AwsCredentials, AwsRegion}

case class SqsSource(
  queueURL: String,
  readParallelism: Int,
  credentialsOpt: Option[AwsCredentials],
  regionOpt: Option[AwsRegion],
  deleteReadMessages: Boolean,
  meter: IngestMeter,
  decoders: Seq[ContentDecoder] = Seq()
) extends FramedSourceProvider[Message] {
  /*
Available settings:

  see https://pekko.apache.org/docs/pekko-connectors/current/sqs.html
  val settings = SqsSourceSettings()
  .withWaitTime(20.seconds)
  .withMaxBufferSize(100)
  .withMaxBatchSize(10)
  .withAttributes(immutable.Seq(SenderId, SentTimestamp))
  .withMessageAttribute(MessageAttributeName.create("bar.*"))
  .withCloseOnEmptyReceive(true)
  .withVisibilityTimeout(10.seconds)
   */
  implicit val client: SqsAsyncClient = SqsAsyncClient
    .builder()
    .credentials(credentialsOpt)
    .region(regionOpt)
    .httpClient(
      NettyNioAsyncHttpClient.builder.maxConcurrency(AwsOps.httpConcurrencyPerClient).build()
    )
    .build()

  //type TryDeserialized = (Try[CypherValue], Message)

  val src: Source[Message, ShutdownSwitch] = withKillSwitches(
    PekkoSqsSource(
      queueURL,
      SqsSourceSettings()
        .withParallelRequests(readParallelism)
    ).via(metered[Message](meter, m => m.body().length))
  )

  def framedSource: FramedSource[Message] = new FramedSource[Message](src, meter) {

    def content(input: Message): Array[Byte] = ContentDecoder.decode(decoders, input.body().getBytes())

    override def ack: Flow[Message, Done, NotUsed] = if (deleteReadMessages)
      Flow[Message].map(MessageAction.delete).via(SqsAckFlow.apply(queueURL)).map {
        //TODO MAP Result result: SqsAckResult => result.getResult.
        _ => Done
      }
    else Flow.fromFunction(_ => Done)

    override def onTermination(): Unit = client.close()

  }

}
