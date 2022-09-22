package com.thatdot.quine.app.ingest

import scala.concurrent.Future
import scala.util.{Success, Try}

import akka.stream.alpakka.sqs.scaladsl.{SqsAckSink, SqsSource}
import akka.stream.alpakka.sqs.{MessageAction, SqsSourceSettings}
import akka.stream.contrib.SwitchMode
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.{Done, NotUsed}

import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.Message

import com.thatdot.quine.app.ingest.serialization.ImportFormat
import com.thatdot.quine.app.ingest.util.AwsOps
import com.thatdot.quine.app.ingest.util.AwsOps.AwsBuilderOps
import com.thatdot.quine.graph.CypherOpsGraph
import com.thatdot.quine.graph.MasterStream.IngestSrcExecToken
import com.thatdot.quine.graph.cypher.Value
import com.thatdot.quine.routes.AwsCredentials

/** The definition of an incoming AWS SQS stream.
  *
  * @param queueURL           the URL of the SQS queue from which to read
  * @param format             the [[ImportFormat]] to use in deserializing and writing records from the queue
  * @param initialSwitchMode  is the ingest stream initially paused or not?
  * @param readParallelism    how many records to pull off the SQS queue at a time
  * @param writeParallelism   how many records to write to the graph at a time
  * @param credentialsOpt     the AWS credentials necessary to access the provided SQS queue
  * @param deleteReadMessages if true, issue an acknowledgement for each successfully-deserialized message,
  *                           causing SQS to delete that message from the queue
  */
final case class SqsStreamSrcDef(
  queueURL: String,
  format: ImportFormat,
  initialSwitchMode: SwitchMode,
  readParallelism: Int,
  writeParallelism: Int,
  credentialsOpt: Option[AwsCredentials],
  deleteReadMessages: Boolean,
  maxPerSecond: Option[Int]
)(implicit graph: CypherOpsGraph)
    extends RawValuesIngestSrcDef(
      format,
      initialSwitchMode,
      writeParallelism,
      maxPerSecond,
      "SQS"
    ) {

  type InputType = Message

  implicit val client: SqsAsyncClient = SqsAsyncClient
    .builder()
    .credentials(credentialsOpt)
    .httpClient(
      NettyNioAsyncHttpClient.builder.maxConcurrency(AwsOps.httpConcurrencyPerClient).build()
    )
    .build()

  graph.system.registerOnTermination(client.close())

  override def ingestToken: IngestSrcExecToken = IngestSrcExecToken(s"$name: $queueURL")

  def source(): Source[Message, NotUsed] =
    SqsSource(queueURL, SqsSourceSettings().withParallelRequests(readParallelism))

  def rawBytes(message: Message): Array[Byte] = message.body.getBytes

  /** For each element, executes the MessageAction specified, and if a Deserialized body is present, returns it.
    *
    * This sends an "ignore" message for messages that fail on deserialization. It's not clear if that's the
    * correct thing to do, but leaving it in for now as it's what the pre-existing code did.
    */
  override val ack: Flow[TryDeserialized, Done, NotUsed] = if (deleteReadMessages) {
    val ackSink: Sink[(Try[Value], Message), Future[Done]] = SqsAckSink(queueURL)
      .contramap[TryDeserialized] {
        case (Success(_), msg) => MessageAction.delete(msg)
        case (_, msg) => MessageAction.ignore(msg)
      }
      .named("sqs-ack-sink")
    Flow[TryDeserialized].alsoTo(ackSink).map(_ => Done.done())
  } else {
    Flow[TryDeserialized].map(_ => Done.done())
  }

}
