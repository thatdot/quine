package com.thatdot.quine.app.ingest

import scala.compat.ExecutionContexts
import scala.concurrent.duration.DurationInt

import akka.NotUsed
import akka.stream.KillSwitches
import akka.stream.alpakka.sqs.scaladsl.{SqsAckSink, SqsSource}
import akka.stream.alpakka.sqs.{MessageAction, SqsSourceSettings}
import akka.stream.contrib.{SwitchMode, Valve}
import akka.stream.scaladsl.{Flow, Keep}

import com.typesafe.scalalogging.LazyLogging
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.services.sqs.SqsAsyncClient

import com.thatdot.quine.app.ControlSwitches
import com.thatdot.quine.app.ingest.serialization.ImportFormat
import com.thatdot.quine.app.ingest.util.AwsOps
import com.thatdot.quine.app.ingest.util.AwsOps.AwsBuilderOps
import com.thatdot.quine.app.routes.IngestMeter
import com.thatdot.quine.graph.CypherOpsGraph
import com.thatdot.quine.graph.MasterStream.{IngestSrcExecToken, IngestSrcType}
import com.thatdot.quine.routes.AwsCredentials

case object SQS extends LazyLogging {

  /** The definition of an incoming AWS SQS stream.
    *
    * @param queueURL the URL of the SQS queue from which to read
    * @param format the [[ImportFormat]] to use in deserializing and writing records from the queue
    * @param meter
    * @param initialSwitchMode
    * @param readParallelism how many records to pull off the SQS queue at a time
    * @param writeParallelism how many records to write to the graph at a time
    * @param credentials the AWS credentials necessary to access the provided SQS queue
    * @param deleteReadMessages if true, issue an acknowledgement for each successfuly-deserialized message,
    *                           causing SQS to delete that message from the queue
    */
  final case class SqsStreamDef(
    queueURL: String,
    format: ImportFormat,
    meter: IngestMeter,
    initialSwitchMode: SwitchMode,
    readParallelism: Int,
    writeParallelism: Int,
    credentialsOpt: Option[AwsCredentials],
    deleteReadMessages: Boolean,
    maxPerSecond: Option[Int]
  ) {

    /** For each element, executes the MessageAction specified, and if a Deserialized body is present, returns it.
      */
    def ackMessagesFlow()(implicit
      client: SqsAsyncClient
    ): Flow[(Option[format.Deserialized], MessageAction), format.Deserialized, NotUsed] =
      if (deleteReadMessages)
        Flow[(Option[format.Deserialized], MessageAction)]
          .alsoTo(SqsAckSink(queueURL).contramap(_._2))
          .collect { case (Some(deserialized), _) => deserialized }
      else
        Flow[(Option[format.Deserialized], MessageAction)]
          .collect { case (Some(deserialized), _) => deserialized }

    def stream(implicit graph: CypherOpsGraph): IngestSrcType[ControlSwitches] = {
      implicit val client: SqsAsyncClient = SqsAsyncClient
        .builder()
        .credentials(credentialsOpt)
        .httpClient(
          NettyNioAsyncHttpClient.builder.maxConcurrency(AwsOps.httpConcurrencyPerClient).build()
        )
        .build()

      graph.system.registerOnTermination(client.close())
      val isSingleHost = graph.isSingleHost
      val execToken = IngestSrcExecToken(s"SQS: $queueURL")

      def throttled[A] = maxPerSecond match {
        case None => Flow[A]
        case Some(perSec) => Flow[A].throttle(perSec, 1.second)
      }

      SqsSource(queueURL, SqsSourceSettings().withParallelRequests(readParallelism))
        .viaMat(KillSwitches.single)(Keep.right)
        .viaMat(Valve(initialSwitchMode))(Keep.both)
        .map { message => // Deserialize the Record using the configured format, or log a warning
          val bytes = message.body().getBytes()
          meter.mark(bytes.length)
          format
            .importMessageSafeBytes(bytes, isSingleHost)
            .fold(
              { err =>
                logger.warn(
                  s"""Failed to deserialize SQS message with ID: ${message.messageId()} with
                     |in queue: $queueURL using format: ${format.getClass.getSimpleName} (Receipt:
                     |"${message.receiptHandle()}"). Skipping and ignoring record.""".stripMargin.replace('\n', ' ')
                )
                logger.info(s"""Failed to decode SQS message: $message""", err)
                (None, MessageAction.Ignore(message))
              },
              deserialized => (Some(deserialized), MessageAction.Delete(message))
            )
        }
        .via(throttled)
        .via(ackMessagesFlow())
        .via(graph.ingestThrottleFlow)
        .mapAsyncUnordered(writeParallelism)(format.writeToGraph(graph, _))
        .map(_ => execToken)
        .watchTermination() { case ((a, b), c) => b.map(v => ControlSwitches(a, v, c))(ExecutionContexts.parasitic) }
    }
  }
}
