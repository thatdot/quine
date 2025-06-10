package com.thatdot.model.v2.outputs.destination

import scala.util.{Failure, Random, Success}

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.connectors.kinesis.KinesisFlowSettings
import org.apache.pekko.stream.connectors.kinesis.scaladsl.KinesisFlow
import org.apache.pekko.stream.scaladsl.Sink

import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry

import com.thatdot.aws.model.{AwsCredentials, AwsRegion}
import com.thatdot.aws.util.AwsOps
import com.thatdot.aws.util.AwsOps.AwsBuilderOps
import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.model.v2.outputs.ResultDestination
import com.thatdot.quine.graph.NamespaceId

final case class Kinesis(
  credentials: Option[AwsCredentials],
  region: Option[AwsRegion],
  streamName: String,
  kinesisParallelism: Option[Int],
  kinesisMaxBatchSize: Option[Int],
  kinesisMaxRecordsPerSecond: Option[Int],
  kinesisMaxBytesPerSecond: Option[Int],
) extends ResultDestination.Bytes.Kinesis {
  override def sink(name: String, inNamespace: NamespaceId)(implicit
    logConfig: LogConfig,
  ): Sink[Array[Byte], NotUsed] = {
    val kinesisAsyncClient: KinesisAsyncClient =
      KinesisAsyncClient
        .builder()
        .credentialsV2(credentials)
        .regionV2(region)
        .httpClient(NettyNioAsyncHttpClient.builder.maxConcurrency(AwsOps.httpConcurrencyPerClient).build())
        .build()

    def closeClient(): Unit = kinesisAsyncClient.close()

    val lifecycleSink = Sink.onComplete {
      case Failure(_) =>
        closeClient()
      case Success(_) =>
        closeClient()
    }

    val settings = {
      var s = KinesisFlowSettings.create()
      s = kinesisParallelism.foldLeft(s)(_ withParallelism _)
      s = kinesisMaxBatchSize.foldLeft(s)(_ withMaxBatchSize _)
      s = kinesisMaxRecordsPerSecond.foldLeft(s)(_ withMaxRecordsPerSecond _)
      s = kinesisMaxBytesPerSecond.foldLeft(s)(_ withMaxBytesPerSecond _)
      s
    }

    KinesisFlow(
      streamName,
      settings,
    )(kinesisAsyncClient)
      .named(sinkName(name))
      .contramap[Array[Byte]] { bytes =>
        val builder = PutRecordsRequestEntry.builder()
        builder.data(SdkBytes.fromByteArray(bytes))
        builder.partitionKey("undefined")
        builder.explicitHashKey(BigInt(128, Random).toString)
        builder.build()
      }
      .to(lifecycleSink)
  }

  private def sinkName(name: String): String = s"result-destination--kinesis--$name"
}
