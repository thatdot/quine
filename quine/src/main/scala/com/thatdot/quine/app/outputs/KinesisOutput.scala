package com.thatdot.quine.app.outputs

import scala.util.Random

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.connectors.kinesis.KinesisFlowSettings
import org.apache.pekko.stream.connectors.kinesis.scaladsl.KinesisFlow
import org.apache.pekko.stream.scaladsl.Flow

import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry

import com.thatdot.quine.app.StandingQueryResultOutput.serialized
import com.thatdot.quine.app.ingest.util.AwsOps
import com.thatdot.quine.app.ingest.util.AwsOps.AwsBuilderOps
import com.thatdot.quine.app.serialization.ProtobufSchemaCache
import com.thatdot.quine.graph.{CypherOpsGraph, MasterStream, NamespaceId, StandingQueryResult}
import com.thatdot.quine.routes.StandingQueryResultOutputUserDef
import com.thatdot.quine.routes.StandingQueryResultOutputUserDef.WriteToKinesis
import com.thatdot.quine.util.Log.{LazySafeLogging, LogConfig}

class KinesisOutput(val config: WriteToKinesis)(implicit
  private val logConfig: LogConfig,
  private val protobufSchemaCache: ProtobufSchemaCache,
) extends OutputRuntime
    with LazySafeLogging {

  def flow(
    name: String,
    inNamespace: NamespaceId,
    output: StandingQueryResultOutputUserDef,
    graph: CypherOpsGraph,
  ): Flow[StandingQueryResult, MasterStream.SqResultsExecToken, NotUsed] = {
    val WriteToKinesis(
      credentialsOpt,
      regionOpt,
      streamName,
      format,
      kinesisParallelism,
      kinesisMaxBatchSize,
      kinesisMaxRecordsPerSecond,
      kinesisMaxBytesPerSecond,
    ) = config
    val token = execToken(name, inNamespace)
    val builder = KinesisAsyncClient
      .builder()
      .credentials(credentialsOpt)
      .region(regionOpt)
      .httpClient(NettyNioAsyncHttpClient.builder.maxConcurrency(AwsOps.httpConcurrencyPerClient).build())
    val kinesisAsyncClient: KinesisAsyncClient =
      builder
        .build()
    graph.system.registerOnTermination(kinesisAsyncClient.close())

    val settings = {
      var s = KinesisFlowSettings.create()
      s = kinesisParallelism.foldLeft(s)(_ withParallelism _)
      s = kinesisMaxBatchSize.foldLeft(s)(_ withMaxBatchSize _)
      s = kinesisMaxRecordsPerSecond.foldLeft(s)(_ withMaxRecordsPerSecond _)
      s = kinesisMaxBytesPerSecond.foldLeft(s)(_ withMaxBytesPerSecond _)
      s
    }

    serialized(name, format, graph)
      .map { bytes =>
        val builder = PutRecordsRequestEntry.builder()
        builder.data(SdkBytes.fromByteArray(bytes))
        builder.partitionKey("undefined")
        builder.explicitHashKey(BigInt(128, Random).toString)
        builder.build()
      }
      .via(
        KinesisFlow(
          streamName,
          settings,
        )(kinesisAsyncClient).named(s"sq-output-kinesis-producer-for-$name"),
      )
      .map(_ => token)

  }
}
