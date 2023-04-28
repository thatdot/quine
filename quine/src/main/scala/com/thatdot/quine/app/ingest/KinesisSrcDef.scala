package com.thatdot.quine.app.ingest

import java.time.Instant

import scala.collection.Set
import scala.compat.java8.FutureConverters.CompletionStageOps
import scala.concurrent.Future
import scala.concurrent.duration.{Duration, DurationInt}
import scala.jdk.CollectionConverters.asScalaBufferConverter

import akka.NotUsed
import akka.stream.alpakka.kinesis.ShardIterator._
import akka.stream.alpakka.kinesis.ShardSettings
import akka.stream.alpakka.kinesis.scaladsl.KinesisSource
import akka.stream.scaladsl.{Flow, Source}

import com.contxt.kinesis.{ConsumerStats, KinesisRecord, NoopConsumerStats}
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.core.retry.RetryPolicy
import software.amazon.awssdk.core.retry.backoff.BackoffStrategy
import software.amazon.awssdk.core.retry.conditions.RetryCondition
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest
import software.amazon.awssdk.services.kinesis.{KinesisAsyncClient, model => kinesisModel}

import com.thatdot.quine.app.ingest.serialization.{ContentDecoder, ImportFormat}
import com.thatdot.quine.app.ingest.util.AwsOps
import com.thatdot.quine.app.ingest.util.AwsOps.AwsBuilderOps
import com.thatdot.quine.graph.CypherOpsGraph
import com.thatdot.quine.graph.MasterStream.IngestSrcExecToken
import com.thatdot.quine.routes.{AwsCredentials, AwsRegion, KinesisIngest}
import com.thatdot.quine.util.SwitchMode

/** The definition of a source stream from Amazon Kinesis
  *
  * @param name           The unique, human-facing name of the ingest stream
  * @param streamName     The Kinesis stream name
  * @param shardIds       The Kinesis shard IDs, or Set.empty to use all shards in the stream. Each probably start "shardId-" Note that this [[KinesisSrcDef]]
  *                       will be invalidated if the stream rescales
  * @param format         The [[ImportFormat]] to use to ingest bytes from Kinesis
  * @param parallelism    How many concurrent writes should be performed on the database
  * @param credentialsOpt The AWS credentials to access the stream
  */
final case class KinesisSrcDef(
  override val name: String,
  streamName: String,
  shardIds: Option[Set[String]],
  format: ImportFormat,
  initialSwitchMode: SwitchMode,
  parallelism: Int = 2,
  credentialsOpt: Option[AwsCredentials],
  regionOpt: Option[AwsRegion],
  iteratorType: KinesisIngest.IteratorType,
  numRetries: Int,
  maxPerSecond: Option[Int],
  decoders: Seq[ContentDecoder]
)(implicit graph: CypherOpsGraph)
    extends RawValuesIngestSrcDef(
      format,
      initialSwitchMode,
      parallelism,
      maxPerSecond,
      decoders,
      s"$name (Kinesis ingest)"
    ) {

  type InputType = kinesisModel.Record

  override def ingestToken: IngestSrcExecToken = IngestSrcExecToken(format.label)

  def rawBytes(record: kinesisModel.Record): Array[Byte] = record.data().asByteArray()

  def source(): Source[kinesisModel.Record, NotUsed] = {

    import KinesisIngest.IteratorType
    val shardIterator = iteratorType match {
      case IteratorType.Latest => Latest
      case IteratorType.TrimHorizon => TrimHorizon
      case IteratorType.AtTimestamp(ms) => AtTimestamp(Instant.ofEpochMilli(ms))
      case IteratorType.AtSequenceNumber(_) | IteratorType.AfterSequenceNumber(_) if shardIds.fold(true)(_.size != 1) =>
        throw new IllegalArgumentException(
          "To use AtSequenceNumber or AfterSequenceNumber, exactly 1 shard must be specified"
        ) // will be caught as an "Invalid" (400) below
      case IteratorType.AtSequenceNumber(seqNo) => AtSequenceNumber(seqNo)
      case IteratorType.AfterSequenceNumber(seqNo) => AfterSequenceNumber(seqNo)
    }

    val kinesisClient = KinesisSrcDef.buildAsyncClient(credentialsOpt, regionOpt, numRetries)

    graph.system.registerOnTermination(kinesisClient.close())

    // a Future yielding the shard IDs to read from
    val shardSettingsFut: Future[List[ShardSettings]] =
      (shardIds.getOrElse(Set()) match {
        case noIds if noIds.isEmpty =>
          kinesisClient
            .describeStream(
              DescribeStreamRequest.builder().streamName(streamName).build()
            )
            .toScala
            .map(response =>
              response
                .streamDescription()
                .shards()
                .asScala
                .map(_.shardId())
                .toSet
            )(graph.materializer.executionContext)
        case atLeastOneId => Future.successful(atLeastOneId)
      })
        .map(ids =>
          ids
            .map(shardId => ShardSettings(streamName, shardId).withShardIterator(shardIterator))
            .toList
        )(graph.materializer.executionContext)

    // A Flow that limits the stream to 2MB * (number of shards) per second
    // TODO This is an imperfect heuristic, as the limit imposed is literally 2MB _per shard_,
    // not 2MB per shard "on average across all shards".
    val kinesisRateLimiter: Flow[kinesisModel.Record, kinesisModel.Record, NotUsed] = Flow
      .futureFlow(
        shardSettingsFut.map { shards =>
          val kinesisShardCount = shards.length
          // there are a maximum of 500 shards per stream
          val throttleBytesPerSecond = kinesisShardCount * 2 * 1024 * 1024
          Flow[kinesisModel.Record]
            .throttle(
              throttleBytesPerSecond,
              1.second,
              rec =>
                // asByteArrayUnsafe avoids extra allocations, to get the length we can't use a readonly bytebuffer
                rec.data().asByteArrayUnsafe().length
            )
        }(graph.materializer.executionContext)
      )
      .mapMaterializedValue(_ => NotUsed)

    Source
      .future(shardSettingsFut)
      .flatMapConcat(shardSettings =>
        KinesisSource
          .basicMerge(shardSettings, kinesisClient)
      )
      .via(kinesisRateLimiter)

  }
}

object KinesisSrcDef {

  def buildAsyncHttpClient: SdkAsyncHttpClient =
    NettyNioAsyncHttpClient.builder.maxConcurrency(AwsOps.httpConcurrencyPerClient).build()
  def buildAsyncClient(
    credentialsOpt: Option[AwsCredentials],
    regionOpt: Option[AwsRegion],
    numRetries: Int
  ): KinesisAsyncClient = {
    val builder = KinesisAsyncClient
      .builder()
      .credentials(credentialsOpt)
      .region(regionOpt)
      .httpClient(buildAsyncHttpClient)
      .overrideConfiguration(
        ClientOverrideConfiguration
          .builder()
          .retryPolicy(
            RetryPolicy
              .builder()
              .backoffStrategy(BackoffStrategy.defaultStrategy())
              .throttlingBackoffStrategy(BackoffStrategy.defaultThrottlingStrategy())
              .numRetries(numRetries)
              .retryCondition(RetryCondition.defaultRetryCondition())
              .build()
          )
          .build()
      )
    builder.build
  }

}
import akka.stream.alpakka.kinesis.KinesisSchedulerCheckpointSettings

// based on https://github.com/StreetContxt/kcl-akka-stream
final case class KinesisCheckpointSrcDef(
  override val name: String,
  streamName: String,
  format: ImportFormat,
  initialSwitchMode: SwitchMode,
  parallelism: Int,
  credentialsOpt: Option[AwsCredentials],
  regionOpt: Option[AwsRegion],
  numRetries: Int,
  maxPerSecond: Option[Int],
  decoders: Seq[ContentDecoder],
  checkpointSettings: KinesisSchedulerCheckpointSettings
)(implicit val graph: CypherOpsGraph)
    extends RawValuesIngestSrcDef(
      format,
      initialSwitchMode,
      parallelism,
      maxPerSecond,
      decoders,
      s"$name (Kinesis CP ingest)"
    ) {

  type InputType = KinesisRecord

  import com.contxt.kinesis.{ConsumerConfig, KinesisSource}

  private val maxWaitForCompletionOnStreamShutdown = Duration.Inf //shorter waits throw an IterationException
  private val shardCheckpointConfig = com.contxt.kinesis.ShardCheckpointConfig(
    checkpointSettings.maxBatchWait,
    checkpointSettings.maxBatchSize,
    maxWaitForCompletionOnStreamShutdown
  )

  private val kinesisClient: KinesisAsyncClient = KinesisSrcDef.buildAsyncClient(credentialsOpt, regionOpt, numRetries)
  private val dynamoClient: DynamoDbAsyncClient = DynamoDbAsyncClient.builder
    .credentials(credentialsOpt)
    .httpClient(KinesisSrcDef.buildAsyncHttpClient)
    .region(regionOpt)
    .build
  private val cloudWatchClient: CloudWatchAsyncClient = CloudWatchAsyncClient.builder
    .credentials(credentialsOpt)
    .httpClient(KinesisSrcDef.buildAsyncHttpClient)
    .region(regionOpt)
    .build

  private val consumerConfig =
    ConsumerConfig(streamName, "atLeastOnceApp")(kinesisClient, dynamoClient, cloudWatchClient)
  private val stats: ConsumerStats = new NoopConsumerStats

  def source(): Source[KinesisRecord, NotUsed.type] = KinesisSource
    .apply(consumerConfig, shardCheckpointConfig, stats)
    .map { message =>
      // After a record is marked as processed, it is eligible to be checkpointed in DynamoDb.
      message.markProcessed()
      message
    }
    .mapMaterializedValue(_ => NotUsed)

  /** Define a way to extract raw bytes from a single input event */
  override def rawBytes(value: KinesisRecord): Array[Byte] = value.data.toArray
}
