package com.thatdot.quine.app.ingest

import java.time.Instant

import scala.collection.Set
import scala.compat.java8.FutureConverters.CompletionStageOps
import scala.concurrent.Future
import scala.jdk.CollectionConverters.asScalaBufferConverter

import akka.NotUsed
import akka.stream.alpakka.kinesis.ShardIterator._
import akka.stream.alpakka.kinesis.ShardSettings
import akka.stream.alpakka.kinesis.scaladsl.KinesisSource
import akka.stream.contrib.SwitchMode
import akka.stream.scaladsl.Source

import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.core.retry.RetryPolicy
import software.amazon.awssdk.core.retry.backoff.BackoffStrategy
import software.amazon.awssdk.core.retry.conditions.RetryCondition
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest
import software.amazon.awssdk.services.kinesis.{KinesisAsyncClient, model => kinesisModel}

import com.thatdot.quine.app.ingest.serialization.ImportFormat
import com.thatdot.quine.app.ingest.util.AwsOps
import com.thatdot.quine.app.ingest.util.AwsOps.AwsBuilderOps
import com.thatdot.quine.graph.CypherOpsGraph
import com.thatdot.quine.graph.MasterStream.IngestSrcExecToken
import com.thatdot.quine.routes.{AwsCredentials, KinesisIngest}

/** The definition of a source stream from Amazon Kinesis
  *
  * @param streamName The Kinesis stream name
  * @param shardIds The Kinesis shard IDs, or Set.empty to use all shards in the stream. Each probably start "shardId-" Note that this [[KinesisSrcDef]]
  *                 will be invalidated if the stream rescales
  * @param format The [[ImportFormat]] to use to ingest bytes from Kinesis
  * @param parallelism How many concurrent writes should be performed on the database
  * @param credentialsOpt The AWS credentials to access the stream
  */
final case class KinesisSrcDef(
  streamName: String,
  shardIds: Option[Set[String]],
  format: ImportFormat,
  initialSwitchMode: SwitchMode,
  parallelism: Int = 2,
  credentialsOpt: Option[AwsCredentials],
  iteratorType: KinesisIngest.IteratorType,
  numRetries: Int,
  maxPerSecond: Option[Int]
)(implicit graph: CypherOpsGraph)
    extends RawValuesIngestSrcDef(
      format,
      initialSwitchMode,
      parallelism,
      maxPerSecond,
      s"kinesis-$streamName"
    ) {

  type InputType = kinesisModel.Record

  override def ingestToken: IngestSrcExecToken = IngestSrcExecToken(format.label)

  def rawBytes(record: kinesisModel.Record): Array[Byte] = record.data().asByteArray()

  def source(): Source[kinesisModel.Record, NotUsed] = {

    val builder = KinesisAsyncClient
      .builder()
      .credentials(credentialsOpt)
      .httpClient(
        NettyNioAsyncHttpClient.builder.maxConcurrency(AwsOps.httpConcurrencyPerClient).build()
      )
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

    val kinesisClient = builder.build()
    graph.system.registerOnTermination(kinesisClient.close()) // TODO

    // a Future yielding the shard IDs to read from
    val shardIdsFut: Future[Set[String]] = shardIds.getOrElse(Set()) match {
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
          )(graph.system.dispatcher)
      case atLeastOneId => Future.successful(atLeastOneId)
    }
    // a source to yielding a single List of shard settings
    val shardSettingsSource: Source[List[ShardSettings], NotUsed] = Source
      .future(shardIdsFut)
      .map(ids =>
        ids
          .map(shardId => ShardSettings(streamName, shardId).withShardIterator(shardIterator))
          .toList
      )

    /* Kinesis data source
     there is an alternate KCL source we could use (https://doc.akka.io/docs/alpakka/current/kinesis.html) but
     it requires more configuration, as well as dynamoDB and cloudwatch access
     */

    // Start polling for and importing records from the configured shard
    shardSettingsSource
      .flatMapConcat(shardSettings =>
        KinesisSource.basicMerge(shardSettings, kinesisClient).named(s"kinesis-ingest-source-for-$streamName")
      )
  }
}
