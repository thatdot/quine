package com.thatdot.quine.app.ingest2.sources

import java.time.Instant

import scala.collection.Set
import scala.compat.java8.FutureConverters.CompletionStageOps
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.connectors.kinesis.ShardIterator._
import org.apache.pekko.stream.connectors.kinesis.scaladsl.{KinesisSource => PekkoKinesisSource}
import org.apache.pekko.stream.connectors.kinesis.{ShardIterator, ShardSettings}
import org.apache.pekko.stream.scaladsl.{Flow, Source}

import cats.data.Validated.{Valid, invalidNel}
import cats.data.ValidatedNel
import software.amazon.awssdk.awscore.retry.AwsRetryStrategy
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.retries.StandardRetryStrategy
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest
import software.amazon.awssdk.services.kinesis.{KinesisAsyncClient, model => kinesisModel}

import com.thatdot.quine.app.ingest.serialization.ContentDecoder
import com.thatdot.quine.app.ingest.util.AwsOps
import com.thatdot.quine.app.ingest.util.AwsOps.AwsBuilderOps
import com.thatdot.quine.app.ingest2.source.FramedSource
import com.thatdot.quine.app.ingest2.sources.KinesisSource.buildAsyncClient
import com.thatdot.quine.app.routes.IngestMeter
import com.thatdot.quine.exceptions.ShardIterationException
import com.thatdot.quine.routes.{AwsCredentials, AwsRegion, KinesisIngest}
import com.thatdot.quine.util.BaseError

object KinesisSource {

  def buildAsyncHttpClient: SdkAsyncHttpClient =
    NettyNioAsyncHttpClient.builder.maxConcurrency(AwsOps.httpConcurrencyPerClient).build()

  def buildAsyncClient(
    credentialsOpt: Option[AwsCredentials],
    regionOpt: Option[AwsRegion],
    numRetries: Int,
  ): KinesisAsyncClient = {
    val retryStrategy: StandardRetryStrategy = AwsRetryStrategy
      .standardRetryStrategy()
      .toBuilder
      .maxAttempts(numRetries)
      .build()
    val builder = KinesisAsyncClient
      .builder()
      .credentials(credentialsOpt)
      .region(regionOpt)
      .httpClient(buildAsyncHttpClient)
      .overrideConfiguration(
        ClientOverrideConfiguration
          .builder()
          .retryStrategy(retryStrategy)
          .build(),
      )

    builder.build
  }

}

case class KinesisSource(
  streamName: String,
  shardIds: Option[Set[String]],
  credentialsOpt: Option[AwsCredentials],
  regionOpt: Option[AwsRegion],
  iteratorType: KinesisIngest.IteratorType,
  numRetries: Int,
  meter: IngestMeter,
  decoders: Seq[ContentDecoder] = Seq(),
)(implicit val ec: ExecutionContext)
    extends FramedSourceProvider {

  val kinesisClient: KinesisAsyncClient = buildAsyncClient(credentialsOpt, regionOpt, numRetries)
  import KinesisIngest.IteratorType
  private val shardIterator: ValidatedNel[BaseError, ShardIterator] = iteratorType match {
    case IteratorType.Latest => Valid(Latest)
    case IteratorType.TrimHorizon => Valid(TrimHorizon)
    case IteratorType.AtTimestamp(ms) => Valid(AtTimestamp(Instant.ofEpochMilli(ms)))
    case IteratorType.AtSequenceNumber(_) | IteratorType.AfterSequenceNumber(_) if shardIds.fold(true)(_.size != 1) =>
      invalidNel[BaseError, ShardIterator](
        ShardIterationException("To use AtSequenceNumber or AfterSequenceNumber, exactly 1 shard must be specified"),
      )
    // will be caught as an "Invalid" (400) below
    case IteratorType.AtSequenceNumber(seqNo) => Valid(AtSequenceNumber(seqNo))
    case IteratorType.AfterSequenceNumber(seqNo) => Valid(AfterSequenceNumber(seqNo))
  }

  private def kinesisStream(shardIterator: ShardIterator): Source[kinesisModel.Record, NotUsed] = {

    // a Future yielding the shard IDs to read from
    val shardSettingsFut: Future[List[ShardSettings]] =
      (shardIds.getOrElse(Set()) match {
        case noIds if noIds.isEmpty =>
          kinesisClient
            .describeStream(
              DescribeStreamRequest.builder().streamName(streamName).build(),
            )
            .toScala
            .map(response =>
              response
                .streamDescription()
                .shards()
                .asScala
                .map(_.shardId())
                .toSet,
            )(ec)
        case atLeastOneId => Future.successful(atLeastOneId)
      })
        .map(ids =>
          ids
            .map(shardId => ShardSettings(streamName, shardId).withShardIterator(shardIterator))
            .toList,
        )

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
                rec.data().asByteArrayUnsafe().length,
            )
            .via(metered[kinesisModel.Record](meter, r => r.data().asByteArrayUnsafe().length))
        }(ec),
      )
      .mapMaterializedValue(_ => NotUsed)

    Source
      .future(shardSettingsFut)
      .flatMapConcat(shardSettings => PekkoKinesisSource.basicMerge(shardSettings, kinesisClient))
      .via(kinesisRateLimiter)
  }

  def framedSource: ValidatedNel[BaseError, FramedSource] =
    shardIterator.map { si =>
      FramedSource[kinesisModel.Record](
        withKillSwitches(kinesisStream(si)),
        meter,
        record => ContentDecoder.decode(decoders, record.data().asByteArrayUnsafe()),
        terminationHook = () => kinesisClient.close(),
      )
    }
}
