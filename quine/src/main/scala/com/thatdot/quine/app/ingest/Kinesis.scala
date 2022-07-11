package com.thatdot.quine.app.ingest

import scala.collection.Set
import scala.compat.java8.FutureConverters.CompletionStageOps
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters.asScalaBufferConverter

import akka.stream.alpakka.kinesis.KinesisErrors.KinesisSourceError
import akka.stream.alpakka.kinesis.scaladsl.KinesisSource
import akka.stream.alpakka.kinesis.{ShardIterator, ShardSettings}
import akka.stream.contrib.{SwitchMode, Valve}
import akka.stream.scaladsl.{Flow, Keep, Source}
import akka.stream.{KillSwitches, Materializer, UniqueKillSwitch}

import com.typesafe.scalalogging.LazyLogging
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.core.retry.RetryPolicy
import software.amazon.awssdk.core.retry.backoff.BackoffStrategy
import software.amazon.awssdk.core.retry.conditions.RetryCondition
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest
import software.amazon.awssdk.services.kinesis.{KinesisAsyncClient, model => kinesisModel}

import com.thatdot.quine.app.ControlSwitches
import com.thatdot.quine.app.ingest.serialization.ImportFormat
import com.thatdot.quine.app.ingest.util.AwsOps
import com.thatdot.quine.app.ingest.util.AwsOps.AwsBuilderOps
import com.thatdot.quine.app.routes.IngestMeter
import com.thatdot.quine.graph.CypherOpsGraph
import com.thatdot.quine.graph.MasterStream.{IngestSrcExecToken, IngestSrcType}
import com.thatdot.quine.routes.AwsCredentials

case object Kinesis extends LazyLogging {

  /** The definition of a source stream from Amazon Kinesis
    *
    * @param streamName The Kinesis stream name
    * @param shardIds The Kinesis shard IDs, or Set.empty to use all shards in the stream. Each probably start "shardId-" Note that this [[KinesisSourceDef]]
    *                 will be invalidated if the stream rescales
    * @param format The [[ImportFormat]] to use to ingest bytes from Kinesis
    * @param meter
    * @param parallelism How many concurrent writes should be performed on the database
    * @param credentials The AWS credentials to access the stream
    */
  final case class KinesisSourceDef(
    streamName: String,
    shardIds: Set[String],
    format: ImportFormat,
    meter: IngestMeter,
    initialSwitchMode: SwitchMode,
    parallelism: Int = 2,
    credentialsOpt: Option[AwsCredentials],
    shardIterator: ShardIterator,
    numRetries: Int,
    maxPerSecond: Option[Int]
  ) {

    /** Start polling for and importing records from the configured shard
      * @param graph
      * @param materializer
      * @param timeout
      * @return a killswitch which can be used to stop the ingest from pulling any further records and a Future
      *         indicating the status of processing. Pending indicates the stream is processing. Failure indicates the
      *         killswitch aborted, the connection to AWS was lost, or an error occured within the stream. Success
      *         indicates the killswitch shutdown the stream and the stream is now empty, or the Kinesis stream returned
      *         a null iterator
      */
    @throws[KinesisSourceError]("KinesisInfo contained an invalid configuration")
    def stream(implicit
      graph: CypherOpsGraph,
      materializer: Materializer
    ): IngestSrcType[ControlSwitches] = {
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
      val kinesisClient = builder.build()
      graph.system.registerOnTermination(kinesisClient.close()) // TODO

      // a Future yielding the shard IDs to read from
      val shardIdsFut: Future[Set[String]] = shardIds match {
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
      val shardSettingsSource = Source
        .future(shardIdsFut)
        .map(ids =>
          ids
            .map(shardId => ShardSettings(streamName, shardId).withShardIterator(shardIterator))
            .toList
        )

      // The Kinesis source, augmented with a killswitch.
      // there is an alternate KCL source we could use (https://doc.akka.io/docs/alpakka/current/kinesis.html) but
      // it requires more configuration, as well as dynamoDB and cloudwatch access
      val kinesisSource: Source[kinesisModel.Record, UniqueKillSwitch] = shardSettingsSource
        .flatMapConcat(shardSettings => KinesisSource.basicMerge(shardSettings, kinesisClient))
        .viaMat(KillSwitches.single)(Keep.right)

      val isSingleHost = graph.isSingleHost
      def throttled[A] = maxPerSecond match {
        case None => Flow[A]
        case Some(perSec) => Flow[A].throttle(perSec, 1.second)
      }

      val ingestToken = IngestSrcExecToken(format.label)
      kinesisSource
        .viaMat(Valve[kinesisModel.Record](initialSwitchMode))(Keep.both)
        .mapConcat { (record: kinesisModel.Record) =>
          // Deserialize the Record using the configured format, or log a warning
          val bytes = record.data().asByteArray()
          meter.mark(bytes.length)
          format
            .importMessageSafeBytes(bytes, isSingleHost)
            .fold(
              { err =>
                // TODO should partition key be treated as PII?
                logger.warn(
                  s"""Failed to deserialize Kinesis record with sequence number: ${record.sequenceNumber} with
                     |partition key: ${record.partitionKey} on stream: $streamName using format:
                     |${format.getClass.getSimpleName}. Skipping record.""".stripMargin.replace('\n', ' ')
                )
                logger.info(s"""Failed to decode Kinesis record: $record""", err)
                List.empty
              },
              List(_)
            )
        }
        .via(throttled)
        .via(graph.ingestThrottleFlow)
        .mapAsyncUnordered(parallelism)(format.writeToGraph(graph, _).map(_ => ingestToken)(graph.system.dispatcher))
        .watchTermination() { case ((a, b), c) => b.map(v => ControlSwitches(a, v, c))(graph.system.dispatcher) }
    }
  }
}
