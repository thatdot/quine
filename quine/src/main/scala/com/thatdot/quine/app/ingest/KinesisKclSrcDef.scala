package com.thatdot.quine.app.ingest

import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.{Date, UUID}

import scala.concurrent.duration._
import scala.util.Try

import org.apache.pekko.stream.connectors.kinesis.scaladsl.KinesisSchedulerSource
import org.apache.pekko.stream.connectors.kinesis.{
  CommittableRecord,
  KinesisSchedulerCheckpointSettings,
  KinesisSchedulerSourceSettings,
}
import org.apache.pekko.stream.scaladsl.{Flow, Source}
import org.apache.pekko.{Done, NotUsed}

import software.amazon.awssdk.awscore.retry.AwsRetryStrategy
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.retries.StandardRetryStrategy
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.kinesis.common.{ConfigsBuilder, InitialPositionInStream, InitialPositionInStreamExtended}
import software.amazon.kinesis.coordinator.Scheduler
import software.amazon.kinesis.processor.{ShardRecordProcessorFactory, SingleStreamTracker}

import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.quine.app.ingest.serialization.{ContentDecoder, ImportFormat}
import com.thatdot.quine.app.ingest.util.AwsOps
import com.thatdot.quine.app.ingest.util.AwsOps.AwsBuilderOps
import com.thatdot.quine.graph.MasterStream.IngestSrcExecToken
import com.thatdot.quine.graph.cypher.Value
import com.thatdot.quine.graph.{CypherOpsGraph, NamespaceId}
import com.thatdot.quine.routes.KinesisIngest.IteratorType
import com.thatdot.quine.routes.{AwsCredentials, AwsRegion, KinesisCheckpointSettings, KinesisIngest}
import com.thatdot.quine.util.SwitchMode

/** The definition of a source stream from Amazon Kinesis using the Kinesis Client Library (KCL).
  *
  * @param name              The unique, human-facing name of the ingest stream
  * @param intoNamespace     The namespace (database) into which the data is ingested
  * @param streamName        The Kinesis stream name
  * @param format            The [[ImportFormat]] describing how to parse bytes read from Kinesis
  * @param initialSwitchMode The initial mode that controls whether ingestion is active or paused
  * @param parallelism       How many concurrent writes should be performed on the database
  * @param credentialsOpt    The AWS credentials to access the stream (if None, default credentials are used)
  * @param regionOpt         The AWS region in which the stream resides (if None, default region is used)
  * @param iteratorType      The type of iterator (e.g., LATEST, TRIM_HORIZON) used by KCL
  * @param numRetries        How many times to retry on ingest failures
  * @param maxPerSecond      Optional rate limit (records per second). If None, no explicit rate limit is applied
  * @param decoders          A sequence of [[ContentDecoder]] instances for transforming the ingested data
  * @param checkpointSettings Settings controlling how checkpoints are managed for this stream
  */
final case class KinesisKclSrcDef(
  override val name: String,
  override val intoNamespace: NamespaceId,
  streamName: String,
  format: ImportFormat,
  initialSwitchMode: SwitchMode,
  parallelism: Int = 2,
  credentialsOpt: Option[AwsCredentials],
  regionOpt: Option[AwsRegion],
  iteratorType: KinesisIngest.IteratorType,
  numRetries: Int,
  maxPerSecond: Option[Int],
  decoders: Seq[ContentDecoder],
  checkpointSettings: KinesisCheckpointSettings,
)(implicit val graph: CypherOpsGraph, protected val logConfig: LogConfig)
    extends RawValuesIngestSrcDef(
      format,
      initialSwitchMode,
      parallelism,
      maxPerSecond,
      decoders,
      s"$name (Kinesis ingest)",
      intoNamespace,
    ) {
  import KinesisKclSrcDef._

  type InputType = CommittableRecord

  override val ingestToken: IngestSrcExecToken = IngestSrcExecToken(format.label)

  def rawBytes(record: CommittableRecord): Array[Byte] = recordBufferToArray(record.record.data())

  def source(): Source[CommittableRecord, NotUsed] = {
    val httpClient = buildAsyncHttpClient
    val kinesisClient = buildAsyncClient(buildAsyncHttpClient, credentialsOpt, regionOpt, numRetries)
    val dynamoClient: DynamoDbAsyncClient = DynamoDbAsyncClient.builder
      .credentials(credentialsOpt)
      .httpClient(httpClient)
      .region(regionOpt)
      .build

    val cloudWatchClient: CloudWatchAsyncClient = CloudWatchAsyncClient.builder
      .credentials(credentialsOpt)
      .httpClient(httpClient)
      .region(regionOpt)
      .build

    Seq(kinesisClient, dynamoClient, cloudWatchClient).foreach { client =>
      graph.system.registerOnTermination(client.close())
    }

    val schedulerSourceSettings = KinesisSchedulerSourceSettings.apply

    val builder: ShardRecordProcessorFactory => Scheduler =
      recordProcessorFactory => {

        // Configuration settings point to set the initial stream position used below in the Scheduler
        val initialPosition: InitialPositionInStreamExtended = iteratorType match {
          case IteratorType.Latest => InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST)
          case IteratorType.TrimHorizon =>
            InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON)
          case IteratorType.AtTimestamp(ms) =>
            InitialPositionInStreamExtended.newInitialPositionAtTimestamp(new Date(ms))
          case _ =>
            throw new IllegalArgumentException(
              s"Only Latest, TrimHorizon, and AtTimestamp are valid Iterator Types when using the KCL version of Kinesis",
            ) // will be caught as an "Invalid" (400) below
        }

        val configsBuilder = new ConfigsBuilder(
          new SingleStreamTracker(streamName, initialPosition),
          checkpointSettings.appName,
          kinesisClient,
          dynamoClient,
          cloudWatchClient,
          s"${InetAddress.getLocalHost.getHostName}:${UUID.randomUUID()}",
          recordProcessorFactory,
        )

        new Scheduler(
          configsBuilder.checkpointConfig,
          configsBuilder.coordinatorConfig,
          configsBuilder.leaseManagementConfig,
          configsBuilder.lifecycleConfig,
          configsBuilder.metricsConfig,
          configsBuilder.processorConfig,
          configsBuilder.retrievalConfig,
        )
      }

    val source = KinesisSchedulerSource(builder, schedulerSourceSettings)
    source.mapMaterializedValue(_ => NotUsed)
  }

  override val ack: Flow[(Try[Value], CommittableRecord), Done, NotUsed] = {
    val settings = KinesisSchedulerCheckpointSettings(
      checkpointSettings.maxBatchSize,
      checkpointSettings.maxBatchWaitMillis.millis,
    )
    KinesisSchedulerSource
      .checkpointRecordsFlow(settings)
      .contramap[(Try[Value], CommittableRecord)]({ case (_, cr) => cr })
      .map(_ => Done)
  }
}

object KinesisKclSrcDef {

  /** Converts the supplied [[ByteBuffer]] to an `Array[Byte]`.
    * A new byte array is allocated and populated by reading from a duplication of the buffer.
    *
    * @param data The [[ByteBuffer]] to convert
    * @return A corresponding array of bytes
    */
  private def recordBufferToArray(data: ByteBuffer): Array[Byte] = {
    // Duplicate in case something else was using the position information
    val duplicateBuffer = data.duplicate()
    val bytes = new Array[Byte](duplicateBuffer.remaining())
    duplicateBuffer.get(bytes)
    bytes
  }

  def buildAsyncHttpClient: SdkAsyncHttpClient =
    NettyNioAsyncHttpClient.builder.maxConcurrency(AwsOps.httpConcurrencyPerClient).build()

  def buildAsyncClient(
    httpClient: SdkAsyncHttpClient,
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
      .httpClient(httpClient)
      .overrideConfiguration(
        ClientOverrideConfiguration
          .builder()
          .retryStrategy(retryStrategy)
          .build(),
      )
    builder.build
  }
}
