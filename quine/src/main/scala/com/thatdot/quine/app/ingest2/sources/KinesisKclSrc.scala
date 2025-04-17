package com.thatdot.quine.app.ingest2.sources

import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.{Calendar, UUID}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

import org.apache.pekko.stream.connectors.kinesis.scaladsl.KinesisSchedulerSource
import org.apache.pekko.stream.connectors.kinesis.{
  CommittableRecord,
  KinesisSchedulerCheckpointSettings,
  KinesisSchedulerSourceSettings,
}
import org.apache.pekko.stream.scaladsl.{Flow, Source}
import org.apache.pekko.{Done, NotUsed}

import cats.data.Validated.Valid
import cats.data.ValidatedNel
import com.typesafe.scalalogging.LazyLogging
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

import com.thatdot.quine.app.ingest.serialization.ContentDecoder
import com.thatdot.quine.app.ingest.util.AwsOps
import com.thatdot.quine.app.ingest.util.AwsOps.AwsBuilderOps
import com.thatdot.quine.app.ingest2.V2IngestEntities.{AtTimestamp, InitialPosition, Latest, TrimHorizon}
import com.thatdot.quine.app.ingest2.source.FramedSource
import com.thatdot.quine.app.routes.IngestMeter
import com.thatdot.quine.util.BaseError
import com.thatdot.quine.{routes => V1}

/** The definition of a source stream from Amazon Kinesis using KCL,
  * now translated to expose a framedSource.
  *
  * @param name               The unique, human-facing name of the ingest stream
  * @param applicationName    The name of the application as seen by KCL and its accompanying DynamoDB instance
  * @param kinesisStreamName  The Kinesis stream name
  * @param meter              An instance of [[IngestMeter]] for metering the ingest flow
  * @param credentialsOpt     The AWS credentials to access the stream (optional)
  * @param regionOpt          The AWS region in which Kinesis resides (optional)
  * @param initialPosition    The KCL iterator type describing where to begin reading records
  * @param numRetries         The maximum number of retry attempts for AWS client calls
  * @param decoders           A sequence of [[ContentDecoder]] for handling inbound Kinesis records
  * @param checkpointSettings Kinesis checkpointing configuration
  */
final case class KinesisKclSrc(
  name: String,
  applicationName: Option[String],
  kinesisStreamName: String,
  meter: IngestMeter,
  credentialsOpt: Option[V1.AwsCredentials],
  regionOpt: Option[V1.AwsRegion],
  initialPosition: InitialPosition,
  numRetries: Int,
  decoders: Seq[ContentDecoder],
  bufferSize: Int,
  backpressureTimeoutMillis: Long,
  checkpointSettings: Option[V1.KinesisIngest.KinesisCheckpointSettings],
)(implicit val ec: ExecutionContext)
    extends FramedSourceProvider
    with LazyLogging {

  import KinesisKclSrc._

  /** Builds and returns a `FramedSource`, wrapped in a `ValidatedNel` for error handling.
    * This method instantiates Kinesis, DynamoDB, and CloudWatch async clients,
    * configures a KCL scheduler, and returns a framed Akka Stream source that
    * emits byte representation of [[CommittableRecord]] instances.
    *
    * @return A [[ValidatedNel]] of [[BaseError]] or a [[FramedSource]].
    */
  override def framedSource: ValidatedNel[BaseError, FramedSource] = {
    val httpClient = buildAsyncHttpClient
    val kinesisClient = buildAsyncClient(httpClient, credentialsOpt, regionOpt, numRetries)
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

    val schedulerSourceSettings =
      KinesisSchedulerSourceSettings(
        bufferSize,
        FiniteDuration(backpressureTimeoutMillis, MILLISECONDS),
      )

    val builder: ShardRecordProcessorFactory => Scheduler = { recordProcessorFactory =>
      val hostname = {
        try InetAddress.getLocalHost.getHostName
        catch {
          case _: Exception => "unknown"
        }
      }

      val initialPositionInStream: InitialPositionInStreamExtended = initialPosition match {
        case Latest =>
          InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST)
        case TrimHorizon =>
          InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON)
        case AtTimestamp(year, month, date, hourOfDay, minute, second) =>
          val time = Calendar.getInstance()
          // Minus one because Calendar Month is 0 indexed
          time.set(year, month - 1, date, hourOfDay, minute, second)
          InitialPositionInStreamExtended.newInitialPositionAtTimestamp(time.getTime)
      }

      val configsBuilder = new ConfigsBuilder(
        new SingleStreamTracker(kinesisStreamName, initialPositionInStream),
        applicationName.getOrElse("Quine"),
        kinesisClient,
        dynamoClient,
        cloudWatchClient,
        s"$hostname:${UUID.randomUUID()}",
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

    val source: Source[CommittableRecord, NotUsed] =
      KinesisSchedulerSource(builder, schedulerSourceSettings)
        .mapMaterializedValue(_ => NotUsed)
        .via(metered[CommittableRecord](meter, r => recordBufferToArray(r.record.data()).length))

    val framed = FramedSource[CommittableRecord](
      withKillSwitches(source),
      meter,
      record => ContentDecoder.decode(decoders, recordBufferToArray(record.record.data())),
      terminationHook = () => {
        Seq(kinesisClient, dynamoClient, cloudWatchClient).foreach { client =>
          client.close()
        }
      },
      // Performs Checkpointing logic, defined below
      ackFlow = ack,
    )
    Valid(framed)
  }

  val ack: Flow[CommittableRecord, Done, NotUsed] = {
    val defaultSettings: KinesisSchedulerCheckpointSettings = KinesisSchedulerCheckpointSettings.defaults
    checkpointSettings
      .map {
        case apiSettings if !apiSettings.disableCheckpointing =>
          KinesisSchedulerCheckpointSettings
            .apply(
              apiSettings.maxBatchSize.getOrElse(defaultSettings.maxBatchSize),
              apiSettings.maxBatchWaitMillis.map(Duration(_, MILLISECONDS)).getOrElse(defaultSettings.maxBatchWait),
            )
        case _ =>
          defaultSettings
      }
      .map(
        KinesisSchedulerSource
          .checkpointRecordsFlow(_)
          .map(_ => Done),
      )
      .getOrElse(Flow[CommittableRecord].map(_ => Done))
  }
}

object KinesisKclSrc {

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
    credentialsOpt: Option[V1.AwsCredentials],
    regionOpt: Option[V1.AwsRegion],
    numRetries: Int,
  ): KinesisAsyncClient = {
    val retryStrategy: StandardRetryStrategy = AwsRetryStrategy
      .standardRetryStrategy()
      .toBuilder
      .maxAttempts(numRetries)
      .build()
    KinesisAsyncClient
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
      .build
  }
}
