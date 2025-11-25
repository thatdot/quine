package com.thatdot.quine.app.model.ingest2.sources

import java.util.UUID

import scala.concurrent.duration.{Duration, FiniteDuration, MILLISECONDS}
import scala.jdk.OptionConverters.RichOptional
import scala.util.{Failure, Success, Try}

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.kafka.scaladsl.{Committer, Consumer}
import org.apache.pekko.kafka.{
  CommitDelivery,
  CommitterSettings,
  ConsumerMessage,
  ConsumerSettings,
  Subscription,
  Subscriptions => KafkaSubscriptions,
}
import org.apache.pekko.stream.scaladsl.{Flow, Source}
import org.apache.pekko.{Done, NotUsed}

import cats.data.ValidatedNel
import cats.implicits.catsSyntaxOption
import cats.syntax.functor._
import cats.syntax.validated._
import org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, Deserializer}

import com.thatdot.common.logging.Log._
import com.thatdot.data.{DataFoldableFrom, DataFolderTo}
import com.thatdot.quine.app.KafkaKillSwitch
import com.thatdot.quine.app.model.ingest.serialization.ContentDecoder
import com.thatdot.quine.app.model.ingest.util.KafkaSettingsValidator
import com.thatdot.quine.app.model.ingest2.source.FramedSource
import com.thatdot.quine.app.model.ingest2.sources.KafkaSource._
import com.thatdot.quine.app.routes.IngestMeter
import com.thatdot.quine.exceptions.KafkaValidationException
import com.thatdot.quine.routes.KafkaOffsetCommitting.ExplicitCommit
import com.thatdot.quine.routes._
import com.thatdot.quine.util.BaseError

object KafkaSource {

  /** Stream values where we won't need to retain committable offset information */
  type NoOffset = ConsumerRecord[Array[Byte], Array[Byte]]

  /** Stream values where we'll retain committable offset information */
  type WithOffset = ConsumerMessage.CommittableMessage[Array[Byte], Array[Byte]]

  //See [[KafkaSrcDef]], same sans decoder
  def buildConsumerSettings(
    bootstrapServers: String,
    groupId: String,
    autoOffsetReset: KafkaAutoOffsetReset,
    kafkaProperties: KafkaIngest.KafkaProperties,
    securityProtocol: KafkaSecurityProtocol,
    decoders: Seq[ContentDecoder],
    system: ActorSystem,
  ): ConsumerSettings[Array[Byte], Array[Byte]] = {

    val deserializer: Deserializer[Array[Byte]] = (_: String, data: Array[Byte]) =>
      ContentDecoder.decode(decoders, data)
    val keyDeserializer: ByteArrayDeserializer = new ByteArrayDeserializer() //NO-OP

    // Create Map of kafka properties: combination of user passed properties from `kafkaProperties`
    // as well as those templated by `KafkaAutoOffsetReset` and `KafkaSecurityProtocol`
    // NOTE: This divergence between how kafka properties are set should be resolved, most likely by removing
    // `KafkaAutoOffsetReset`, `KafkaSecurityProtocol`, and `KafkaOffsetCommitting.AutoCommit`
    // in favor of `KafkaIngest.KafkaProperties`. Additionally, the current "template" properties override those in kafkaProperties
    val properties = kafkaProperties ++ Map(
      AUTO_OFFSET_RESET_CONFIG -> autoOffsetReset.name,
      SECURITY_PROTOCOL_CONFIG -> securityProtocol.name,
    )

    ConsumerSettings(system, keyDeserializer, deserializer)
      .withBootstrapServers(bootstrapServers)
      .withGroupId(groupId)
      // Note: The ConsumerSettings stop-timeout delays stopping the Kafka Consumer
      // and the stream, but when using drainAndShutdown that delay is not required and can be set to zero (as below).
      // https://doc.akka.io/docs/alpakka-kafka/current/consumer.html#draining-control
      // We're calling .drainAndShutdown on the Kafka [[Consumer.Control]]
      .withStopTimeout(Duration.Zero)
      .withProperties(properties)
  }

  def subscription(topics: Either[KafkaIngest.Topics, KafkaIngest.PartitionAssignments]): Subscription =
    topics.fold(
      KafkaSubscriptions.topics,
      assignments =>
        KafkaSubscriptions.assignment(
          (
            for {
              (topic, partitions) <- assignments
              partition <- partitions
            } yield new TopicPartition(topic, partition)
          ).toSet,
        ),
    )

  def ackFlow(
    koc: KafkaOffsetCommitting.ExplicitCommit,
    system: ActorSystem,
  ): Flow[WithOffset, Done, NotUsed] = {
    val committer: Flow[ConsumerMessage.Committable, ConsumerMessage.CommittableOffsetBatch, NotUsed] =
      Committer
        .batchFlow(
          CommitterSettings(system)
            .withMaxBatch(koc.maxBatch)
            .withMaxInterval(FiniteDuration(koc.maxIntervalMillis.toLong, MILLISECONDS))
            .withParallelism(koc.parallelism)
            .withDelivery(
              if (koc.waitForCommitConfirmation) CommitDelivery.WaitForAck else CommitDelivery.SendAndForget,
            ),
        )

    // Note - In cases where we are in ExplicitCommit mode with CommitDelivery.WaitForAck _and_ there is an
    // endingOffset set , we will get a akka.kafka.CommitTimeoutException here, since the commit delivery is
    // batched and it's possible to have remaining commit offsets remaining that don't get sent.
    //
    // e.g. partition holds 1000 values, we set koc.maxBatch=100, and endingOffset to 150. Last ack sent will
    // be 100, last 50 will not be sent.
    Flow[WithOffset]
      .map(_.committableOffset)
      .via(committer)
      .map(_ => Done)
  }

  val withOffsetFoldable: DataFoldableFrom[WithOffset] = new DataFoldableFrom[WithOffset] {
    def fold[B](value: WithOffset, folder: DataFolderTo[B]): B = {
      val recordBuilder = folder.mapBuilder()
      recordBuilder.add("value", folder.bytes(value.record.value()))
      recordBuilder.add("key", folder.bytes(value.record.key()))
      recordBuilder.add("topic", folder.string(value.record.topic()))
      recordBuilder.add("partition", folder.integer(value.record.partition().toLong))
      recordBuilder.add("offset", folder.integer(value.record.offset()))
      recordBuilder.add("timestamp", folder.integer(value.record.timestamp()))
      recordBuilder.add("timestampType", folder.string(value.record.timestampType().name()))
      value.record.leaderEpoch().toScala.foreach { epoch =>
        recordBuilder.add("leaderEpoch", folder.integer(epoch.toLong))
      }

      recordBuilder.add("serializedKeySize", folder.integer(value.record.serializedKeySize().toLong))
      recordBuilder.add("serializedValueSize", folder.integer(value.record.serializedValueSize().toLong))

      if (value.record.headers() != null && value.record.headers().iterator().hasNext) {
        val headersBuilder = folder.mapBuilder()
        val it = value.record.headers().iterator()
        while (it.hasNext) {
          val h = it.next()
          headersBuilder.add(h.key(), folder.bytes(h.value()))
        }
        recordBuilder.add("headers", headersBuilder.finish())
      }

      val partitionBuilder = folder.mapBuilder()
      val committableOffset = value.committableOffset

      val partitionOffset = committableOffset.partitionOffset

      partitionBuilder.add("topic", folder.string(partitionOffset.key.topic))
      partitionBuilder.add("partition", folder.integer(partitionOffset.key.partition.toLong))
      partitionBuilder.add("offset", folder.integer(partitionOffset.offset))

      val committableOffsetBuilder = folder.mapBuilder()
      committableOffsetBuilder.add("partitionOffset", partitionBuilder.finish())

      committableOffset match {
        case metadata: ConsumerMessage.CommittableOffsetMetadata =>
          committableOffsetBuilder.add("metadata", folder.string(metadata.metadata))
      }

      val committableMessageBuilder = folder.mapBuilder()
      committableMessageBuilder.add("record", recordBuilder.finish())
      committableMessageBuilder.add("committableOffset", committableOffsetBuilder.finish())
      committableMessageBuilder.finish()

    }
  }

  val noOffsetFoldable: DataFoldableFrom[NoOffset] = new DataFoldableFrom[NoOffset] {
    def fold[B](value: NoOffset, folder: DataFolderTo[B]): B = {
      val builder = folder.mapBuilder()
      builder.add("value", folder.bytes(value.value()))
      builder.add("key", folder.bytes(value.key()))
      builder.add("topic", folder.string(value.topic()))
      builder.add("partition", folder.integer(value.partition().toLong))
      builder.add("offset", folder.integer(value.offset()))
      builder.add("timestamp", folder.integer(value.timestamp()))
      builder.add("timestampType", folder.string(value.timestampType().name()))
      value.leaderEpoch().toScala.foreach { epoch =>
        builder.add("leaderEpoch", folder.integer(epoch.toLong))
      }

      builder.add("serializedKeySize", folder.integer(value.serializedKeySize().toLong))
      builder.add("serializedValueSize", folder.integer(value.serializedValueSize().toLong))

      if (value.headers() != null && value.headers().iterator().hasNext) {
        val headersBuilder = folder.mapBuilder()
        val it = value.headers().iterator()
        while (it.hasNext) {
          val h = it.next()
          headersBuilder.add(h.key(), folder.bytes(h.value()))
        }
        builder.add("headers", headersBuilder.finish())
      }

      builder.finish()

    }
  }
}

case class KafkaSource(
  topics: Either[KafkaIngest.Topics, KafkaIngest.PartitionAssignments],
  bootstrapServers: String,
  groupId: String,
  securityProtocol: KafkaSecurityProtocol,
  maybeExplicitCommit: Option[KafkaOffsetCommitting],
  autoOffsetReset: KafkaAutoOffsetReset,
  kafkaProperties: KafkaIngest.KafkaProperties,
  endingOffset: Option[Long],
  decoders: Seq[ContentDecoder],
  meter: IngestMeter,
  system: ActorSystem,
) extends FramedSourceProvider
    with LazySafeLogging {

  def framedSource: ValidatedNel[BaseError, FramedSource] = Try {
    val subs = subscription(topics)
    val consumerSettings: ConsumerSettings[Array[Byte], Array[Byte]] =
      buildConsumerSettings(
        bootstrapServers,
        groupId,
        autoOffsetReset,
        kafkaProperties,
        securityProtocol,
        decoders,
        system,
      )

    val complaintsFromValidator: ValidatedNel[BaseError, Unit] =
      KafkaSettingsValidator
        .validateInput(consumerSettings.properties, assumeConfigIsFinal = true)
        .map(_.map(KafkaValidationException.apply))
        .toInvalid(())

    maybeExplicitCommit match {
      case Some(explicitCommit: ExplicitCommit) => // Committing source
        complaintsFromValidator.as {
          val consumer: Source[WithOffset, Consumer.Control] =
            Consumer.committableSource(consumerSettings, subs)

          val source: Source[WithOffset, KafkaKillSwitch] = endingOffset
            .fold(consumer)(o => consumer.takeWhile(r => r.record.offset() <= o))
            .via(metered[WithOffset](meter, o => o.record.serializedValueSize()))
            .mapMaterializedValue(KafkaKillSwitch)

          FramedSource[WithOffset](
            source,
            meter,
            input => input.record.value(),
            withOffsetFoldable,
            ackFlow(explicitCommit, system),
          )
        }

      case None => // Non-committing source

        complaintsFromValidator.as {
          val consumer: Source[NoOffset, Consumer.Control] = Consumer.plainSource(consumerSettings, subs)
          val source = endingOffset
            .fold(consumer)(o => consumer.takeWhile(r => r.offset() <= o))
            .via(metered[NoOffset](meter, o => o.serializedValueSize()))
            .mapMaterializedValue(KafkaKillSwitch)
          FramedSource[NoOffset](source, meter, noOffset => noOffset.value(), noOffsetFoldable)
        }
    }
  } match {
    case Success(result) => result
    case Failure(configEx: ConfigException) =>
      val correlationId = UUID.randomUUID()
      logger.error(
        safe"Kafka ConfigException during source creation [correlationId: ${Safe(correlationId.toString)}]: ${Safe(configEx.getMessage)}",
      )
      KafkaValidationException(
        s"Kafka configuration error check logs for [correlationId: ${correlationId.toString}]",
      ).invalidNel
    case Failure(exception) =>
      val correlationId = UUID.randomUUID()
      logger.error(
        safe"Error during source creation [correlationId: ${Safe(correlationId.toString)}]: ${Safe(exception.getMessage)},",
      )
      KafkaValidationException(
        s"A configuration error occurred check logs for [correlationId: ${correlationId.toString}]",
      ).invalidNel
  }

}
