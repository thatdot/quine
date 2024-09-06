package com.thatdot.quine.app.ingest2.sources

import scala.concurrent.duration.{Duration, FiniteDuration, MILLISECONDS}

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.kafka.scaladsl.{Committer, Consumer}
import org.apache.pekko.kafka.{
  CommitDelivery,
  CommitterSettings,
  ConsumerMessage,
  ConsumerSettings,
  Subscription,
  Subscriptions => KafkaSubscriptions
}
import org.apache.pekko.stream.scaladsl.{Flow, Source}
import org.apache.pekko.{Done, NotUsed}

import cats.data.ValidatedNel
import cats.implicits.catsSyntaxOption
import cats.syntax.functor._
import org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, Deserializer}

import com.thatdot.quine.app.KafkaKillSwitch
import com.thatdot.quine.app.ingest.serialization.ContentDecoder
import com.thatdot.quine.app.ingest.util.KafkaSettingsValidator
import com.thatdot.quine.app.ingest2.source.FramedSource
import com.thatdot.quine.app.ingest2.sources.KafkaSource._
import com.thatdot.quine.app.routes.IngestMeter
import com.thatdot.quine.routes.KafkaOffsetCommitting.ExplicitCommit
import com.thatdot.quine.routes._

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
    system: ActorSystem
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
      SECURITY_PROTOCOL_CONFIG -> securityProtocol.name
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
          ).toSet
        )
    )

  def ackFlow(
    koc: KafkaOffsetCommitting.ExplicitCommit,
    system: ActorSystem
  ): Flow[WithOffset, Done, NotUsed] = {
    val committer: Flow[ConsumerMessage.Committable, ConsumerMessage.CommittableOffsetBatch, NotUsed] =
      Committer
        .batchFlow(
          CommitterSettings(system)
            .withMaxBatch(koc.maxBatch)
            .withMaxInterval(FiniteDuration(koc.maxIntervalMillis.toLong, MILLISECONDS))
            .withParallelism(koc.parallelism)
            .withDelivery(
              if (koc.waitForCommitConfirmation) CommitDelivery.WaitForAck else CommitDelivery.SendAndForget
            )
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
  system: ActorSystem
) {

  def framedSource: FramedSource = {
    val subs = subscription(topics)
    val consumerSettings: ConsumerSettings[Array[Byte], Array[Byte]] =
      buildConsumerSettings(
        bootstrapServers,
        groupId,
        autoOffsetReset,
        kafkaProperties,
        securityProtocol,
        decoders,
        system
      )

    val complaintsFromValidator: ValidatedNel[String, Unit] =
      KafkaSettingsValidator
        .validateInput(consumerSettings.properties, assumeConfigIsFinal = true)
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
            ackFlow(explicitCommit, system)
          )
        }

      case None => // Non-committing source
        complaintsFromValidator.as {
          val consumer: Source[NoOffset, Consumer.Control] = Consumer.plainSource(consumerSettings, subs)
          val source = endingOffset
            .fold(consumer)(o => consumer.takeWhile(r => r.offset() <= o))
            .via(metered[NoOffset](meter, o => o.serializedValueSize()))
            .mapMaterializedValue(KafkaKillSwitch)
          FramedSource[NoOffset](source, meter, noOffset => noOffset.value())
        }
    }
  }.toOption.get //TODO From ValidateNel

}
