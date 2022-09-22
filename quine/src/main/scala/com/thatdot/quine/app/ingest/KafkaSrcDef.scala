package com.thatdot.quine.app.ingest

import scala.concurrent.duration.{Duration, FiniteDuration, MILLISECONDS}
import scala.util.Try

import akka.kafka.scaladsl.{Committer, Consumer}
import akka.kafka.{
  CommitDelivery,
  CommitterSettings,
  ConsumerMessage,
  ConsumerSettings,
  Subscription,
  Subscriptions => KafkaSubscriptions
}
import akka.stream.contrib.SwitchMode
import akka.stream.scaladsl.{Flow, Source}
import akka.{Done, NotUsed}

import org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.{
  AUTO_COMMIT_INTERVAL_MS_CONFIG,
  AUTO_OFFSET_RESET_CONFIG,
  ENABLE_AUTO_COMMIT_CONFIG
}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, Deserializer}

import com.thatdot.quine.app.KafkaKillSwitch
import com.thatdot.quine.app.ingest.serialization.ImportFormat
import com.thatdot.quine.graph.CypherOpsGraph
import com.thatdot.quine.graph.cypher.Value
import com.thatdot.quine.routes.{KafkaAutoOffsetReset, KafkaIngest, KafkaOffsetCommitting, KafkaSecurityProtocol}

object KafkaSrcDef {

  /** Stream values where we won't need to retain committable offset information */
  type NoOffset = ConsumerRecord[Array[Byte], Try[Value]]

  /** Stream values where we'll retain committable offset information */
  type WithOffset = ConsumerMessage.CommittableMessage[Array[Byte], Try[Value]]

  private def buildConsumerSettings(
    format: ImportFormat,
    isSingleHost: Boolean,
    bootstrapServers: String,
    groupId: String,
    autoOffsetReset: KafkaAutoOffsetReset,
    securityProtocol: KafkaSecurityProtocol
  )(implicit graph: CypherOpsGraph): ConsumerSettings[Array[Byte], Try[Value]] = {

    val deserializer: Deserializer[Try[Value]] =
      (_: String, data: Array[Byte]) => format.importMessageSafeBytes(data, isSingleHost)

    val keyDeserializer: ByteArrayDeserializer = new ByteArrayDeserializer() //NO-OP

    ConsumerSettings(graph.system, keyDeserializer, deserializer)
      .withBootstrapServers(bootstrapServers)
      .withGroupId(groupId)
      // Note: The ConsumerSettings stop-timeout delays stopping the Kafka Consumer
      // and the stream, but when using drainAndShutdown that delay is not required and can be set to zero (as below).
      // https://doc.akka.io/docs/alpakka-kafka/current/consumer.html#draining-control
      // We're calling .drainAndShutdown on the Kafka [[Consumer.Control]]
      .withStopTimeout(Duration.Zero)
      .withProperties(
        AUTO_OFFSET_RESET_CONFIG -> autoOffsetReset.name,
        SECURITY_PROTOCOL_CONFIG -> securityProtocol.name
      )
  }

  def apply(
    topics: Either[KafkaIngest.Topics, KafkaIngest.PartitionAssignments],
    bootstrapServers: String,
    groupId: String,
    format: ImportFormat,
    initialSwitchMode: SwitchMode,
    parallelism: Int = 2,
    securityProtocol: KafkaSecurityProtocol,
    offsetCommitting: Option[KafkaOffsetCommitting],
    autoOffsetReset: KafkaAutoOffsetReset,
    endingOffset: Option[Long],
    maxPerSecond: Option[Int]
  )(implicit graph: CypherOpsGraph): IngestSrcDef = {
    val isSingleHost: Boolean = graph.isSingleHost
    val subscription: Subscription = topics.fold(
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

    val consumerSettings: ConsumerSettings[Array[Byte], Try[Value]] =
      buildConsumerSettings(format, isSingleHost, bootstrapServers, groupId, autoOffsetReset, securityProtocol)

    offsetCommitting match {
      case Some(KafkaOffsetCommitting.AutoCommit(commitIntervalMs)) =>
        val consumer: Source[NoOffset, Consumer.Control] = Consumer.plainSource(
          consumerSettings.withProperties(
            ENABLE_AUTO_COMMIT_CONFIG -> "true",
            AUTO_COMMIT_INTERVAL_MS_CONFIG -> commitIntervalMs.toString
          ),
          subscription
        )

        NonCommitting(
          format,
          initialSwitchMode,
          parallelism,
          consumer,
          endingOffset,
          maxPerSecond
        )
      case None =>
        val consumer: Source[NoOffset, Consumer.Control] = Consumer.plainSource(consumerSettings, subscription)

        NonCommitting(
          format,
          initialSwitchMode,
          parallelism,
          consumer,
          endingOffset,
          maxPerSecond
        )
      case Some(koc @ KafkaOffsetCommitting.ExplicitCommit(_, _, _, _)) =>
        val consumer: Source[WithOffset, Consumer.Control] =
          Consumer.committableSource(consumerSettings, subscription)

        Committing(
          format,
          initialSwitchMode,
          parallelism,
          consumer,
          endingOffset,
          maxPerSecond,
          koc
        )
    }
  }

  /** Kafka type that does not ack offset information. */
  case class NonCommitting(
    format: ImportFormat,
    initialSwitchMode: SwitchMode,
    parallelism: Int = 2,
    kafkaConsumer: Source[NoOffset, Consumer.Control],
    endingOffset: Option[Long],
    maxPerSecond: Option[Int]
  )(implicit graph: CypherOpsGraph)
      extends IngestSrcDef(format, initialSwitchMode, parallelism, maxPerSecond, "kafka") {

    type InputType = NoOffset

    override def sourceWithShutdown(): Source[(Try[Value], NoOffset), KafkaKillSwitch] =
      endingOffset
        .fold(kafkaConsumer)(o => kafkaConsumer.takeWhile(r => r.offset() <= o))
        .wireTap((o: NoOffset) => meter.mark(o.serializedValueSize()))
        .mapMaterializedValue(KafkaKillSwitch)
        .map((o: NoOffset) => (o.value(), o))

  }

  /** Kafka type with ack. */
  case class Committing(
    format: ImportFormat,
    initialSwitchMode: SwitchMode,
    parallelism: Int = 2,
    kafkaConsumer: Source[WithOffset, Consumer.Control],
    endingOffset: Option[Long],
    maxPerSecond: Option[Int],
    koc: KafkaOffsetCommitting.ExplicitCommit
  )(implicit graph: CypherOpsGraph)
      extends IngestSrcDef(format, initialSwitchMode, parallelism, maxPerSecond, "kafka") {
    type InputType = WithOffset
    override def sourceWithShutdown(): Source[TryDeserialized, KafkaKillSwitch] =
      endingOffset
        .fold(kafkaConsumer)(o => kafkaConsumer.takeWhile(r => r.record.offset() <= o))
        .wireTap((o: WithOffset) => meter.mark(o.record.key().length))
        .mapMaterializedValue(KafkaKillSwitch)
        .map((o: WithOffset) => (o.record.value(), o))

    /** For ack-ing source override the default mapAsyncUnordered behavior.
      */
    override val writeToGraph: Flow[TryDeserialized, TryDeserialized, NotUsed] =
      Flow[TryDeserialized].mapAsync(parallelism)(writeSuccessValues)

    override val ack: Flow[TryDeserialized, Done, NotUsed] = {
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

      Flow[TryDeserialized].map(_._2.committableOffset).via(committer).map(_ => Done)
    }

  }
}
