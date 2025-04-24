package com.thatdot.quine.app.model.ingest

import scala.concurrent.duration.{Duration, FiniteDuration, MILLISECONDS}
import scala.util.Try

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
import com.codahale.metrics.Timer
import org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, Deserializer}

import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.quine.app.KafkaKillSwitch
import com.thatdot.quine.app.model.ingest.serialization.{ContentDecoder, ImportFormat}
import com.thatdot.quine.app.model.ingest.util.KafkaSettingsValidator
import com.thatdot.quine.graph.cypher.Value
import com.thatdot.quine.graph.{CypherOpsGraph, NamespaceId}
import com.thatdot.quine.routes.{KafkaAutoOffsetReset, KafkaIngest, KafkaOffsetCommitting, KafkaSecurityProtocol}
import com.thatdot.quine.util.SwitchMode

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
    kafkaProperties: KafkaIngest.KafkaProperties,
    securityProtocol: KafkaSecurityProtocol,
    decoders: Seq[ContentDecoder],
    deserializationTimer: Timer,
  )(implicit graph: CypherOpsGraph): ConsumerSettings[Array[Byte], Try[Value]] = {

    val deserializer: Deserializer[Try[Value]] =
      (_: String, data: Array[Byte]) =>
        format.importMessageSafeBytes(ContentDecoder.decode(decoders, data), isSingleHost, deserializationTimer)

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

    ConsumerSettings(graph.system, keyDeserializer, deserializer)
      .withBootstrapServers(bootstrapServers)
      .withGroupId(groupId)
      // Note: The ConsumerSettings stop-timeout delays stopping the Kafka Consumer
      // and the stream, but when using drainAndShutdown that delay is not required and can be set to zero (as below).
      // https://pekko.apache.org/docs/pekko-connectors-kafka/current/consumer.html#draining-control
      // We're calling .drainAndShutdown on the Kafka [[Consumer.Control]]
      .withStopTimeout(Duration.Zero)
      .withProperties(properties)
  }

  def apply(
    name: String,
    intoNamespace: NamespaceId,
    topics: Either[KafkaIngest.Topics, KafkaIngest.PartitionAssignments],
    bootstrapServers: String,
    groupId: String,
    format: ImportFormat,
    initialSwitchMode: SwitchMode,
    parallelism: Int = 2,
    securityProtocol: KafkaSecurityProtocol,
    offsetCommitting: Option[KafkaOffsetCommitting],
    autoOffsetReset: KafkaAutoOffsetReset,
    kafkaProperties: KafkaIngest.KafkaProperties,
    endingOffset: Option[Long],
    maxPerSecond: Option[Int],
    decoders: Seq[ContentDecoder],
  )(implicit
    graph: CypherOpsGraph,
    logConfig: LogConfig,
  ): ValidatedNel[KafkaSettingsValidator.ErrorString, IngestSrcDef] = {
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
          ).toSet,
        ),
    )

    val consumerSettings: ConsumerSettings[Array[Byte], Try[Value]] =
      buildConsumerSettings(
        format,
        isSingleHost,
        bootstrapServers,
        groupId,
        autoOffsetReset,
        kafkaProperties,
        securityProtocol,
        decoders,
        graph.metrics.ingestDeserializationTimer(intoNamespace, name),
      )

    val complaintsFromValidator: ValidatedNel[String, Unit] =
      KafkaSettingsValidator
        .validateInput(consumerSettings.properties, assumeConfigIsFinal = true)
        .toInvalid(())

    complaintsFromValidator.map { _ =>
      offsetCommitting match {
        case None =>
          val consumer: Source[NoOffset, Consumer.Control] = Consumer.plainSource(consumerSettings, subscription)
          NonCommitting(
            name,
            intoNamespace,
            format,
            initialSwitchMode,
            parallelism,
            consumer,
            endingOffset,
            maxPerSecond,
            decoders,
          )
        case Some(koc @ KafkaOffsetCommitting.ExplicitCommit(_, _, _, _)) =>
          val consumer: Source[WithOffset, Consumer.Control] =
            Consumer.committableSource(consumerSettings, subscription)

          Committing(
            name,
            intoNamespace,
            format,
            initialSwitchMode,
            parallelism,
            consumer,
            endingOffset,
            maxPerSecond,
            koc,
            decoders,
          )
      }
    }
  }

  /** Kafka type that does not ack offset information. */
  case class NonCommitting(
    override val name: String,
    override val intoNamespace: NamespaceId,
    format: ImportFormat,
    initialSwitchMode: SwitchMode,
    parallelism: Int = 2,
    kafkaConsumer: Source[NoOffset, Consumer.Control],
    endingOffset: Option[Long],
    maxPerSecond: Option[Int],
    decoders: Seq[ContentDecoder],
  )(implicit val graph: CypherOpsGraph, val logConfig: LogConfig)
      extends IngestSrcDef(
        format,
        initialSwitchMode,
        parallelism,
        maxPerSecond,
        s"$name (Kafka ingest)",
        intoNamespace,
      ) {

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
    override val name: String,
    override val intoNamespace: NamespaceId,
    format: ImportFormat,
    initialSwitchMode: SwitchMode,
    parallelism: Int = 2,
    kafkaConsumer: Source[WithOffset, Consumer.Control],
    endingOffset: Option[Long],
    maxPerSecond: Option[Int],
    koc: KafkaOffsetCommitting.ExplicitCommit,
    decoders: Seq[ContentDecoder],
  )(implicit val graph: CypherOpsGraph, val logConfig: LogConfig)
      extends IngestSrcDef(
        format,
        initialSwitchMode,
        parallelism,
        maxPerSecond,
        s"$name (Kafka ingest)",
        intoNamespace,
      ) {
    type InputType = WithOffset
    override def sourceWithShutdown(): Source[TryDeserialized, KafkaKillSwitch] =
      endingOffset
        .fold(kafkaConsumer)(o => kafkaConsumer.takeWhile(r => r.record.offset() <= o))
        .wireTap((o: WithOffset) => meter.mark(o.record.serializedValueSize()))
        .mapMaterializedValue(KafkaKillSwitch)
        .map((o: WithOffset) => (o.record.value(), o))

    /** For ack-ing source override the default mapAsyncUnordered behavior.
      */
    override def writeToGraph(intoNamespace: NamespaceId): Flow[TryDeserialized, TryDeserialized, NotUsed] =
      Flow[TryDeserialized].mapAsync(parallelism)(writeSuccessValues(intoNamespace))

    override val ack: Flow[TryDeserialized, Done, NotUsed] = {
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
      // endingOffset set, we will get an org.apache.pekko.kafka.CommitTimeoutException here, since the commit delivery
      // is batched and it's possible to have remaining commit offsets remaining that don't get sent.
      //
      // e.g. partition holds 1000 values, we set koc.maxBatch=100, and endingOffset to 150. Last ack sent will
      // be 100, last 50 will not be sent.
      Flow[TryDeserialized]
        .map(_._2.committableOffset)
        .via(committer)
        .map(_ => Done)
    }
  }
}
