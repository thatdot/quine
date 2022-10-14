package com.thatdot.quine.app.ingest

import scala.util.Try

import akka.stream.contrib.SwitchMode
import akka.stream.scaladsl.{Flow, Source}
import akka.{Done, NotUsed}

import com.sksamuel.pulsar4s._
import com.sksamuel.pulsar4s.akka.streams.{source => pulsarSource}
import org.apache.pulsar.client.api.{Schema, SubscriptionType}

import com.thatdot.quine.app.ingest.serialization.ImportFormat
import com.thatdot.quine.app.{PulsarKillSwitch, ShutdownSwitch}
import com.thatdot.quine.graph.CypherOpsGraph
import com.thatdot.quine.graph.cypher.Value
import com.thatdot.quine.routes.PulsarSubscriptionType

/** Note that this is a minimal implementation. Pulsar client config has a number of additional
  * parameters for future expansion, e.g. authentication, parallelism, threading...
  */
case class PulsarSrcDef(
  override val name: String,
  pulsarUrl: String,
  topics: Seq[String],
  subscriptionName: String,
  subscriptionType: PulsarSubscriptionType,
  format: ImportFormat,
  initialSwitchMode: SwitchMode,
  parallelism: Int = 2,
  maxPerSecond: Option[Int]
)(implicit graph: CypherOpsGraph)
    extends RawValuesIngestSrcDef(
      format,
      initialSwitchMode,
      parallelism,
      maxPerSecond,
      s"pulsar"
    ) {
  type InputType = ConsumerMessage[Array[Byte]]

  /** Translate to java library subscription type */
  val nativeSubscriptionType: Option[SubscriptionType] =
    Try(SubscriptionType.valueOf(subscriptionType.toString)).toOption

  implicit val schema: Schema[Array[Byte]] = Schema.BYTES

  val consumer: Consumer[Array[Byte]] = {
    val subscription: Subscription = Subscription(subscriptionName)

    val client: PulsarAsyncClient = PulsarClient(PulsarClientConfig(pulsarUrl))

    val consumerConfig: ConsumerConfig =
      ConsumerConfig(subscription, topics.map(Topic), subscriptionType = nativeSubscriptionType)
    graph.system.registerOnTermination(client.consumer(consumerConfig))
    client.consumer(consumerConfig)
  }

  /** Define a way to extract raw bytes from a single input event */
  def rawBytes(value: ConsumerMessage[Array[Byte]]): Array[Byte] = value.value

  /** As Pulsar's shutdown switch is materialized by its Source, those stages
    * are inseperable and thus implemented in [[sourceWithShutdown]].
    * This implementation of `source()` is a placeholder and is unused.
    */
  def source(): Source[ConsumerMessage[Array[Byte]], NotUsed] = Source.never

  override def sourceWithShutdown(): Source[(Try[Value], ConsumerMessage[Array[Byte]]), ShutdownSwitch] =
    pulsarSource(() => consumer, Some(MessageId.earliest))
      .mapMaterializedValue(c => PulsarKillSwitch(c, graph.materializer.executionContext))
      .via(deserializeAndMeter)

  override val ack: Flow[TryDeserialized, Done, NotUsed] = Flow[TryDeserialized]
    .mapAsync(parallelism) { m =>
      consumer.acknowledgeAsync(m._2.messageId)
    }
    .map(_ => Done)

}
