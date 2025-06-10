package com.thatdot.model.v2.outputs.destination

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.kafka.scaladsl.{Producer => KafkaProducer}
import org.apache.pekko.kafka.{ProducerMessage, ProducerSettings}
import org.apache.pekko.stream.scaladsl.{Flow, Sink}

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer

import com.thatdot.common.logging.Log
import com.thatdot.common.logging.Log.{LazySafeLogging, Safe, SafeLoggableInterpolator}
import com.thatdot.model.v2.outputs.ResultDestination
import com.thatdot.quine.graph.NamespaceId
import com.thatdot.quine.util.Log.implicits._

final case class Kafka(
  topic: String,
  bootstrapServers: String,
  kafkaProperties: Map[String, String],
)(implicit system: ActorSystem)
    extends ResultDestination.Bytes.Kafka
    with LazySafeLogging {
  override def sink(name: String, inNamespace: NamespaceId)(implicit
    logConfig: Log.LogConfig,
  ): Sink[Array[Byte], NotUsed] = {

    val settings = ProducerSettings(
      system,
      new ByteArraySerializer,
      new ByteArraySerializer,
    ).withBootstrapServers(bootstrapServers)
      .withProperties(kafkaProperties)
    logger.info(safe"Writing to kafka with properties ${Safe(kafkaProperties)}")

    Flow[Array[Byte]]
      .map { bytes =>
        ProducerMessage
          .single(new ProducerRecord[Array[Byte], Array[Byte]](topic, bytes))
      }
      .via(KafkaProducer.flexiFlow(settings).named(sinkName(name)))
      .to(Sink.ignore)
  }

  private def sinkName(name: String): String = s"result-destination--kafka--$name"
}
