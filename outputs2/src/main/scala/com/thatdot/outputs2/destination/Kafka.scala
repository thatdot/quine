package com.thatdot.outputs2.destination

import scala.annotation.unused

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.kafka.scaladsl.{Producer => KafkaProducer}
import org.apache.pekko.kafka.{ProducerMessage, ProducerSettings}
import org.apache.pekko.stream.scaladsl.{Flow, Sink}

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer

import com.thatdot.common.logging.Log
import com.thatdot.common.logging.Log.{LazySafeLogging, Safe, SafeLoggableInterpolator}
import com.thatdot.common.security.Secret
import com.thatdot.outputs2.{ResultDestination, SaslJaasConfig}
import com.thatdot.quine.graph.NamespaceId
import com.thatdot.quine.util.Log.implicits._

final case class Kafka(
  topic: String,
  bootstrapServers: String,
  sslKeystorePassword: Option[Secret] = None,
  sslTruststorePassword: Option[Secret] = None,
  sslKeyPassword: Option[Secret] = None,
  saslJaasConfig: Option[SaslJaasConfig] = None,
  kafkaProperties: Map[String, String] = Map.empty,
)(implicit system: ActorSystem)
    extends ResultDestination.Bytes.Kafka
    with LazySafeLogging {
  import Secret.Unsafe._

  override def slug: String = "kafka"

  /** Log warnings for any kafkaProperties keys that will be overridden by typed Secret params. */
  private def warnOnOverriddenProperties()(implicit @unused logConfig: Log.LogConfig): Unit = {
    val typedSecretKeys: Set[String] = Set.empty ++
      sslKeystorePassword.map(_ => "ssl.keystore.password") ++
      sslTruststorePassword.map(_ => "ssl.truststore.password") ++
      sslKeyPassword.map(_ => "ssl.key.password") ++
      saslJaasConfig.map(_ => "sasl.jaas.config")

    val overriddenKeys = kafkaProperties.keySet.intersect(typedSecretKeys)
    overriddenKeys.foreach { key =>
      logger.warn(
        safe"Kafka property '${Safe(key)}' in kafkaProperties will be overridden by typed Secret parameter. " +
        safe"Remove '${Safe(key)}' from kafkaProperties to suppress this warning.",
      )
    }
  }

  /** Merge typed secret params into Kafka properties. Typed params take precedence. */
  private[destination] def effectiveProperties: Map[String, String] = {
    val secretProps: Map[String, String] = Map.empty ++
      sslKeystorePassword.map("ssl.keystore.password" -> _.unsafeValue) ++
      sslTruststorePassword.map("ssl.truststore.password" -> _.unsafeValue) ++
      sslKeyPassword.map("ssl.key.password" -> _.unsafeValue) ++
      saslJaasConfig.map("sasl.jaas.config" -> SaslJaasConfig.toJaasConfigString(_))

    kafkaProperties ++ secretProps
  }

  override def sink(name: String, inNamespace: NamespaceId)(implicit
    logConfig: Log.LogConfig,
  ): Sink[Array[Byte], NotUsed] = {

    warnOnOverriddenProperties()

    val settings = ProducerSettings(
      system,
      new ByteArraySerializer,
      new ByteArraySerializer,
    ).withBootstrapServers(bootstrapServers)
      .withProperties(effectiveProperties)

    saslJaasConfig.foreach(config => logger.info(safe"Kafka SASL config: $config"))
    logger.info(safe"Writing to kafka with properties ${Safe(kafkaProperties)}")

    Flow[Array[Byte]]
      .map { bytes =>
        ProducerMessage
          .single(new ProducerRecord[Array[Byte], Array[Byte]](topic, bytes))
      }
      .via(KafkaProducer.flexiFlow(settings).named(sinkName(name)))
      .to(Sink.ignore)
  }
}
