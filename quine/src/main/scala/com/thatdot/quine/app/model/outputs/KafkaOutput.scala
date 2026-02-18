package com.thatdot.quine.app.model.outputs

import org.apache.pekko.NotUsed
import org.apache.pekko.kafka.scaladsl.{Producer => KafkaProducer}
import org.apache.pekko.kafka.{ProducerMessage, ProducerSettings}
import org.apache.pekko.stream.scaladsl.Flow

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer

import com.thatdot.common.logging.Log.{LazySafeLogging, LogConfig, Safe, SafeLoggableInterpolator}
import com.thatdot.common.security.Secret
import com.thatdot.quine.app.StandingQueryResultOutput.serialized
import com.thatdot.quine.graph.MasterStream.SqResultsExecToken
import com.thatdot.quine.graph.{CypherOpsGraph, NamespaceId, StandingQueryResult}
import com.thatdot.quine.routes.StandingQueryResultOutputUserDef.WriteToKafka
import com.thatdot.quine.routes.{SaslJaasConfig, StandingQueryResultOutputUserDef}
import com.thatdot.quine.serialization.ProtobufSchemaCache
import com.thatdot.quine.util.Log.implicits._

class KafkaOutput(val config: WriteToKafka)(implicit
  private val logConfig: LogConfig,
  private val protobufSchemaCache: ProtobufSchemaCache,
) extends OutputRuntime
    with LazySafeLogging {
  import Secret.Unsafe._

  /** Log warnings for any kafkaProperties keys that will be overridden by typed Secret params. */
  private def warnOnOverriddenProperties(): Unit = {
    val typedSecretKeys: Set[String] = Set.empty ++
      config.sslKeystorePassword.map(_ => "ssl.keystore.password") ++
      config.sslTruststorePassword.map(_ => "ssl.truststore.password") ++
      config.sslKeyPassword.map(_ => "ssl.key.password") ++
      config.saslJaasConfig.map(_ => "sasl.jaas.config")

    val overriddenKeys = config.kafkaProperties.keySet.intersect(typedSecretKeys)
    overriddenKeys.foreach { key =>
      logger.warn(
        safe"Kafka property '${Safe(key)}' in kafkaProperties will be overridden by typed Secret parameter. " +
        safe"Remove '${Safe(key)}' from kafkaProperties to suppress this warning.",
      )
    }
  }

  /** Merge typed secret params into Kafka properties. Typed params take precedence. */
  private def effectiveProperties: Map[String, String] = {
    val secretProps: Map[String, String] = Map.empty ++
      config.sslKeystorePassword.map("ssl.keystore.password" -> _.unsafeValue) ++
      config.sslTruststorePassword.map("ssl.truststore.password" -> _.unsafeValue) ++
      config.sslKeyPassword.map("ssl.key.password" -> _.unsafeValue) ++
      config.saslJaasConfig.map("sasl.jaas.config" -> SaslJaasConfig.toJaasConfigString(_))

    config.kafkaProperties ++ secretProps
  }

  override def flow(
    name: String,
    inNamespace: NamespaceId,
    output: StandingQueryResultOutputUserDef,
    graph: CypherOpsGraph,
  ): Flow[StandingQueryResult, SqResultsExecToken, NotUsed] = {
    val WriteToKafka(
      topic,
      bootstrapServers,
      format,
      kafkaProperties,
      _,
      _,
      _,
      _,
      structure,
    ) = config

    warnOnOverriddenProperties()

    val token = execToken(name, inNamespace)
    val settings = ProducerSettings(
      graph.system,
      new ByteArraySerializer,
      new ByteArraySerializer,
    ).withBootstrapServers(bootstrapServers)
      .withProperties(effectiveProperties)

    // Log only non-secret kafkaProperties, not effectiveProperties
    config.saslJaasConfig.foreach(sasl =>
      logger.info(safe"Kafka SASL config: ${Safe(SaslJaasConfig.toRedactedString(sasl))}"),
    )
    logger.info(safe"Writing to kafka with properties ${Safe(kafkaProperties)}")

    serialized(name, format, graph, structure)
      .map(bytes => ProducerMessage.single(new ProducerRecord[Array[Byte], Array[Byte]](topic, bytes)))
      .via(KafkaProducer.flexiFlow(settings).named(s"sq-output-kafka-producer-for-$name"))
      .map(_ => token)
  }
}
