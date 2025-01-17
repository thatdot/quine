package com.thatdot.quine.app.outputs

import org.apache.pekko.NotUsed
import org.apache.pekko.kafka.scaladsl.{Producer => KafkaProducer}
import org.apache.pekko.kafka.{ProducerMessage, ProducerSettings}
import org.apache.pekko.stream.scaladsl.Flow

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer

import com.thatdot.common.logging.Log.{LazySafeLogging, LogConfig, Safe, SafeLoggableInterpolator}
import com.thatdot.quine.app.StandingQueryResultOutput.serialized
import com.thatdot.quine.app.serialization.ProtobufSchemaCache
import com.thatdot.quine.graph.MasterStream.SqResultsExecToken
import com.thatdot.quine.graph.{CypherOpsGraph, NamespaceId, StandingQueryResult}
import com.thatdot.quine.routes.StandingQueryResultOutputUserDef
import com.thatdot.quine.routes.StandingQueryResultOutputUserDef.WriteToKafka
import com.thatdot.quine.util.Log.implicits._
class KafkaOutput(val config: WriteToKafka)(implicit
  private val logConfig: LogConfig,
  private val protobufSchemaCache: ProtobufSchemaCache,
) extends OutputRuntime
    with LazySafeLogging {
  override def flow(
    name: String,
    inNamespace: NamespaceId,
    output: StandingQueryResultOutputUserDef,
    graph: CypherOpsGraph,
  ): Flow[StandingQueryResult, SqResultsExecToken, NotUsed] = {
    val WriteToKafka(topic, bootstrapServers, format, properties, structure) = config
    val token = execToken(name, inNamespace)
    val settings = ProducerSettings(
      graph.system,
      new ByteArraySerializer,
      new ByteArraySerializer,
    ).withBootstrapServers(bootstrapServers)
      .withProperties(properties)
    logger.info(safe"Writing to kafka with properties ${Safe(properties)}")
    serialized(name, format, graph, structure)
      .map(bytes => ProducerMessage.single(new ProducerRecord[Array[Byte], Array[Byte]](topic, bytes)))
      .via(KafkaProducer.flexiFlow(settings).named(s"sq-output-kafka-producer-for-$name"))
      .map(_ => token)
  }
}
