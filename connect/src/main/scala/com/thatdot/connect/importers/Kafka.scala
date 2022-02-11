package com.thatdot.connect.importers

import java.net.URL

import scala.compat.ExecutionContexts
import scala.concurrent.Future
import scala.concurrent.duration.{Duration, DurationInt, FiniteDuration, MILLISECONDS}
import scala.util.{Failure, Success}

import akka.kafka.scaladsl.{Committer, Consumer => KafkaConsumer}
import akka.kafka.{CommitDelivery, CommitterSettings, ConsumerSettings, Subscription => KafkaSubscription}
import akka.stream.contrib.{SwitchMode, Valve, ValveSwitch}
import akka.stream.scaladsl.{Flow, Keep, Source}

import cats.Functor
import cats.instances.future.catsStdInstancesForFuture
import cats.syntax.functor._
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.{
  AUTO_COMMIT_INTERVAL_MS_CONFIG,
  AUTO_OFFSET_RESET_CONFIG,
  ENABLE_AUTO_COMMIT_CONFIG
}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, Deserializer}

import com.thatdot.connect.KafkaControl
import com.thatdot.connect.importers.serialization.{ImportFormat, Protobuf}
import com.thatdot.connect.routes.IngestMeter
import com.thatdot.quine.graph.CypherOpsGraph
import com.thatdot.quine.graph.MasterStream.{IngestSrcExecToken, IngestSrcType}
import com.thatdot.quine.routes.{KafkaAutoOffsetReset, KafkaOffsetCommitting, KafkaSecurityProtocol}

/** Formats that Quine can ingest from Kafka */
sealed trait KafkaImportFormat extends ImportFormat with LazyLogging {

  type K = Array[Byte]
  type V = Option[Deserialized]

  /** Wraps the [[ImportFormat.importBytes]] into a Kafka [[Deserializer]]
    * which may be used by alpakka.
    */
  def deserializer(isSingleHost: Boolean): Deserializer[Option[Deserialized]] = new Deserializer[Option[Deserialized]] {

    /** Attempt to deserialize bytes to a [[Deserialized]] or return None
      * @param topic The kafka topic
      * @param data The data to import off the kafka stream
      * @return a Some[Deserialized] if and only deserialization succeeds, otherwise, None
      */
    override def deserialize(topic: String, data: Array[Byte]): Option[Deserialized] =
      importMessageSafeBytes(data, isSingleHost) match {
        case Success(a) => Some(a)
        case Failure(err) =>
          logger.warn(s"Deserialization error while reading from Kafka topic $topic", err)
          None
      }
  }

  /** Stream in from Kafka creation queries in binary or JSON format. Each
    * record is interpreted as a creation query (eg [[DomainGraphBranch]])
    *
    * @param subscription the topic, offset, partition information Kafka needs
    * @param bootstrapServers comma-separated list of host/port pairs
    * @param groupId consumer group this consumer belongs to
    * @param parallelism number of records be creating in parallel
    * @return a future to watch for crashes and a handle that can be used to
    *         cancel the Kafka ingest
    */
  def importFromKafka(
    subscription: KafkaSubscription,
    bootstrapServers: String,
    groupId: String,
    meter: IngestMeter,
    initialSwitchMode: SwitchMode,
    parallelism: Int = 2,
    securityProtocol: KafkaSecurityProtocol,
    offsetCommitting: Option[KafkaOffsetCommitting],
    autoOffsetReset: KafkaAutoOffsetReset,
    endingOffset: Option[Long],
    maxPerSecond: Option[Int]
  )(implicit
    graph: CypherOpsGraph
  ): IngestSrcType[KafkaControl] = {
    val system = graph.system

    // For the .as method on Futures
    // Constructed explicitly so we can explicitly specify it use the (non-implicit) parasitic ExecutionContext
    implicit val futureFunctor: Functor[Future] = catsStdInstancesForFuture(ExecutionContexts.parasitic)

    val isSingleHost = graph.isSingleHost
    val keyDeserializer = new ByteArrayDeserializer()
    val consumerSettings = ConsumerSettings(system, keyDeserializer, deserializer(isSingleHost))
      .withBootstrapServers(bootstrapServers)
      .withGroupId(groupId)
      // Note: The ConsumerSettings stop-timeout delays stopping the Kafka Consumer and the stream, but when using drainAndShutdown that delay is not required and can be set to zero (as below).
      // https://doc.akka.io/docs/alpakka-kafka/current/consumer.html#draining-control
      // We're calling .drainAndShutdown on the Kafka [[Consumer.Control]]
      .withStopTimeout(Duration.Zero)
      .withProperties(
        AUTO_OFFSET_RESET_CONFIG -> autoOffsetReset.name,
        SECURITY_PROTOCOL_CONFIG -> securityProtocol.name
      )

    def throttled[A] = maxPerSecond match {
      case None => Flow[A]
      case Some(perSec) => Flow[A].throttle(perSec, 1.second)
    }

    def streamSource[A](
      kafkaConsumer: Source[A, KafkaConsumer.Control]
    )(toRecord: A => ConsumerRecord[K, V]): Source[A, (KafkaConsumer.Control, Future[ValveSwitch])] = endingOffset
      .fold(kafkaConsumer)(o => kafkaConsumer.takeWhile(r => toRecord(r).offset <= o))
      .viaMat(Valve(initialSwitchMode))(Keep.both)
      .wireTap(rec => meter.mark(toRecord(rec).serializedValueSize()))
      .via(throttled)
      .via(graph.ingestThrottleFlow)

    val ingestToken = IngestSrcExecToken(label)

    def nonCommitingFlow(
      source: Source[ConsumerRecord[K, V], (KafkaConsumer.Control, Future[ValveSwitch])]
    ): Source[IngestSrcExecToken, (KafkaConsumer.Control, Future[ValveSwitch])] =
      source.mapAsyncUnordered(parallelism)(record =>
        record.value match {
          case Some(value) => writeToGraph(graph, value) as ingestToken
          case None => Future.successful(ingestToken)
        }
      )

    val ingestStream: Source[IngestSrcExecToken, (KafkaConsumer.Control, Future[ValveSwitch])] =
      offsetCommitting match {
        case Some(KafkaOffsetCommitting.AutoCommit(commitIntervalMs)) =>
          val kafkaConsumer = KafkaConsumer.plainSource(
            consumerSettings.withProperties(
              ENABLE_AUTO_COMMIT_CONFIG -> "true",
              AUTO_COMMIT_INTERVAL_MS_CONFIG -> commitIntervalMs.toString
            ),
            subscription
          )
          nonCommitingFlow(streamSource(kafkaConsumer)(identity))
        case Some(
              KafkaOffsetCommitting.ExplicitCommit(
                maxBatch,
                maxIntervalMillis,
                commitParallelism,
                waitForCommitConfirmation
              )
            ) =>
          val kafkaConsumer = KafkaConsumer.committableSource(consumerSettings, subscription)
          streamSource(kafkaConsumer)(_.record)
            .mapAsync(parallelism)(record =>
              record.record.value match {
                case Some(value) => writeToGraph(graph, value) as record.committableOffset
                case None => Future.successful(record.committableOffset)
              }
            )
            .via(
              Committer.batchFlow(
                CommitterSettings(system)
                  .withMaxBatch(maxBatch)
                  .withMaxInterval(FiniteDuration(maxIntervalMillis.toLong, MILLISECONDS))
                  .withParallelism(commitParallelism)
                  .withDelivery(
                    if (waitForCommitConfirmation) CommitDelivery.WaitForAck else CommitDelivery.SendAndForget
                  )
              )
            )
            // Emits one ingest token per batch committed
            .map(_ => ingestToken)
        case None =>
          val kafkaConsumer = KafkaConsumer.plainSource(consumerSettings, subscription)
          nonCommitingFlow(streamSource(kafkaConsumer)(identity))
      }

    ingestStream
      .watchTermination() { case ((killSwitch, b), termSignal) =>
        b.map(valveHandle => new KafkaControl(killSwitch, valveHandle, termSignal))(ExecutionContexts.parasitic)
      }
  }
}

object KafkaImportFormat {

  /** Create using a cypher query, expecting the records to be JSON */
  final case class CypherJson(query: String, parameter: String)
      extends ImportFormat.CypherJson(query, parameter)
      with KafkaImportFormat

  /** Create using a cypher query, treating the records as byte arrays.
    * @param query
    * @param parameter
    */
  final case class CypherRaw(query: String, parameter: String)
      extends ImportFormat.CypherRaw(query, parameter)
      with KafkaImportFormat

  case object Drop extends ImportFormat.TestOnlyDrop with KafkaImportFormat

  final case class CypherProtobuf(query: String, parameter: String, schemaUrl: URL, typeName: String)
      extends Protobuf(query, parameter, schemaUrl, typeName)
      with KafkaImportFormat
}
