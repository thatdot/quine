package com.thatdot.quine.app.ingest.util

import cats.data.NonEmptyList
import com.typesafe.scalalogging.LazyLogging

import com.thatdot.quine.app.ingest.util.KafkaSettingsValidator.ErrorString
import com.thatdot.quine.routes.KafkaIngest

case class KafkaSettingsValidator(ingest: KafkaIngest) extends LazyLogging {

  private val props = ingest.kafkaProperties

  private def member(values: Set[String]): String => Option[ErrorString] =
    s =>
      values.contains(s) match {
        case true => None
        case _ => Some(s"""Illegal value $s must be one of ${values.mkString(",")}""")
      }

  /** Variables that have analogues in kafka properties. Settings in both properties
    *  and the direct setting via the api should generate errors.
    */
  private def findConflict(
    keys: Set[String],
    ingestField: Option[_]
  ): Option[ErrorString] = ingestField match {
    case Some(_) =>
      val usedKeys: Set[ErrorString] = props.keySet.filter(keys.contains)
      if (usedKeys.nonEmpty) Some(f"Property value conflicts with property ${usedKeys.mkString(",")}") else None
    case _ => None
  }

  /** Field conflicts with an explicitly set property on the ingest */
  private def disallowField(key: String, errorString: String): Option[ErrorString] =
    if (props.keySet.contains(key)) Some(s"$key is not allowed in the kafkaProperties Map. $errorString") else None

  /** Validate the property value if it exists */
  private def validateProperty(value: String, validator: String => Option[ErrorString]): Option[ErrorString] =
    props.get(value).flatMap(validator.apply)

  /** These are derived from org.apache.kafka.clients.consumer.ConsumerConfig values.
    * See https://kafka.apache.org/31/javadoc/org/apache/kafka/clients/consumer/ConsumerConfig.html
    */
  private val validKeys = Set(
    "allow.auto.create.topics",
    "auto.commit.interval.ms",
    "auto.offset.reset",
    "bootstrap.servers",
    "check.crcs",
    "client.dns.lookup",
    "client.id",
    "client.rack",
    "config.providers",
    "connections.max.idle.ms",
    "default.api.timeout.ms",
    "enable.auto.commit",
    "exclude.internal.topics",
    "fetch.max.bytes",
    "fetch.max.wait.ms",
    "fetch.min.bytes",
    "group.id",
    "group.instance.id",
    "heartbeat.interval.ms",
    "interceptor.classes",
    "isolation.level",
    "key.deserializer",
    "max.partition.fetch.bytes",
    "max.poll.interval.ms",
    "max.poll.records",
    "metadata.max.age.ms",
    "metrics.num.samples",
    "metrics.recording.level",
    "metrics.sample.window.ms",
    "metric.reporters",
    "partition.assignment.strategy",
    "receive.buffer.bytes",
    "reconnect.backoff.max.ms",
    "reconnect.backoff.ms",
    "request.timeout.ms",
    "retry.backoff.ms",
    "security.providers",
    "send.buffer.bytes",
    "session.timeout.ms",
    "socket.connection.setup.timeout.max.ms",
    "socket.connection.setup.timeout.ms",
    "value.deserializer"
  )

  /** Will return error strings or None. Will not return an empty sequence. */
  def validate: Option[NonEmptyList[ErrorString]] = {
    /*
      these values have no direct analogues in Kafka settings:

      - parallelism: Int
       - ingest.topics
       - ingest.format

     */

    val errors = List(
      findConflict(Set("group.id"), ingest.groupId),
      findConflict(Set("enable.auto.commit", "auto.commit.interval.ms"), Some(ingest.offsetCommitting)),
      //boostrap servers is mandatory on ingest. If it is set in properties that's a conflict
      disallowField("bootstrap.servers", "Please use the Kafka ingest 'bootstrapServers' field."),
      disallowField("value.deserializer", "Please use one of the 'format' field cypher options."),
      //properties for which we can test valid values
      validateProperty("auto.offset.reset", member(Set("earliest", "latest", "none"))),
      props.keySet.diff(validKeys) match {
        case s if s.isEmpty => None
        case s @ _ =>
          logger.warn(s"Ingest fails validation with errors ${s.mkString(",")}");
          Some(s"Unrecognized properties: ${s.mkString(",")}")
      }
    ) collect { case Some(c) => c }

    NonEmptyList.fromList(errors)
  }

}

object KafkaSettingsValidator {
  type ErrorString = String
}
