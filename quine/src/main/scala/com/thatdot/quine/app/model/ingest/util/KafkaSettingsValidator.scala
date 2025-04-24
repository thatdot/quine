package com.thatdot.quine.app.model.ingest.util

import java.lang.reflect.Field

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

import cats.data.NonEmptyList
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.config.SaslConfigs.SASL_JAAS_CONFIG
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef, ConfigValue}

import com.thatdot.common.logging.Log._
import com.thatdot.quine.app.model.ingest.util.KafkaSettingsValidator.ErrorString
import com.thatdot.quine.routes.KafkaIngest.KafkaProperties
import com.thatdot.quine.routes.KafkaOffsetCommitting
object KafkaSettingsValidator extends LazySafeLogging {
  type ErrorString = String

  private def underlyingValidator[C <: AbstractConfig](c: Class[C]): ConfigDef = Try {
    val config: Field = c.getDeclaredField("CONFIG")
    config.setAccessible(true)
    config.get(null).asInstanceOf[ConfigDef]
  } match {
    case Failure(e) =>
      // Should be impossible.
      logger.error(
        safe"""Expected Kafka settings validator to be available at ${Safe(c.getName)}.CONFIG --
              |did you override your classpath with a custom kafka JAR? Kafka config validation
              |will now fail.""".cleanLines,
      )
      throw e
    case Success(validator) => validator
  }

  /** Will return error strings or None.
    * If [[assumeConfigIsFinal]] is true, the properties will also be checked against kafka's internal property
    * validator (additional checks include things like verifying that values fall within enumerated options and that
    * all required fields to construct a Kafka Consumer are present)
    */
  def validateInput(
    properties: KafkaProperties,
    explicitGroupId: Option[String] = None,
    explicitOffsetCommitting: Option[KafkaOffsetCommitting] = None,
    assumeConfigIsFinal: Boolean = false,
  ): Option[NonEmptyList[String]] = {
    val v = new KafkaSettingsValidator(underlyingValidator(classOf[ConsumerConfig]), properties)

    /*
          these values have no direct analogues in Kafka settings:

          - parallelism: Int
           - ingest.topics
           - ingest.format

     */

    val errors: Seq[String] =
      if (assumeConfigIsFinal) {
        // config is already merged, so we can rely on the kafka-provided validator for any errors
        for {
          validatedConfigEntry <- v.underlyingValues
          configName = validatedConfigEntry.name()
          // TODO why does a finalized config not have key.deserializer set?
          //      Does pekko tack it on in settings.consumerFactory?
          if configName != ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG
          if configName != ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG
          err: ErrorString <- validatedConfigEntry.errorMessages.asScala
        } yield s"Error in Kafka setting $configName: $err"
      } else {
        // config is not yet merged (multiple sources of truth), so we can look for conflicts between the parts of config
        List(
          v.findConflict(Set(CommonClientConfigs.GROUP_ID_CONFIG), explicitGroupId),
          v.findConflict(
            Set(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG),
            Some(explicitOffsetCommitting),
          ),
          //boostrap servers is mandatory on ingest. If it is set in properties that's a conflict
          v.disallowField(
            CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
            "Please use the Kafka ingest `bootstrapServers` field.",
          ),
          v.disallowField(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            "Please use one of the `format` field cypher options, which rely on their hard-coded deserializers.",
          ),
          v.disallowField(
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            "Please use one of the `format` field cypher options, which rely on their hard-coded deserializers.",
          ),
          v.disallowField(
            CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
            "Please use the Kafka ingest `securityProtocol` field.",
          ),
          v.disallowField(
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
            "Please use the Kafka ingest `autoOffsetReset` field.",
          ),
          //
          // --- if any of these keys points to something containing "com.sun.security.auth.module.JndiLoginModule"
          //
          // Conservative fix for CVE-2023-25194: disable keys including ${SaslConfigs.SASL_JAAS_CONFIG}
          v.disallowJaasSubstring(SASL_JAAS_CONFIG),
          // these 3 config scopes may allow "overrides" -- the security advisory at https://archive.ph/P6q2A
          // recommends blacklisting the `override` subkey for each scope. These are already considered
          // invalid by `unrecognizedProperties`, but better safe than sorry.
          v.disallowJaasSubstring(s"producer.override.$SASL_JAAS_CONFIG"),
          v.disallowJaasSubstring(s"consumer.override.$SASL_JAAS_CONFIG"),
          v.disallowJaasSubstring(s"admin.override.$SASL_JAAS_CONFIG"),
        ).flatten
      }

    v.withUnrecognizedErrors(errors)
  }

  def validateOutput(properties: KafkaProperties): Option[NonEmptyList[String]] = {
    val v = new KafkaSettingsValidator(underlyingValidator(classOf[ProducerConfig]), properties)

    val errors: Seq[ErrorString] = List(
      //boostrap servers is mandatory. If it is set in properties that's a conflict
      v.disallowField(
        CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
        "Please use the result output `bootstrapServers` field.",
      ),
      v.disallowField(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "Please use one of the `format` field cypher options, which rely on their hard-coded deserializers.",
      ),
      //
      // --- if any of these keys points to something containing "com.sun.security.auth.module.JndiLoginModule"
      //
      // Conservative fix for CVE-2023-25194: disable keys including ${SaslConfigs.SASL_JAAS_CONFIG}
      v.disallowJaasSubstring(SASL_JAAS_CONFIG),
      // these 3 config scopes may allow "overrides" -- the security advisory at https://archive.ph/P6q2A
      // recommends blacklisting the `override` subkey for each scope. These are already considered
      // invalid by `unrecognizedProperties`, but better safe than sorry.
      v.disallowJaasSubstring(s"producer.override.$SASL_JAAS_CONFIG"),
      v.disallowJaasSubstring(s"consumer.override.$SASL_JAAS_CONFIG"),
      v.disallowJaasSubstring(s"admin.override.$SASL_JAAS_CONFIG"),
    ).flatten

    v.withUnrecognizedErrors(errors)
  }
}

class KafkaSettingsValidator(
  validator: ConfigDef,
  properties: KafkaProperties,
) extends LazySafeLogging {

  private val underlyingKnownKeys: Set[String] = validator.configKeys.values.asScala.map(_.name).toSet
  def underlyingValues: Seq[ConfigValue] = validator.validate(properties.asJava).asScala.toVector

  /** Variables that have analogues in kafka properties. Settings in both properties
    * and the direct setting via the api should generate errors. Use this when the
    * setting must be provided via EITHER the API or the properties object, but not
    * both
    */
  protected def findConflict(
    keys: Set[String],
    ingestField: Option[_],
  ): Option[ErrorString] = ingestField match {
    case Some(_) =>
      val usedKeys: Set[ErrorString] = properties.keySet.intersect(keys)
      if (usedKeys.nonEmpty) Some(f"Property value conflicts with property ${usedKeys.mkString(",")}") else None
    case _ => None
  }

  protected def disallowJaasSubstring(key: String): Option[ErrorString] = {
    val forbiddenJaasModule = "com.sun.security.auth.module.JndiLoginModule"
    if (properties.get(key).exists((userSetValue: String) => userSetValue.contains(forbiddenJaasModule)))
      Some(s"$key may not be set to: ${properties(key)}, as it contains: $forbiddenJaasModule")
    else None
  }

  /** Field conflicts with an explicitly set property on the ingest. Use this when
    * the setting MUST be provided via the API
    */

  protected def disallowField(key: String, errorString: String): Option[ErrorString] =
    if (properties.keySet.contains(key)) Some(s"$key is not allowed in the kafkaProperties Map. $errorString") else None

  val unrecognizedPropertiesError: List[String] = properties.keySet.diff(underlyingKnownKeys) match {
    case s if s.isEmpty => Nil
    case s @ _ =>
      List(s"Unrecognized properties: ${s.mkString(",")}")
  }

  def withUnrecognizedErrors(errors: Seq[String]): Option[NonEmptyList[String]] =
    NonEmptyList.fromList(unrecognizedPropertiesError ++ errors)

}
