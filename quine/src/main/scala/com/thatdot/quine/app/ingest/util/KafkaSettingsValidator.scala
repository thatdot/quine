package com.thatdot.quine.app.ingest.util

import java.lang.reflect.Field

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

import cats.data.NonEmptyList
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.SaslConfigs.SASL_JAAS_CONFIG

import com.thatdot.quine.app.ingest.util.KafkaSettingsValidator.{ErrorString, underlyingValidator}
import com.thatdot.quine.routes.KafkaIngest.KafkaProperties
import com.thatdot.quine.routes.KafkaOffsetCommitting

case class KafkaSettingsValidator(
  properties: KafkaProperties,
  explicitGroupId: Option[String] = None,
  explicitOffsetCommitting: Option[KafkaOffsetCommitting] = None
) extends LazyLogging {

  /** Variables that have analogues in kafka properties. Settings in both properties
    *  and the direct setting via the api should generate errors. Use this when the
    *  setting must be provided via EITHER the API or the properties object, but not
    *  both
    */
  private def findConflict(
    keys: Set[String],
    ingestField: Option[_]
  ): Option[ErrorString] = ingestField match {
    case Some(_) =>
      val usedKeys: Set[ErrorString] = properties.keySet.intersect(keys)
      if (usedKeys.nonEmpty) Some(f"Property value conflicts with property ${usedKeys.mkString(",")}") else None
    case _ => None
  }

  private def disallowSubstring(forbiddenValue: String)(key: String): Option[ErrorString] =
    if (properties.get(key).exists((userSetValue: String) => userSetValue.contains(forbiddenValue)))
      Some(s"$key may not be set to: ${properties(key)}, as it contains: $forbiddenValue")
    else None

  /** Field conflicts with an explicitly set property on the ingest. Use this when
    * the setting MUST be provided via the API
    */
  private def disallowField(key: String, errorString: String): Option[ErrorString] =
    if (properties.keySet.contains(key)) Some(s"$key is not allowed in the kafkaProperties Map. $errorString") else None

  /** Will return error strings or None.
    * If [[assumeConfigIsFinal]] is true, the properties will also be checked against kafka's internal property
    * validator (additional checks include things like verifying that values fall within enumerated options and that
    * all required fields to construct a Kafka Consumer are present)
    */
  def validate(assumeConfigIsFinal: Boolean = false): Option[NonEmptyList[ErrorString]] = {
    val underlyingKnownKeys: Set[String] = underlyingValidator match {
      case Failure(e) =>
        // Should be impossible.
        logger.error(
          s"Expected Kafka settings validator to be available at ${classOf[ConsumerConfig].getName}.CONFIG -- " +
          s"did you override your classpath with a custom kafka JAR? Kafka config validation will now fail."
        )
        throw e
      case Success(validator) => validator.configKeys.values.asScala.map(_.name).toSet
    }
    val validator: ConfigDef = underlyingValidator.get

    val unrecognizedPropertiesError: List[String] = properties.keySet.diff(underlyingKnownKeys) match {
      case s if s.isEmpty => Nil
      case s @ _ =>
        List(s"Unrecognized properties: ${s.mkString(",")}")
    }

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
          validatedConfigEntry <- validator.validate(properties.asJava).asScala
          configName = validatedConfigEntry.name()
          // TODO why does a finalized config not have key.deserializer set?
          //      Does pekko tack it on in settings.consumerFactory?
          if configName != ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG
          if configName != ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG
          err: ErrorString <- validatedConfigEntry.errorMessages.asScala
        } yield s"Error in Kafka setting $configName: $err"
      } else {
        // config is not yet merged (multiple sources of truth), so we can look for conflicts between the parts of config
        (
          List(
            findConflict(Set(CommonClientConfigs.GROUP_ID_CONFIG), explicitGroupId),
            findConflict(
              Set(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG),
              Some(explicitOffsetCommitting)
            ),
            //boostrap servers is mandatory on ingest. If it is set in properties that's a conflict
            disallowField(
              CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
              "Please use the Kafka ingest `bootstrapServers` field."
            ),
            disallowField(
              ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
              "Please use one of the `format` field cypher options, which rely on their hard-coded deserializers."
            ),
            disallowField(
              ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
              "Please use one of the `format` field cypher options, which rely on their hard-coded deserializers."
            ),
            disallowField(
              CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
              "Please use the Kafka ingest `securityProtocol` field."
            ),
            disallowField(
              ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
              "Please use the Kafka ingest `autoOffsetReset` field."
            )
          ) ::: { // Conservative fix for CVE-2023-25194: disable keys including ${SaslConfigs.SASL_JAAS_CONFIG}
            val forbiddenJaasModule = "com.sun.security.auth.module.JndiLoginModule"
            (disallowSubstring(forbiddenJaasModule)(SASL_JAAS_CONFIG) :: List(
              // these 3 config scopes may allow "overrides" -- the security advisory at https://archive.ph/P6q2A
              // recommends blacklisting the `override` subkey for each scope. These are already considered
              // invalid by `unrecognizedProperties`, but better safe than sorry.
              "producer",
              "consumer",
              "admin"
            )
              .map(scope => s"$scope.override.$SASL_JAAS_CONFIG")
              .map(disallowSubstring(forbiddenJaasModule)))
          }
        ).flatten
      }

    NonEmptyList.fromList(unrecognizedPropertiesError ++ errors)
  }

}

object KafkaSettingsValidator {
  type ErrorString = String

  // Reflectively load the ConfigDef at ConsumerConfig.CONFIG, which is what Kafka uses for validation
  val underlyingValidator: Try[ConfigDef] = Try {
    val consumerConfig: Field = classOf[ConsumerConfig].getDeclaredField("CONFIG")
    consumerConfig.setAccessible(true)
    consumerConfig.get(null).asInstanceOf[ConfigDef]
  }
}
