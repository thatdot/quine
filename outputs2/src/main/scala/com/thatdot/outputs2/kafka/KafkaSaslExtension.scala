package com.thatdot.outputs2.kafka

/** A pluggable extension that contributes additional Kafka client properties beyond
  * `sasl.jaas.config` for a particular SASL/auth scenario.
  *
  * The canonical use is `sasl.login.callback.handler.class` for OAuth Bearer auth methods
  * that need a custom token-acquisition handler (e.g. cert-based `private_key_jwt` against
  * AD FS, which has no out-of-the-box Kafka support).
  *
  * Implementations live wherever the handler class lives — typically in an Enterprise-only
  * module — so they can use `classOf[Handler].getName` for compile-time-safe FQCN strings
  * instead of hardcoding them across module boundaries.
  */
trait KafkaSaslExtension {

  /** Additional Kafka client properties to merge into the consumer/producer config map. */
  def additionalProperties: Map[String, String]
}

/** Strategy for selecting a [[KafkaSaslExtension]] based on a SASL/JAAS config value.
  *
  * Parameterized on the SASL config type because Quine has two parallel `SaslJaasConfig`
  * representations (`api.v2` for the V2 API surface, `outputs2` for the internal model);
  * a single provider implementation can serve both by being instantiated with each type.
  */
trait KafkaExtensionProvider[C] {

  /** Return the extension to use for this config value, or `None` if no extension applies. */
  def saslExtensionFor(config: C): Option[KafkaSaslExtension]
}

object KafkaExtensionProvider {

  /** Provider that returns no extension for any input. Used by OSS code paths and tests
    * that don't need Enterprise auth extensions.
    */
  def empty[C]: KafkaExtensionProvider[C] = (_: C) => None
}
