package com.thatdot.quine.app.config

/** Safe, non-secret projection of the running config -- the only config data exposed over the API.
  * The v1 and v2 APIs each mirror this shape in their own wire types (`SystemConfigViewV1`,
  * `SystemConfigView`) and map from it field-for-field. A config option is exposed only by being
  * added here, so new options stay private by default.
  */
final case class SystemConfigSummary(
  persistor: PersistorSummary,
  webserver: Option[WebserverSummary],
  shardCount: Option[Int],
  inMemorySoftNodeLimit: Option[Int],
  inMemoryHardNodeLimit: Option[Int],
  metricsReporterTypes: List[String],
  defaultApiVersion: String,
  cluster: Option[ClusterFormationSummary],
  bolt: Option[BoltEndpointSummary],
)

final case class PersistorSummary(persistorType: String, isLocal: Boolean)
object PersistorSummary {
  def of(store: PersistenceAgentType): PersistorSummary = PersistorSummary(store.label, store.isLocal)
}

final case class WebserverSummary(address: String, port: Int, enabled: Boolean, useTls: Boolean)
object WebserverSummary {
  def of(webserver: WebServerBindConfig): WebserverSummary =
    WebserverSummary(webserver.address.asString, webserver.port.asInt, webserver.enabled, webserver.useTls)
}

/** Cluster formation settings (Quine Enterprise only). Kept as primitives (rather than depending on
  * `ClusterFormationConfig` directly) since that type lives in `quine-core-plus`, a module that
  * depends on this one.
  */
final case class ClusterFormationSummary(name: String, targetSize: Int, shardsPerMember: Int)

/** Bolt endpoint settings (Quine Enterprise only). Kept as primitives (rather than depending on
  * `BoltConfig` directly) for the same layering reason as `ClusterFormationSummary`.
  */
final case class BoltEndpointSummary(enabled: Boolean, address: String, port: Int, encryption: String)
