package com.thatdot.api.v2

import io.circe.generic.extras.{Configuration => CirceConfig}
import sttp.tapir.generic.{Configuration => TapirConfig}

/** Unified configuration for Tapir Schema and Circe codec derivation.
  *
  * Tapir and Circe each have their own `Configuration` types:
  *   - `sttp.tapir.generic.Configuration` controls Tapir's `Schema.derived` macro
  *   - `io.circe.generic.extras.Configuration` controls Circe's `deriveConfiguredEncoder`/`deriveConfiguredDecoder`
  *
  * This class provides a single source of truth for settings like discriminator field names and
  * constructor renaming. This ensures the OpenAPI schema documentation matches the runtime JSON
  * serialization behavior.
  *
  * @param discriminator Optional field name for ADT type discriminators (e.g., "type")
  * @param renameConstructors Map from Scala constructor names to JSON discriminator values
  */
case class TapirCirceUnifiedConfig(discriminator: Option[String], renameConstructors: Map[String, String]) {
  def asTapir: TapirConfig =
    discriminator
      .fold(TapirConfig.default)(d => TapirConfig.default.withDiscriminator(d))
      .copy(
        toDiscriminatorValue = { s =>
          val className = TapirConfig.default.toDiscriminatorValue(s)
          renameConstructors.getOrElse(className, className)
        },
      )
  def asCirce: CirceConfig =
    discriminator
      .fold(CirceConfig.default)(d => CirceConfig.default.withDiscriminator(d))
      .copy(
        transformConstructorNames = s => renameConstructors.getOrElse(s, s),
      )
      .withDefaults

  def withDiscriminator(d: String): TapirCirceUnifiedConfig =
    copy(discriminator = Some(d))

  def renameConstructor(from: String, to: String): TapirCirceUnifiedConfig =
    copy(renameConstructors = renameConstructors + (from -> to))

}

object TapirCirceUnifiedConfig {
  val default: TapirCirceUnifiedConfig = TapirCirceUnifiedConfig(None, Map.empty)
}
