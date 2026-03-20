package com.thatdot.api.v2

import io.circe.generic.extras.{Configuration => CirceConfig}
import sttp.tapir.generic.{Configuration => TapirConfig}

/** Provides unified Tapir/Circe configuration with `type` discriminator for ADTs.
  *
  * ==Configuration Effects==
  *
  * '''Sum types (sealed traits/classes):''' The `"type"` discriminator field identifies the subtype:
  * {{{
  * {"type": "SubtypeName", "field1": "value", ...}
  * }}}
  *
  * '''Product types (case classes):''' The discriminator has no effect. Only `.withDefaults`
  * matters, enabling Scala default parameter values to be used when fields are absent from JSON.
  *
  * ==Usage==
  * {{{
  * import com.thatdot.api.v2.TypeDiscriminatorConfig.instances.circeConfig
  * // or for both Tapir and Circe:
  * import com.thatdot.api.v2.TypeDiscriminatorConfig.instances._
  * }}}
  * Extension is also possible, but not preferred.
  */
trait TypeDiscriminatorConfig {

  /** Based on [[TapirCirceUnifiedConfig.default]], which presumably adds `withDefaults`, this is the configuration that also adds the "type" discriminator. */
  implicit val config: TapirCirceUnifiedConfig = TapirCirceUnifiedConfig.default.withDiscriminator("type")
}

object TypeDiscriminatorConfig extends TypeDiscriminatorConfig {

  /** Implicit instances for circeConfig and tapirConfig.
    *
    * Usage: `import com.thatdot.api.v2.TypeDiscriminatorConfig.instances._`
    */
  object instances {
    implicit val circeConfig: CirceConfig = TypeDiscriminatorConfig.config.asCirce
    implicit val tapirConfig: TapirConfig = TypeDiscriminatorConfig.config.asTapir
  }
}
