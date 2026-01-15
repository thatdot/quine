package com.thatdot.api.v2

import io.circe.generic.extras.{Configuration => CirceConfig}
import sttp.tapir.generic.{Configuration => TapirConfig}

/** Provides unified Tapir/Circe configuration with `type` discriminator for ADTs.
  *
  * Usage: `import com.thatdot.api.v2.TypeDiscriminatorConfig.instances._`
  */
trait TypeDiscriminatorConfig {
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
