package com.thatdot.api.v2.schema

import io.circe.Printer
import sttp.tapir.json.circe.TapirJsonCirce

/** Provides `jsonBody[T]` for endpoint definitions, using overridden settings. */
trait TapirJsonConfig extends TapirJsonCirce {
  override def jsonPrinter: Printer = TapirJsonConfig.printer
}

object TapirJsonConfig extends TapirJsonConfig {

  /** Circe JSON printer that will
    * - Drop null values from output JSON
    * - Use no indentation (compact output)
    */
  private val printer: Printer = Printer(dropNullValues = true, indent = "")
}
