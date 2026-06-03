package com.thatdot.quine.app.v2api.definitions

import sttp.apispec.openapi.OpenAPI

/** Post-processing that turns the raw tapir-derived OpenAPI into the publishable
  * V2 spec. Every builder of a published document (OSS / Enterprise / Novelty
  * generators and the live `/openapi.json` endpoint) runs this as its final
  * step. It only composes steps that each live in their own module:
  *   - [[CustomMethod.rewriteOpenAPI]] — rewrite AIP-136 colon-verb path templates
  *   - [[ExperimentalApiVisibility.hide]] — drop experimental ingest sources
  */
object V2OpenApiPostProcessing {
  def apply(raw: OpenAPI): OpenAPI =
    ExperimentalApiVisibility.hide(CustomMethod.rewriteOpenAPI(raw))
}
