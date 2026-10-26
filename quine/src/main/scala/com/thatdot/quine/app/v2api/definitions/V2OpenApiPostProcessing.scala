package com.thatdot.quine.app.v2api.definitions

import sttp.apispec.openapi.OpenAPI

import com.thatdot.api.v2.ProductVersion

/** Post-processing that turns the raw tapir-derived OpenAPI into a product's publishable
  * V2 spec. Every builder of a published document (OSS / Enterprise / Novelty
  * generators and the live `/openapi.json` endpoint) runs this as its final step:
  *   - [[CustomMethod.rewriteOpenAPI]] — rewrite AIP-136 colon-verb path templates
  *   - [[experimentalSchemas]] — drop experimental ingest sources
  *   - [[ProductApiVisibility.show]] — drop what belongs to other products
  */
object V2OpenApiPostProcessing {

  /** Component-schema names of experimental ingest sources. They are wired into the API, so they can be created and
    * run, but are not yet ready to advertise to users: the runtime still accepts them, and only the published spec
    * leaves them out (along with schemas only they reach; see [[ApiSchemaPruning]]).
    */
  val experimentalSchemas: Set[String] = Set("DeltaSharingCdf")

  def apply(raw: OpenAPI, product: ProductVersion): OpenAPI = {
    val withoutExperimental = ApiSchemaPruning.prune(CustomMethod.rewriteOpenAPI(raw), experimentalSchemas, Map.empty)
    ProductApiVisibility.show(withoutExperimental, product)
  }
}
