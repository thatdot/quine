package com.thatdot.quine.app.v2api.definitions

import io.circe.parser.decode
import sttp.apispec.openapi.OpenAPI
import sttp.apispec.{Schema, SchemaLike}

import com.thatdot.api.v2.{ProductSpecific, ProductVersion}

/** Shows each product its own API: drops from a product's published OpenAPI document every component schema and
  * property marked (see [[ProductSpecific.only]]) as belonging to other products, along with what only they reach (see
  * [[ApiSchemaPruning]]), and then removes the marks from what remains.
  *
  * Marks render as the [[ProductSpecific.docsExtensionKey]] extension on the component schema of a marked type or
  * variant, and on the property of a marked field.
  */
object ProductApiVisibility {

  def show(api: OpenAPI, product: ProductVersion): OpenAPI = {
    val schemas = api.components.fold(Map.empty[String, SchemaLike])(_.schemas)
    def excluded(s: SchemaLike): Boolean = s match {
      case schema: Schema => marksOf(schema).exists(!_.contains(product.slug))
      case _ => false
    }
    val hiddenSchemas = schemas.collect { case (name, s) if excluded(s) => name }.toSet
    val hiddenProperties = schemas.collect {
      case (name, s: Schema) if s.properties.values.exists(excluded) =>
        name -> s.properties.collect { case (property, p) if excluded(p) => property }.toSet
    }
    unmark(ApiSchemaPruning.prune(api, hiddenSchemas, hiddenProperties))
  }

  private def marksOf(s: Schema): Option[Set[String]] =
    s.extensions.get(ProductSpecific.docsExtensionKey).map { mark =>
      decode[List[String]](mark.value).fold(
        e => throw new IllegalStateException(s"Malformed ${ProductSpecific.docsExtensionKey} mark: ${mark.value}", e),
        _.toSet,
      )
    }

  private def unmark(api: OpenAPI): OpenAPI = {
    def strip(s: SchemaLike): SchemaLike = s match {
      case schema: Schema =>
        schema.copy(
          extensions = schema.extensions - ProductSpecific.docsExtensionKey,
          properties = schema.properties.map { case (k, p) => k -> strip(p) },
        )
      case other => other
    }
    api.copy(components = api.components.map(c => c.copy(schemas = c.schemas.map { case (k, s) => k -> strip(s) })))
  }
}
