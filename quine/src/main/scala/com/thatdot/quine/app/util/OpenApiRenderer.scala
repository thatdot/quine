// Copied from https://github.com/endpoints4s/endpoints4s/blob/7045cd4cbcfd2a623b5749e9f21f20411b73377c/openapi/openapi/src/main/scala/endpoints4s/openapi/model/OpenApi.scala
// Changes we have applied intentionally are marked with "FORK DIFFERENCE" comments.
// Other changes should be kept in sync with upstream updates as time allows
// This file is excluded from scalafmt formatting in order to make diffing against the upstream code easier
package com.thatdot.quine.app.util

import endpoints4s.Encoder
import endpoints4s.algebra.{ExternalDocumentationObject, Tag}
import endpoints4s.openapi.model.{Components, In, Info, MediaType, OpenApi, Operation, Parameter, PathItem, RequestBody, Response, ResponseHeader, Schema, SecurityRequirement, SecurityScheme, Server, ServerVariable}
import io.circe.yaml.v12.syntax._
import ujson.circe.CirceJson

case class OpenApiRenderer(isEnterprise: Boolean) {
  import OpenApiRenderer._

  val openApiVersion = "3.0.0"


  // FORK DIFFERENCE: mapJson that accepts keys in function
  private def mapJsonUsingKeys[A](map: collection.Map[String, A])(f: (String, A) => ujson.Value): ujson.Obj = {
    val result = ujson.Obj()
    //preserve order defined by user or sort by key to minimize diff
    val stableMap = map match {
      case map: collection.mutable.LinkedHashMap[String, A] => map
      case map: collection.immutable.ListMap[String, A]     => map
      case map                                              => map.toSeq.sortBy(_._1)
    }
    stableMap.foreach { case (k, v) => result.value.put(k, f(k, v)) }
    result
  }
  private def mapJson[A](map: collection.Map[String, A])(f: A => ujson.Value): ujson.Obj = {
    mapJsonUsingKeys(map)((_, v) => f(v))
  }

  private[util] def schemaJson(schema: Schema): ujson.Obj = {
    val result = ujson.Obj()

    for (description <- schema.description) {
      result.value.put("description", ujson.Str(description))
    }
    for (example <- schema.example) {
      result.value.put("example", example)
    }
    for (title <- schema.title) {
      result.value.put("title", title)
    }
    for (default <- schema.default) {
      result.value.put("default", default)
    }

    schema match {
      case primitive: Schema.Primitive =>
        result.value.put("type", ujson.Str(primitive.name))
        primitive.format.foreach(s => result.value.put("format", ujson.Str(s)))
        primitive.minimum.foreach(d => result.value.put("minimum", ujson.Num(d)))
        primitive.exclusiveMinimum.foreach(b => result.value.put("exclusiveMinimum", ujson.Bool(b)))
        primitive.maximum.foreach(d => result.value.put("maximum", ujson.Num(d)))
        primitive.exclusiveMaximum.foreach(b => result.value.put("exclusiveMaximum", ujson.Bool(b)))
        primitive.multipleOf.foreach(d => result.value.put("multipleOf", ujson.Num(d)))
      case obj: Schema.Object =>
        result.value.put("type", "object")
        val properties = ujson.Obj()
        obj.properties.foreach { (p: Schema.Property) =>
                val schema = p.schema
                  .withDefinedDescription(p.description)
                  .withDefinedDefault(p.defaultValue)
          properties.value.put(p.name, schemaJson(schema))
        }
        result.value.put("properties", properties)

        val required = obj.properties.filter(_.isRequired).map(_.name)
        if (required.nonEmpty) {
          result.value.put("required", ujson.Arr.from(required))
        }
        obj.additionalProperties.foreach(p =>
          result.value.put("additionalProperties", schemaJson(p))
        )
      case array: Schema.Array =>
        result.value.put("type", "array")
        array.elementType match {
          case Left(value) =>
            result.value.put("items", schemaJson(value))
          case Right(value) =>
            // Best effort (not 100% accurate) to represent the heterogeneous array in OpenAPI 3.0
            // This should be changed with OpenAPI 3.1 and more idiomatic representation using `prefixItems`
            result.value ++= List(
              "items" -> schemaJson(
                Schema.OneOf(
                  alternatives = Schema.EnumeratedAlternatives(value),
                  description = None,
                  example = None,
                  title = None
                )
              ),
              "minItems" -> ujson.Num(value.length.toDouble),
              "maxItems" -> ujson.Num(value.length.toDouble)
            )
        }
      case enm: Schema.Enum =>
        result.value ++= schemaJson(
          enm.elementType.withDefinedDescription(enm.description)
        ).value
        result.value.put("enum", ujson.Arr.from(enm.values))
      case oneOf: Schema.OneOf =>
        result.value ++=
          (oneOf.alternatives match {
            case discAlternatives: Schema.DiscriminatedAlternatives =>
              val mapping = ujson.Obj()
              discAlternatives.alternatives.foreach {
                case (tag, ref: Schema.Reference) =>
                  mapping.value.put(tag, ujson.Str(Schema.Reference.toRefPath(ref.name)))
                case _ =>
              }
              val discriminator = ujson.Obj()
              discriminator.value += "propertyName" -> ujson.Str(
                discAlternatives.discriminatorFieldName
              )
              if (mapping.value.nonEmpty) {
                discriminator.value += "mapping" -> mapping
              }
              List(
                "oneOf" ->
                  ujson.Arr.from(discAlternatives.alternatives.map(kv => schemaJson(kv._2))),
                "discriminator" -> discriminator
              )
            case enumAlternatives: Schema.EnumeratedAlternatives =>
              List(
                "oneOf" -> ujson.Arr.from(enumAlternatives.alternatives.map(schemaJson))
              )
          })
      case allOf: Schema.AllOf =>
        result.value.put("allOf", ujson.Arr.from(allOf.schemas.map(schemaJson)))
      case reference: Schema.Reference =>
        /* In OpenAPI 3.0 (and 2.0), reference schemas are special in that all
         * their sibling values are ignored!
         *
         * This means that if any other sibling schema fields have been set
         * (eg. for a `description`, `example`, etc.), we need to nest the
         * schema reference object inside a `allOf` or `anyOf` field, depending
         * on if we're using Swagger-UI or Stoplight Elements.
         * This is a FORK DIFFERENCE
         *
         * See <https://stackoverflow.com/a/41752575/3072788>.
         */
        val refSchemaName = ujson.Str(Schema.Reference.toRefPath(reference.name))
        if (result.value.isEmpty) {
          result.value.put("$ref", refSchemaName)
        } else {
          result.value.put("anyOf", ujson.Arr(ujson.Obj("$ref" -> refSchemaName)))
        }
    }

    result
  }

  private def securitySchemeJson(securityScheme: SecurityScheme): ujson.Obj = {
    val result = ujson.Obj()
    result.value.put("type", ujson.Str(securityScheme.`type`))
    for (description <- securityScheme.description) {
      result.value.put("description", ujson.Str(description))
    }
    for (name <- securityScheme.name) {
      result.value.put("name", ujson.Str(name))
    }
    for (in <- securityScheme.in) {
      result.value.put("in", ujson.Str(in))
    }
    for (scheme <- securityScheme.scheme) {
      result.value.put("scheme", ujson.Str(scheme))
    }
    for (bearerFormat <- securityScheme.bearerFormat) {
      result.value.put("bearerFormat", ujson.Str(bearerFormat))
    }
    result
  }

  private def infoJson(info: Info): ujson.Obj = {
    val result = ujson.Obj()
    result.value.put("title", ujson.Str(info.title))
    result.value.put("version", ujson.Str(info.version))
    info.description.foreach(description => result.value.put("description", ujson.Str(description)))
    result
  }

  private def componentsJson(components: Components): ujson.Obj =
    ujson.Obj(
      "schemas" -> mapJson(components.schemas)(schemaJson),
      "securitySchemes" -> mapJson(components.securitySchemes)(
        securitySchemeJson
      )
    )

  private def responseJson(response: Response): ujson.Obj = {
    val result = ujson.Obj()
    result.value.put("description", ujson.Str(response.description))
    if (response.headers.nonEmpty) {
      result.value.put("headers", mapJson(response.headers)(responseHeaderJson))
    }
    if (response.content.nonEmpty) {
      result.value.put("content", mapJson(response.content)(mediaTypeJson))
    }
    result
  }

  def responseHeaderJson(responseHeader: ResponseHeader): ujson.Value = {
    val result = ujson.Obj()
    result.value.put("schema", schemaJson(responseHeader.schema))
    if (responseHeader.required) {
      result.value.put("required", ujson.True)
    }
    responseHeader.description.foreach { description =>
      result.value.put("description", ujson.Str(description))
    }
    result
  }

  // FORK DIFFERENCE Returns an Obj rather than Value
  def mediaTypeJson(mediaType: MediaType): ujson.Obj =
    mediaType.schema match {
      case Some(schema) => ujson.Obj("schema" -> schemaJson(schema))
      case None => ujson.Obj()
    }

  private def operationJson(operation: Operation): ujson.Obj = {
    val obj = ujson.Obj()
    obj.value.put("responses", mapJson(operation.responses)(responseJson))
    operation.operationId.foreach { id =>
      obj.value.put("operationId", ujson.Str(id))
    }
    operation.summary.foreach { summary =>
      obj.value.put("summary", ujson.Str(summary))
    }
    operation.description.foreach { description =>
      obj.value.put("description", ujson.Str(description))
    }
    if (operation.parameters.nonEmpty) {
      obj.value.put(
        "parameters",
        ujson.Arr.from(
          // FORK DIFFERENCE
          operation.parameters
            .filter(param => isEnterprise || !enterpriseParams.contains(param.name))
            .map(parameterJson),
        )
      )
    }
    operation.requestBody.foreach { requestBody =>
      obj.value.put("requestBody", requestBodyJson(requestBody))
    }
    if (operation.tags.nonEmpty) {
      val tags = ujson.Arr()
      operation.tags.foreach(tag => tags.value += ujson.Str(tag.name))
      obj.value.put("tags", tags)
    }
    if (operation.security.nonEmpty) {
      val security = ujson.Arr()
      operation.security.foreach(item => security.value += securityRequirementJson(item))
      obj.value.put("security", security)
    }
    if (operation.callbacks.nonEmpty) {
      obj.value.put("callbacks", mapJson(operation.callbacks)(pathsJson))
    }
    if (operation.deprecated) {
      obj.value.put("deprecated", ujson.True)
    }
    obj
  }

  private def parameterJson(parameter: Parameter): ujson.Value = {
    val result = ujson.Obj(
      "name" -> ujson.Str(parameter.name),
      "in" -> inJson(parameter.in),
      "schema" -> schemaJson(parameter.schema)
    )
    parameter.description.foreach { description =>
      result.value.put("description", ujson.Str(description))
    }
    if (parameter.required) {
      result.value.put("required", ujson.True)
    }
    result
  }

  private def inJson(in: In): ujson.Value =
    in match {
      case In.Query => ujson.Str("query")
      case In.Path => ujson.Str("path")
      case In.Header => ujson.Str("header")
      case In.Cookie => ujson.Str("cookie")
    }

  // FORK DIFFERENCE utility for resolving indirect examples
  private def getExample(schema: Schema): Option[ujson.Value] = schema match {
    case reference: Schema.Reference => reference.example orElse reference.original.flatMap(_.example)
    case other => other.example
  }
  private def requestBodyJson(body: RequestBody): ujson.Value = {
    val result = ujson.Obj()
    result.value.put("required", ujson.True)
    result.value.put("content", mapJsonUsingKeys(body.content){(mediaType, schema) =>
      val schemaJson = mediaTypeJson(schema)
      if (mediaType == "application/yaml") {
        val exampleOpt = schema.schema.flatMap(getExample)
        .map(ex => "example" -> ujson.Str(ex.transform(CirceJson).asYaml.spaces2))
        exampleOpt.foreach(Function.tupled(schemaJson.value.put))
      }
      schemaJson
    })
    body.description.foreach { description =>
      result.value.put("description", ujson.Str(description))
    }
    result
  }

  private def tagJson(tag: Tag): ujson.Value = {
    val result = ujson.Obj()
    result.value.put("name", ujson.Str(tag.name))

    for (description <- tag.description) {
      result.value.put("description", description)
    }
    for (externalDocs <- tag.externalDocs) {
      result.value.put("externalDocs", externalDocumentationObjectJson(externalDocs))
    }
    result
  }

  private def serverJson(server: Server): ujson.Value = {
    val result = ujson.Obj()
    result.value.put("url", ujson.Str(server.url))
    for (description <- server.description) {
      result.value.put("description", ujson.Str(description))
    }
    if (server.variables.nonEmpty) {
      result.value.put("variables", mapJson(server.variables)(serverVariableJson))
    }
    result
  }

  private def serverVariableJson(variable: ServerVariable): ujson.Value = {
    val result = ujson.Obj()
    result.value.put("default", ujson.Str(variable.default))
    for (description <- variable.description) {
      result.value.put("description", ujson.Str(description))
    }
    for (alternatives <- variable.`enum`) {
      result.value.put(
        "enum",
        ujson.Arr.from(alternatives.map(alternative => ujson.Str(alternative)))
    )
    }
    result
  }

  private def externalDocumentationObjectJson(
      externalDoc: ExternalDocumentationObject
  ): ujson.Value = {
    val result = ujson.Obj(
        "url" -> ujson.Str(externalDoc.url)
      )
    for (description <- externalDoc.description) {
      result.value.put("description", description)
    }
    result
  }

  private def securityRequirementJson(
      securityRequirement: SecurityRequirement
  ): ujson.Value =
    ujson.Obj(
      securityRequirement.name -> ujson.Arr.from(securityRequirement.scopes.map(ujson.Str))
    )

  private def pathsJson(paths: collection.Map[String, PathItem]): ujson.Obj =
    mapJson(paths)(pathItem => mapJson(pathItem.operations)(operationJson))

  private val jsonEncoder: Encoder[OpenApi, ujson.Value] =
    openApi => {
      val result = ujson.Obj()
      result.value.put("openapi", ujson.Str(openApiVersion))
      result.value.put("info", infoJson(openApi.info))
      result.value.put("paths", pathsJson(openApi.paths))

      if (openApi.servers.nonEmpty) {
        val servers = ujson.Arr()
        openApi.servers.foreach(server => servers.value += serverJson(server))
        result.value.put("servers", servers)
      }
      if (openApi.tags.nonEmpty) {
        val tagsAsJson = openApi.tags.map(tag => tagJson(tag)).toList
        result.value.put("tags", ujson.Arr.from(tagsAsJson))
      }
      if (openApi.components.schemas.nonEmpty || openApi.components.securitySchemes.nonEmpty) {
        result.value.put("components", componentsJson(openApi.components))
      }
      result.value
    }

  implicit val stringEncoder: Encoder[OpenApi, String] =
    openApi => jsonEncoder.encode(openApi).transform(ujson.StringRenderer()).toString

}

object OpenApiRenderer {
  val enterpriseParams: Set[String] = Set("namespace")
}
