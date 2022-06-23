// Copied from https://github.com/endpoints4s/endpoints4s/blob/f9ac4c66e83ae2b38c8c538ca5caba8524afc1a4/openapi/openapi/src/main/scala/endpoints4s/openapi/model/OpenApi.scala
package com.thatdot.quine.app.util

import scala.collection.mutable

import endpoints4s.Encoder
import endpoints4s.algebra.{ExternalDocumentationObject, Tag}
import endpoints4s.openapi.model.{
  Components,
  In,
  Info,
  MediaType,
  OpenApi,
  Operation,
  Parameter,
  PathItem,
  RequestBody,
  Response,
  ResponseHeader,
  Schema,
  SecurityRequirement,
  SecurityScheme
}
object OpenApiRenderer {

  val openApiVersion = "3.0.0"

  private def mapJson[A](map: collection.Map[String, A])(f: A => ujson.Value): ujson.Obj =
    new ujson.Obj(mutable.LinkedHashMap(map.iterator.map { case (k, v) =>
      (k, f(v))
    }.toSeq: _*))

  private[util] def schemaJson(schema: Schema): ujson.Obj = {
    val fields = mutable.LinkedHashMap.empty[String, ujson.Value]

    for (description <- schema.description)
      fields += "description" -> ujson.Str(description)
    for (example <- schema.example)
      fields += "example" -> example
    for (title <- schema.title)
      fields += "title" -> title
    for (default <- schema.default)
      fields += "default" -> default

    schema match {
      case primitive: Schema.Primitive =>
        fields += "type" -> ujson.Str(primitive.name)
        primitive.format.foreach(s => fields += "format" -> ujson.Str(s))
        primitive.minimum.foreach(d => fields += "minimum" -> ujson.Num(d))
        primitive.exclusiveMinimum.foreach(b => fields += "exclusiveMinimum" -> ujson.Bool(b))
        primitive.maximum.foreach(d => fields += "maximum" -> ujson.Num(d))
        primitive.exclusiveMaximum.foreach(b => fields += "exclusiveMaximum" -> ujson.Bool(b))
        primitive.multipleOf.foreach(d => fields += "multipleOf" -> ujson.Num(d))
      case obj: Schema.Object =>
        fields ++= List(
          "type" -> "object",
          "properties" -> new ujson.Obj(
            mutable.LinkedHashMap(
              obj.properties.map { (p: Schema.Property) =>
                val schema = p.schema
                  .withDefinedDescription(p.description)
                  .withDefinedDefault(p.defaultValue)
                p.name -> schemaJson(schema)
              }: _*
            )
          )
        )
        val required = obj.properties.filter(_.isRequired).map(_.name)
        if (required.nonEmpty) {
          fields += "required" -> ujson.Arr(required.map(ujson.Str): _*)
        }
        obj.additionalProperties.foreach(p => fields += "additionalProperties" -> schemaJson(p))
      case array: Schema.Array =>
        fields += "type" -> "array"
        array.elementType match {
          case Left(value) =>
            fields += "items" -> schemaJson(value)
          case Right(value) =>
            // Best effort (not 100% accurate) to represent the heterogeneous array in OpenAPI 3.0
            // This should be changed with OpenAPI 3.1 and more idiomatic representation using `prefixItems`
            fields ++= List(
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
        fields ++= schemaJson(
          enm.elementType.withDefinedDescription(enm.description)
        ).value
        fields += "enum" -> ujson.Arr(enm.values: _*)
      case oneOf: Schema.OneOf =>
        fields ++=
          (oneOf.alternatives match {
            case discAlternatives: Schema.DiscriminatedAlternatives =>
              val mappingFields: mutable.LinkedHashMap[String, ujson.Value] =
                mutable.LinkedHashMap(discAlternatives.alternatives.collect { case (tag, ref: Schema.Reference) =>
                  tag -> ujson.Str(Schema.Reference.toRefPath(ref.name))
                }: _*)
              val discFields = mutable.LinkedHashMap.empty[String, ujson.Value]
              discFields += "propertyName" -> ujson.Str(
                discAlternatives.discriminatorFieldName
              )
              if (mappingFields.nonEmpty) {
                discFields += "mapping" -> new ujson.Obj(mappingFields)
              }
              List(
                "oneOf" -> ujson
                  .Arr(
                    discAlternatives.alternatives
                      .map(kv => schemaJson(kv._2)): _*
                  ),
                "discriminator" -> ujson.Obj(discFields)
              )
            case enumAlternatives: Schema.EnumeratedAlternatives =>
              List(
                "oneOf" -> ujson
                  .Arr(enumAlternatives.alternatives.map(schemaJson): _*)
              )
          })
      case allOf: Schema.AllOf =>
        fields += "allOf" -> ujson.Arr(allOf.schemas.map(schemaJson): _*)
      case reference: Schema.Reference =>
        /* In OpenAPI 3.0 (and 2.0), reference schemas are special in that all
         * their sibling values are ignored!
         *
         * This means that if any other sibling schema fields have been set
         * (eg. for a `description`, `example`, etc.), we need to nest the
         * schema reference object inside a `allOf` or `anyOf` field, depending
         * on if we're using Swagger-UI or Stoplight Elements.
         *
         * See <https://stackoverflow.com/a/41752575/3072788>.
         */
        val refSchemaName = ujson.Str(Schema.Reference.toRefPath(reference.name))
        if (fields.isEmpty) {
          fields += "$ref" -> refSchemaName
        } else {
          fields += "anyOf" -> ujson.Arr(ujson.Obj("$ref" -> refSchemaName))
        }
    }

    new ujson.Obj(fields)
  }

  private def securitySchemeJson(securityScheme: SecurityScheme): ujson.Obj = {
    val fields = mutable.LinkedHashMap[String, ujson.Value](
      "type" -> ujson.Str(securityScheme.`type`)
    )
    for (description <- securityScheme.description)
      fields += "description" -> ujson.Str(description)
    for (name <- securityScheme.name)
      fields += "name" -> ujson.Str(name)
    for (in <- securityScheme.in)
      fields += "in" -> ujson.Str(in)
    for (scheme <- securityScheme.scheme)
      fields += "scheme" -> ujson.Str(scheme)
    for (bearerFormat <- securityScheme.bearerFormat)
      fields += "bearerFormat" -> ujson.Str(bearerFormat)
    new ujson.Obj(fields)
  }

  private def infoJson(info: Info): ujson.Obj = {
    val fields: mutable.LinkedHashMap[String, ujson.Value] =
      mutable.LinkedHashMap(
        "title" -> ujson.Str(info.title),
        "version" -> ujson.Str(info.version)
      )
    info.description.foreach(description => fields += "description" -> ujson.Str(description))
    ujson.Obj(fields)
  }

  private def componentsJson(components: Components): ujson.Obj =
    ujson.Obj(
      "schemas" -> mapJson(components.schemas)(schemaJson),
      "securitySchemes" -> mapJson(components.securitySchemes)(
        securitySchemeJson
      )
    )

  private def responseJson(response: Response): ujson.Obj = {
    val fields = mutable.LinkedHashMap[String, ujson.Value](
      "description" -> ujson.Str(response.description)
    )
    if (response.headers.nonEmpty) {
      fields += "headers" -> mapJson(response.headers)(responseHeaderJson)
    }
    if (response.content.nonEmpty) {
      fields += "content" -> mapJson(response.content)(mediaTypeJson)
    }
    new ujson.Obj(fields)
  }

  def responseHeaderJson(responseHeader: ResponseHeader): ujson.Value = {
    val fields = mutable.LinkedHashMap[String, ujson.Value](
      "schema" -> schemaJson(responseHeader.schema)
    )
    if (responseHeader.required) {
      fields += "required" -> ujson.True
    }
    responseHeader.description.foreach { description =>
      fields += "description" -> ujson.Str(description)
    }
    new ujson.Obj(fields)
  }

  def mediaTypeJson(mediaType: MediaType): ujson.Value =
    mediaType.schema match {
      case Some(schema) => ujson.Obj("schema" -> schemaJson(schema))
      case None => ujson.Obj()
    }

  private def operationJson(operation: Operation): ujson.Obj = {
    val fields = mutable.LinkedHashMap[String, ujson.Value](
      "responses" -> mapJson(operation.responses)(responseJson)
    )
    operation.operationId.foreach { id =>
      fields += "operationId" -> ujson.Str(id)
    }
    operation.summary.foreach { summary =>
      fields += "summary" -> ujson.Str(summary)
    }
    operation.description.foreach { description =>
      fields += "description" -> ujson.Str(description)
    }
    if (operation.parameters.nonEmpty) {
      fields += "parameters" -> ujson.Arr(
        operation.parameters.map(parameterJson): _*
      )
    }
    operation.requestBody.foreach { requestBody =>
      fields += "requestBody" -> requestBodyJson(requestBody)
    }
    if (operation.tags.nonEmpty) {
      fields += "tags" -> ujson.Arr(
        operation.tags.map(tag => ujson.Str(tag.name)): _*
      )
    }
    if (operation.security.nonEmpty) {
      fields += "security" -> ujson.Arr(
        operation.security.map(securityRequirementJson): _*
      )
    }
    if (operation.callbacks.nonEmpty) {
      fields += "callbacks" -> mapJson(operation.callbacks)(pathsJson)
    }
    if (operation.deprecated) {
      fields += "deprecated" -> ujson.True
    }
    new ujson.Obj(fields)
  }

  private def parameterJson(parameter: Parameter): ujson.Value = {
    val fields = mutable.LinkedHashMap[String, ujson.Value](
      "name" -> ujson.Str(parameter.name),
      "in" -> inJson(parameter.in),
      "schema" -> schemaJson(parameter.schema)
    )
    parameter.description.foreach { description =>
      fields += "description" -> ujson.Str(description)
    }
    if (parameter.required) {
      fields += "required" -> ujson.True
    }
    new ujson.Obj(fields)
  }

  private def inJson(in: In): ujson.Value =
    in match {
      case In.Query => ujson.Str("query")
      case In.Path => ujson.Str("path")
      case In.Header => ujson.Str("header")
      case In.Cookie => ujson.Str("cookie")
    }

  private def requestBodyJson(body: RequestBody): ujson.Value = {
    val fields = mutable.LinkedHashMap[String, ujson.Value](
      "content" -> mapJson(body.content)(mediaTypeJson)
    )
    body.description.foreach { description =>
      fields += "description" -> ujson.Str(description)
    }
    new ujson.Obj(fields)
  }

  private def tagJson(tag: Tag): ujson.Value = {
    val fields: mutable.LinkedHashMap[String, ujson.Value] =
      mutable.LinkedHashMap(
        "name" -> ujson.Str(tag.name)
      )

    if (tag.description.nonEmpty) {
      fields += "description" -> tag.description.get
    }
    if (tag.externalDocs.nonEmpty) {
      fields += "externalDocs" -> externalDocumentationObjectJson(
        tag.externalDocs.get
      )
    }
    new ujson.Obj(fields)
  }

  private def externalDocumentationObjectJson(
    externalDoc: ExternalDocumentationObject
  ): ujson.Value = {
    val fields: mutable.LinkedHashMap[String, ujson.Value] =
      mutable.LinkedHashMap(
        "url" -> ujson.Str(externalDoc.url)
      )

    if (externalDoc.description.nonEmpty)
      fields += "description" -> externalDoc.description.get
    new ujson.Obj(fields)
  }

  private def securityRequirementJson(
    securityRequirement: SecurityRequirement
  ): ujson.Value =
    ujson.Obj(
      securityRequirement.name -> ujson.Arr(
        securityRequirement.scopes.map(ujson.Str): _*
      )
    )

  private def pathsJson(paths: collection.Map[String, PathItem]): ujson.Obj =
    mapJson(paths)(pathItem => mapJson(pathItem.operations)(operationJson))

  private val jsonEncoder: Encoder[OpenApi, ujson.Value] =
    openApi => {
      val fields: mutable.LinkedHashMap[String, ujson.Value] =
        mutable.LinkedHashMap(
          "openapi" -> ujson.Str(openApiVersion),
          "info" -> infoJson(openApi.info),
          "paths" -> pathsJson(openApi.paths)
        )
      if (openApi.tags.nonEmpty) {
        val tagsAsJson = openApi.tags.map(tag => tagJson(tag)).toList
        fields += "tags" -> ujson.Arr(tagsAsJson: _*)
      }
      if (openApi.components.schemas.nonEmpty || openApi.components.securitySchemes.nonEmpty) {
        fields += "components" -> componentsJson(openApi.components)
      }
      new ujson.Obj(fields)
    }

  implicit val stringEncoder: Encoder[OpenApi, String] =
    openApi => jsonEncoder.encode(openApi).transform(ujson.StringRenderer()).toString

}
