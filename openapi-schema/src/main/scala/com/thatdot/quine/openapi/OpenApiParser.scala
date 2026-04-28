package com.thatdot.quine.openapi

import io.circe.{Json, JsonObject}

/** Parses an OpenAPI 3.0 JSON document into a [[ParsedSpec]].
  *
  * Only extracts the subset needed for spec-driven form rendering:
  * component schemas (with $ref resolution) and endpoint definitions.
  */
object OpenApiParser {

  private val MaxRefDepth = 10

  def parse(rawJson: String): Either[String, ParsedSpec] =
    io.circe.parser
      .parse(rawJson)
      .left
      .map(_.getMessage)
      .flatMap(parseRoot)

  private def parseRoot(root: Json): Either[String, ParsedSpec] = {
    val schemas = parseSchemas(root)
    val endpoints = parseEndpoints(root, schemas)
    Right(ParsedSpec(schemas = schemas, endpoints = endpoints))
  }

  // --- Schema parsing ---

  private def parseSchemas(root: Json): Map[String, SchemaNode] = {
    val schemasObj = root.hcursor
      .downField("components")
      .downField("schemas")
      .focus
      .flatMap(_.asObject)
      .getOrElse(JsonObject.empty)

    schemasObj.toMap.map { case (name, json) =>
      // Tag each top-level schema with its registered name. The tag survives
      // `resolveRef` (which returns this node) and `copy(...)` (which is how
      // the renderer derives variants), so the UI hints overlay can be looked
      // up by schema name without the renderer needing caller-provided hints.
      name -> parseSchemaNode(json).copy(schemaName = Some(name))
    }
  }

  def parseSchemaNode(json: Json): SchemaNode = {
    val cursor = json.hcursor
    SchemaNode(
      typ = cursor.get[String]("type").toOption,
      title = cursor.get[String]("title").toOption,
      description = cursor.get[String]("description").toOption,
      default = cursor.downField("default").focus,
      `enum` = cursor
        .downField("enum")
        .focus
        .flatMap(_.asArray)
        .map(_.toList),
      format = cursor.get[String]("format").toOption,
      properties = cursor
        .downField("properties")
        .focus
        .flatMap(_.asObject)
        .map(_.toMap.map { case (k, v) => k -> parseSchemaNode(v) }),
      required = cursor
        .downField("required")
        .focus
        .flatMap(_.asArray)
        .map(_.flatMap(_.asString).toSet),
      additionalProperties = cursor
        .downField("additionalProperties")
        .focus
        .map { ap =>
          ap.asBoolean match {
            case Some(b) => Left(b)
            case None => Right(parseSchemaNode(ap))
          }
        },
      items = cursor.downField("items").focus.map(parseSchemaNode),
      oneOf = cursor
        .downField("oneOf")
        .focus
        .flatMap(_.asArray)
        .map(_.toList.map(parseSchemaNode)),
      allOf = cursor
        .downField("allOf")
        .focus
        .flatMap(_.asArray)
        .map(_.toList.map(parseSchemaNode)),
      anyOf = cursor
        .downField("anyOf")
        .focus
        .flatMap(_.asArray)
        .map(_.toList.map(parseSchemaNode)),
      discriminator = cursor
        .downField("discriminator")
        .focus
        .map(parseDiscriminator),
      ref = cursor.get[String]("$ref").toOption,
      nullable = cursor.get[Boolean]("nullable").toOption.getOrElse(false),
    )
  }

  private def parseDiscriminator(json: Json): Discriminator = {
    val cursor = json.hcursor
    Discriminator(
      propertyName = cursor.get[String]("propertyName").getOrElse("type"),
      mapping = cursor
        .downField("mapping")
        .focus
        .flatMap(_.asObject)
        .map(_.toMap.collect { case (k, v) if v.isString => k -> v.asString.get }),
    )
  }

  // --- $ref resolution ---

  /** Resolve a $ref string like "#/components/schemas/KafkaIngest" against the schemas map.
    * Returns the resolved SchemaNode, or an empty SchemaNode if not found.
    * Tracks depth to prevent infinite recursion on self-referencing schemas.
    */
  def resolveRef(ref: String, schemas: Map[String, SchemaNode], depth: Int = 0): SchemaNode =
    if (depth >= MaxRefDepth) SchemaNode(description = Some(s"(max ref depth reached for $ref)"))
    else {
      val name = ref.split("/").last
      schemas.get(name) match {
        case Some(node) if node.ref.isDefined =>
          resolveRef(node.ref.get, schemas, depth + 1)
        case Some(node) => node
        case None => SchemaNode(description = Some(s"(unresolved ref: $ref)"))
      }
    }

  /** Fully resolve a SchemaNode: if it's a $ref, follow it; if it has allOf, flatten it. */
  def resolveNode(node: SchemaNode, schemas: Map[String, SchemaNode], depth: Int = 0): SchemaNode =
    if (depth >= MaxRefDepth) node
    else {
      val resolved = node.ref match {
        case Some(ref) => resolveRef(ref, schemas, depth)
        case None => node
      }
      resolved.allOf match {
        case Some(parts) => flattenAllOf(parts.map(resolveNode(_, schemas, depth + 1)), resolved)
        case None => resolved
      }
    }

  /** Merge multiple schemas from an allOf into a single object schema by combining properties. */
  private def flattenAllOf(parts: List[SchemaNode], parent: SchemaNode): SchemaNode = {
    val mergedProperties = parts.flatMap(_.properties).foldLeft(Map.empty[String, SchemaNode])(_ ++ _)
    val mergedRequired = parts.flatMap(_.required).foldLeft(Set.empty[String])(_ ++ _)
    val mergedTitle = parts.flatMap(_.title).headOption.orElse(parent.title)
    val mergedDescription = parts.flatMap(_.description).headOption.orElse(parent.description)
    val mergedDiscriminator = parts.flatMap(_.discriminator).headOption.orElse(parent.discriminator)
    val mergedOneOf = parts.flatMap(_.oneOf).reduceOption(_ ++ _).orElse(parent.oneOf)

    parent.copy(
      typ = Some("object"),
      title = mergedTitle,
      description = mergedDescription,
      properties = if (mergedProperties.nonEmpty) Some(mergedProperties) else parent.properties,
      required = if (mergedRequired.nonEmpty) Some(mergedRequired) else parent.required,
      discriminator = mergedDiscriminator,
      oneOf = mergedOneOf,
      allOf = None,
    )
  }

  // --- Endpoint parsing ---

  private def parseEndpoints(root: Json, schemas: Map[String, SchemaNode]): List[ApiEndpoint] = {
    val pathsObj = root.hcursor
      .downField("paths")
      .focus
      .flatMap(_.asObject)
      .getOrElse(JsonObject.empty)

    pathsObj.toList.flatMap { case (path, pathJson) =>
      val pathParams = """\{([^}]+)\}""".r.findAllMatchIn(path).map(_.group(1)).toList
      val methodsObj = pathJson.asObject.getOrElse(JsonObject.empty)

      methodsObj.toList.collect {
        case (method, opJson) if Set("get", "post", "put", "delete", "patch").contains(method.toLowerCase) =>
          val cursor = opJson.hcursor
          ApiEndpoint(
            method = method.toUpperCase,
            path = path,
            summary = cursor.get[String]("summary").toOption,
            operationId = cursor.get[String]("operationId").toOption,
            requestBodySchema = extractRequestBodySchema(opJson),
            responseSchema = extractResponseSchema(opJson),
            pathParams = pathParams,
            queryParams = extractQueryParams(opJson),
          )
      }
    }
  }

  private def extractRequestBodySchema(opJson: Json): Option[SchemaNode] =
    opJson.hcursor
      .downField("requestBody")
      .downField("content")
      .focus
      .flatMap(contentJson => firstMediaTypeSchema(contentJson))

  private def extractResponseSchema(opJson: Json): Option[SchemaNode] = {
    val responses = opJson.hcursor.downField("responses").focus.flatMap(_.asObject)
    responses.flatMap { obj =>
      // Try 200, 201, 202 in order
      List("200", "201", "202").collectFirst(Function.unlift { code =>
        obj(code).flatMap { resp =>
          resp.hcursor
            .downField("content")
            .focus
            .flatMap(firstMediaTypeSchema)
        }
      })
    }
  }

  private def firstMediaTypeSchema(contentJson: Json): Option[SchemaNode] =
    contentJson.asObject.flatMap { obj =>
      // Prefer application/json, fall back to first available
      obj("application/json")
        .orElse(obj.values.headOption)
        .flatMap(_.hcursor.downField("schema").focus)
        .map(parseSchemaNode)
    }

  private def extractQueryParams(opJson: Json): List[QueryParam] =
    opJson.hcursor
      .downField("parameters")
      .focus
      .flatMap(_.asArray)
      .getOrElse(Vector.empty)
      .toList
      .filter { p =>
        p.hcursor.get[String]("in").toOption.contains("query")
      }
      .map { p =>
        val cursor = p.hcursor
        QueryParam(
          name = cursor.get[String]("name").getOrElse(""),
          required = cursor.get[Boolean]("required").getOrElse(false),
          schema = cursor.downField("schema").focus.map(parseSchemaNode),
        )
      }
}
