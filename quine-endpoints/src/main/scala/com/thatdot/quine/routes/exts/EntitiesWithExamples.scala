package com.thatdot.quine.routes.exts

/** Augument the usual methods for creating request/response entities with
  * variants that are annotated with examples. This is particularly useful when
  * an endpoint can benefit from a more specific example (than might otherwise
  * be provided by the schema).
  */
trait EntitiesWithExamples extends endpoints4s.algebra.JsonEntities with endpoints4s.algebra.JsonSchemas {

  /** Like [[jsonResponse]], but includes an example */
  def jsonResponseWithExample[A](example: A)(implicit codec: JsonResponse[A]): ResponseEntity[A]

  /** Like [[jsonRequest]] but includes an example */
  def jsonRequestWithExample[A](example: A)(implicit codec: JsonRequest[A]): RequestEntity[A]

  /** Like [[textResponse]], but includes an example */
  def textResponseWithExample(example: String): ResponseEntity[String]

  /** Like [[textRequest]], but includes an example */
  def textRequestWithExample(example: String): RequestEntity[String]

  /** Expect a CSV as input */
  def csvRequest: RequestEntity[List[List[String]]]

  /** Like [[csvRequest]], but includes an example */
  def csvRequestWithExample(example: List[List[String]]): RequestEntity[List[List[String]]]

  /** Turn a CSV into a string */
  final def renderCsv(csv: List[List[String]]): String =
    csv.view
      .map(_.view.map(cell => '"' + cell.replace("\"", "\"\"") + '"').mkString(","))
      .mkString("\r\n")
}

/** Mix-in that make the example-annotating endpoints no-ops */
trait NoopEntitiesWithExamples extends EntitiesWithExamples {

  def jsonResponseWithExample[A](example: A)(implicit codec: JsonResponse[A]): ResponseEntity[A] = jsonResponse[A]
  def jsonRequestWithExample[A](example: A)(implicit codec: JsonRequest[A]): RequestEntity[A] = jsonRequest[A]

  def textResponseWithExample(example: String) = textResponse
  def textRequestWithExample(example: String) = textRequest

  def csvRequestWithExample(example: List[List[String]]) = csvRequest
}

/** Mix-in implementing the example-annotating endpoints for OpenAPI */
trait OpenApiEntitiesWithExamples extends EntitiesWithExamples with endpoints4s.openapi.JsonEntitiesFromSchemas {

  import endpoints4s.openapi.model._

  def jsonResponseWithExample[A](example: A)(implicit codec: JsonSchema[A]): ResponseEntity[A] =
    jsonResponse[A](codec.withExample(example))
  def jsonRequestWithExample[A](example: A)(implicit codec: JsonSchema[A]): RequestEntity[A] =
    jsonRequest[A](codec.withExample(example))

  def textResponseWithExample(example: String): Map[String, MediaType] =
    Map(
      "text/plain" -> MediaType(
        Some(
          Schema.Primitive(
            name = "string",
            format = None,
            description = None,
            example = Some(ujson.Str(example)),
            title = None
          )
        )
      )
    )

  def textRequestWithExample(example: String): Map[String, MediaType] =
    Map(
      "text/plain" -> MediaType(
        Some(
          Schema.Primitive(
            name = "string",
            format = None,
            description = None,
            example = Some(ujson.Str(example)),
            title = None
          )
        )
      )
    )

  def csvRequest: Map[String, MediaType] =
    Map(
      "text/csv" -> MediaType(
        Some(
          Schema.Primitive(
            name = "string",
            format = None,
            description = None,
            example = None,
            title = None
          )
        )
      )
    )

  def csvRequestWithExample(example: List[List[String]]): Map[String, MediaType] =
    Map(
      "text/csv" -> MediaType(
        Some(
          Schema.Primitive(
            name = "string",
            format = None,
            description = None,
            example = Some(ujson.Str(renderCsv(example))),
            title = None
          )
        )
      )
    )

  def ServiceUnavailable = 503
}
