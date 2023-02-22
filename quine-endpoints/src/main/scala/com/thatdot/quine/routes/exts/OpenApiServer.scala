package com.thatdot.quine.routes.exts

/** see <https://github.com/OAI/OpenAPI-Specification/blob/main/versions/3.0.3.md#server-object> */
case class OpenApiServer(
  url: String,
  description: Option[String] = None,
  variables: Map[String, OpenApiServerVariable] = Map.empty
)

/** see <https://github.com/OAI/OpenAPI-Specification/blob/main/versions/3.0.3.md#server-variable-object> */
case class OpenApiServerVariable(
  `enum`: Option[Seq[String]] = None,
  default: String,
  description: Option[String] = None
)
