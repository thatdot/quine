package com.thatdot.quine.docs

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths, StandardOpenOption}

import endpoints4s.openapi.model.OpenApi

import com.thatdot.quine.app.routes.QuineAppOpenApiDocs
import com.thatdot.quine.app.util.OpenApiRenderer
import com.thatdot.quine.graph.QuineUUIDProvider
import com.thatdot.quine.util.Log._

class GenerateOpenApi(implicit protected val logConfig: LogConfig) extends App {

  private val (outputPath: Path, isEnterprise: Boolean) = args match {
    case Array(stringPath) => (Paths.get(stringPath), false)
    case Array(stringPath, isEnterprise) => (Paths.get(stringPath), isEnterprise == "true")
    case _ =>
      println(s"GenerateOpenApi expected a path and optional isEnterprise argument but got: ${args.mkString(",")}")
      sys.exit(1)
  }

  val openApiRoutes: OpenApi = new QuineAppOpenApiDocs(QuineUUIDProvider).api
  val openApiDocumentationJson: String =
    OpenApiRenderer(isEnterprise).stringEncoder(servers = None).encode(openApiRoutes)

  Files.createDirectories(outputPath.getParent())
  Files.write(
    outputPath,
    openApiDocumentationJson.getBytes(StandardCharsets.UTF_8),
    StandardOpenOption.TRUNCATE_EXISTING,
    StandardOpenOption.CREATE
  )
}
