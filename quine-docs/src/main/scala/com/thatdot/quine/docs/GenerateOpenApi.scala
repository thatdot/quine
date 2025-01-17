package com.thatdot.quine.docs

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths, StandardOpenOption}

import endpoints4s.openapi.model.OpenApi

import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.quine.app.routes.QuineAppOpenApiDocs
import com.thatdot.quine.app.util.OpenApiRenderer
import com.thatdot.quine.graph.QuineUUIDProvider

object GenerateOpenApi extends App {
  val logConfig: LogConfig = LogConfig()

  val outputPath: Path = args match {
    case Array(stringPath) => Paths.get(stringPath)
    case _ =>
      println(this.getClass.getSimpleName + " expected one path argument but got: " + args.mkString("[", ", ", "]"))
      sys.exit(1)
  }

  val openApiRoutes: OpenApi = new QuineAppOpenApiDocs(QuineUUIDProvider)(logConfig).api
  val openApiDocumentationJson: String =
    OpenApiRenderer(isEnterprise = false).stringEncoder.encode(openApiRoutes)

  Files.createDirectories(outputPath.getParent())
  Files.write(
    outputPath,
    openApiDocumentationJson.getBytes(StandardCharsets.UTF_8),
    StandardOpenOption.TRUNCATE_EXISTING,
    StandardOpenOption.CREATE,
  )
}
