package com.thatdot.connect

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths, StandardOpenOption}

import endpoints4s.openapi.model.OpenApi

import com.thatdot.connect.routes.ConnectOpenApiDocs
import com.thatdot.quine.graph.QuineIdRandomLongProvider

object GenerateOpenApi extends App {

  val outputPath: Path = args match {
    case Array(stringPath) => Paths.get(stringPath)
    case _ =>
      println(s"GenerateOpenApi expected one path argument but got: ${args.mkString(",")}")
      sys.exit(1)
  }

  val openApiRoutes: OpenApi = new ConnectOpenApiDocs(QuineIdRandomLongProvider).api
  val openApiDocumentationJson: String = OpenApi.stringEncoder.encode(openApiRoutes)

  Files.createDirectories(outputPath.getParent())
  Files.write(
    outputPath,
    openApiDocumentationJson.getBytes(StandardCharsets.UTF_8),
    StandardOpenOption.TRUNCATE_EXISTING,
    StandardOpenOption.CREATE
  )
}
