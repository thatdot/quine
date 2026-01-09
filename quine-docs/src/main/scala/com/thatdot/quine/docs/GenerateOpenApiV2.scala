package com.thatdot.quine.docs

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths, StandardOpenOption}

import io.circe.Printer
import io.circe.syntax._
import sttp.apispec.openapi.circe._

import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.quine.app.v2api.QuineOssV2OpenApiDocs
import com.thatdot.quine.graph.QuineUUIDProvider
import com.thatdot.quine.model.QuineIdProvider

object GenerateOpenApiV2 {

  def main(args: Array[String]): Unit = {
    val outputPath: Path = args match {
      case Array(stringPath) => Paths.get(stringPath)
      case _ =>
        println(this.getClass.getSimpleName + " expected one path argument but got: " + args.mkString("[", ", ", "]"))
        sys.exit(1)
    }

    val docs = new QuineOssV2OpenApiDocsImpl()
    val openApiJson = Printer.spaces2.print(docs.api.asJson)

    Files.createDirectories(outputPath.getParent)
    Files.write(
      outputPath,
      openApiJson.getBytes(StandardCharsets.UTF_8),
      StandardOpenOption.TRUNCATE_EXISTING,
      StandardOpenOption.CREATE,
    )

    println(s"Generated V2 OpenAPI spec at: $outputPath")
  }

  class QuineOssV2OpenApiDocsImpl extends QuineOssV2OpenApiDocs {
    override lazy val idProvider: QuineIdProvider = QuineUUIDProvider
    implicit protected val logConfig: LogConfig = LogConfig()
  }
}
