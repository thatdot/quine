package com.thatdot.quine.app.routes

import java.nio.file.{FileAlreadyExistsException, FileSystemException, Files, InvalidPathException, Paths}

import scala.compat.ExecutionContexts
import scala.concurrent.Future
import scala.util.Try
import scala.util.control.NonFatal

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.alpakka.s3.scaladsl.S3
import akka.stream.scaladsl.FileIO
import akka.util.Timeout

import endpoints4s.Invalid

import com.thatdot.quine.app.routes.exts.circe.JsonEntitiesFromSchemas
import com.thatdot.quine.compiler.cypher
import com.thatdot.quine.graph.AlgorithmGraph
import com.thatdot.quine.graph.cypher.{CompiledQuery, CypherException, Location}
import com.thatdot.quine.model.Milliseconds
import com.thatdot.quine.routes.AlgorithmRoutes

trait AlgorithmRoutesImpl
    extends AlgorithmRoutes
    with endpoints4s.akkahttp.server.Endpoints
    with JsonEntitiesFromSchemas
    with exts.ServerQuineEndpoints {

  implicit def graph: AlgorithmGraph

  implicit def timeout: Timeout

  private def compileWalkQuery(queryOpt: Option[String]): CompiledQuery[Location.OnNode] = {
    val queryText = queryOpt.fold(AlgorithmGraph.defaults.walkQuery)(AlgorithmGraph.defaults.walkPrefix + _)
    val compiledQuery = cypher.compile(queryText, unfixedParameters = List("n"))
    require(compiledQuery.isReadOnly, s"Query must conclusively be a read-only query. Provided: $queryText")
    require(!compiledQuery.canContainAllNodeScan, s"Query must not scan all nodes. Provided: $queryText")
    compiledQuery
  }

  private val algorithmSaveRandomWalksRoute = algorithmSaveRandomWalks.implementedBy {
    case (
          lengthOpt,
          countOpt,
          queryOpt,
          returnParamOpt,
          inOutParamOpt,
          seedOpt,
          atTime: Option[Milliseconds],
          parallelism,
          saveLocation
        ) =>
      val defaultFileName =
        s"""graph-walk-
           |${atTime.map(_.millis).getOrElse(s"${System.currentTimeMillis}_T")}-
           |${lengthOpt.getOrElse(AlgorithmGraph.defaults.walkLength)}x
           |${countOpt.getOrElse(AlgorithmGraph.defaults.walkCount)}-q
           |${queryOpt.map(_.length).getOrElse("0")}-
           |${returnParamOpt.getOrElse(AlgorithmGraph.defaults.returnParam)}x
           |${inOutParamOpt.getOrElse(AlgorithmGraph.defaults.inOutParam)}-
           |${seedOpt.getOrElse("_")}.csv""".stripMargin.replace("\n", "")
      val fileName = saveLocation match {
        case S3Bucket(_, keyOpt) => keyOpt.getOrElse(defaultFileName)
        case LocalFile(None) => defaultFileName
        case LocalFile(Some(fileName)) =>
          if (fileName.nonEmpty) fileName else defaultFileName
      }
      Try {
        require(!lengthOpt.exists(_ < 1), "walk length cannot be less than one.")
        require(!countOpt.exists(_ < 0), "walk count cannot be less than zero.")
        require(!inOutParamOpt.exists(_ < 0d), "in-out parameter cannot be less than zero.")
        require(!returnParamOpt.exists(_ < 0d), "return parameter cannot be less than zero.")
        require(parallelism >= 1, "parallelism cannot be less than one.")
        val saveSink = saveLocation match {
          case S3Bucket(bucketName, _) =>
            S3.multipartUpload(bucketName, fileName)
          case LocalFile(_) =>
            val p = Paths.get(fileName)
            Files.createFile(p) // Deliberately cause an error if it is not accessible
            FileIO.toPath(p)
        }
        saveSink -> compileWalkQuery(queryOpt)
      }.map { case (sink, compiledQuery) =>
        graph.algorithms
          .saveRandomWalks(
            sink,
            compiledQuery,
            lengthOpt.getOrElse(AlgorithmGraph.defaults.walkLength),
            countOpt.getOrElse(AlgorithmGraph.defaults.walkCount),
            returnParamOpt.getOrElse(AlgorithmGraph.defaults.returnParam),
            inOutParamOpt.getOrElse(AlgorithmGraph.defaults.inOutParam),
            seedOpt,
            atTime,
            parallelism
          )
        fileName
      }.toEither
        .left
        .map {
          case _: InvalidPathException | _: FileAlreadyExistsException | _: SecurityException |
              _: FileSystemException =>
            Invalid(s"Invalid file name: $fileName") // Return a Bad Request Error
          case e: CypherException => Invalid(s"Invalid query: ${e.getMessage}")
          case e: IllegalArgumentException => Invalid(e.getMessage)
          case NonFatal(e) => throw e // Return an Internal Server Error
        }
  }

  private val algorithmRandomWalkRoute = algorithmRandomWalk.implementedByAsync {
    case (qid, (lengthOpt, queryOpt, returnParamOpt, inOutParamOpt, seedOpt, atTime)) =>
      val errors = Try {
        require(!lengthOpt.exists(_ < 1), "walk length cannot be less than one.")
        require(!inOutParamOpt.exists(_ < 0d), "in-out parameter cannot be less than zero.")
        require(!returnParamOpt.exists(_ < 0d), "return parameter cannot be less than zero.")
        Nil
      }.toEither.left
        .map {
          case e: CypherException => Invalid(s"Invalid query: ${e.getMessage}")
          case e: IllegalArgumentException => Invalid(e.getMessage)
          case NonFatal(e) => throw e // Return an Internal Server Error
        }
      if (errors.isLeft) Future.successful[Either[Invalid, List[String]]](errors)
      else
        graph.algorithms
          .randomWalk(
            qid,
            compileWalkQuery(queryOpt),
            lengthOpt.getOrElse(AlgorithmGraph.defaults.walkLength),
            returnParamOpt.getOrElse(AlgorithmGraph.defaults.returnParam),
            inOutParamOpt.getOrElse(AlgorithmGraph.defaults.inOutParam),
            None,
            seedOpt,
            atTime
          )
          .map(w => Right(w.acc))(ExecutionContexts.parasitic)
  }

  final val algorithmRoutes: Route = {
    algorithmSaveRandomWalksRoute ~
    algorithmRandomWalkRoute
  }
}
