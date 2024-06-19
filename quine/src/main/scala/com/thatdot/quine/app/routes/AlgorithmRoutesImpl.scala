package com.thatdot.quine.app.routes

import java.nio.file.{FileAlreadyExistsException, FileSystemException, Files, InvalidPathException, Paths}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try
import scala.util.control.NonFatal

import org.apache.pekko.http.scaladsl.server.Directives._
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.stream.connectors.s3.scaladsl.S3
import org.apache.pekko.stream.scaladsl.FileIO
import org.apache.pekko.util.Timeout

import endpoints4s.Invalid

import com.thatdot.quine.app.routes.exts.circe.JsonEntitiesFromSchemas
import com.thatdot.quine.compiler.cypher
import com.thatdot.quine.graph.cypher.{CompiledQuery, CypherException, Location}
import com.thatdot.quine.graph.{AlgorithmGraph, NamespaceId}
import com.thatdot.quine.model.Milliseconds
import com.thatdot.quine.routes.AlgorithmRoutes
trait AlgorithmMethods {
  def compileWalkQuery(queryOpt: Option[String]): CompiledQuery[Location.OnNode] = {
    val queryText = queryOpt.fold(AlgorithmGraph.defaults.walkQuery)(AlgorithmGraph.defaults.walkPrefix + _)
    val compiledQuery = cypher.compile(queryText, unfixedParameters = List("n"))
    require(compiledQuery.isReadOnly, s"Query must conclusively be a read-only query. Provided: $queryText")
    require(!compiledQuery.canContainAllNodeScan, s"Query must not scan all nodes. Provided: $queryText")
    compiledQuery
  }

  def generateDefaultFileName(
    atTime: Option[Milliseconds],
    lengthOpt: Option[Int],
    countOpt: Option[Int],
    queryOpt: Option[String],
    returnParamOpt: Option[Double],
    inOutParamOpt: Option[Double],
    seedOpt: Option[String]
  ): String = s"""graph-walk-
              |${atTime.map(_.millis).getOrElse(s"${System.currentTimeMillis}_T")}-
              |${lengthOpt.getOrElse(AlgorithmGraph.defaults.walkLength)}x
              |${countOpt.getOrElse(AlgorithmGraph.defaults.walkCount)}-q
              |${queryOpt.map(_.length).getOrElse("0")}-
              |${returnParamOpt.getOrElse(AlgorithmGraph.defaults.returnParam)}x
              |${inOutParamOpt.getOrElse(AlgorithmGraph.defaults.inOutParam)}-
              |${seedOpt.getOrElse("_")}.csv""".stripMargin.replace("\n", "")
}

trait AlgorithmRoutesImpl
    extends AlgorithmRoutes
    with AlgorithmMethods
    with endpoints4s.pekkohttp.server.Endpoints
    with JsonEntitiesFromSchemas
    with exts.ServerQuineEndpoints {

  implicit def graph: AlgorithmGraph

  implicit def timeout: Timeout

  private val algorithmSaveRandomWalksRoute = algorithmSaveRandomWalks.implementedBy {
    case (
          lengthOpt,
          countOpt,
          queryOpt,
          returnParamOpt,
          inOutParamOpt,
          seedOpt,
          namespaceParam,
          atTime: Option[Milliseconds],
          parallelism,
          saveLocation
        ) =>
      graph.requiredGraphIsReady()
      val namespaceId = namespaceFromParam(namespaceParam)
      if (!graph.getNamespaces.contains(namespaceId)) Right(None)
      else {
        val defaultFileName =
          generateDefaultFileName(atTime, lengthOpt, countOpt, queryOpt, returnParamOpt, inOutParamOpt, seedOpt)

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
              namespaceFromParam(namespaceParam),
              atTime,
              parallelism
            )
          Some(fileName)
        }.toEither
          .left
          .map {
            case _: InvalidPathException | _: FileAlreadyExistsException | _: SecurityException |
                _: FileSystemException =>
              Invalid(s"Invalid file name: $fileName") // Return a Bad Request Error
            case e: CypherException => Invalid(s"Invalid query: ${e.getMessage}")
            case e: IllegalArgumentException => Invalid(e.getMessage)
            case NonFatal(e) => throw e // Return an Internal Server Error
            case other => throw other // This might expose more than we want
          }
      }
  }

  private val algorithmRandomWalkRoute = algorithmRandomWalk.implementedByAsync {
    case (qid, (lengthOpt, queryOpt, returnParamOpt, inOutParamOpt, seedOpt, atTime, namespaceParam)) =>
      val errors = Try {
        require(!lengthOpt.exists(_ < 1), "walk length cannot be less than one.")
        require(!inOutParamOpt.exists(_ < 0d), "in-out parameter cannot be less than zero.")
        require(!returnParamOpt.exists(_ < 0d), "return parameter cannot be less than zero.")
        Some(Nil)
      }.toEither.left
        .map {
          case e: CypherException => Invalid(s"Invalid query: ${e.getMessage}")
          case e: IllegalArgumentException => Invalid(e.getMessage)
          case NonFatal(e) => throw e // Return an Internal Server Error
          case other => throw other // this might expose more than we want
        }
      if (errors.isLeft) Future.successful[Either[Invalid, Option[List[String]]]](errors)
      else {
        val ns = namespaceFromParam(namespaceParam)
        graph.requiredGraphIsReady()
        ifNamespaceFound(ns)(
          graph.algorithms
            .randomWalk(
              qid,
              compileWalkQuery(queryOpt),
              lengthOpt.getOrElse(AlgorithmGraph.defaults.walkLength),
              returnParamOpt.getOrElse(AlgorithmGraph.defaults.returnParam),
              inOutParamOpt.getOrElse(AlgorithmGraph.defaults.inOutParam),
              None,
              seedOpt,
              ns,
              atTime
            )
            .map(w => Right(w.acc))(ExecutionContext.parasitic)
        )
      }
  }

  final val algorithmRoutes: Route =
    algorithmSaveRandomWalksRoute ~
    algorithmRandomWalkRoute

  final private def ifNamespaceFound[A](namespaceId: NamespaceId)(
    ifFound: => Future[Either[ClientErrors, A]]
  ): Future[Either[ClientErrors, Option[A]]] =
    if (!graph.getNamespaces.contains(namespaceId)) Future.successful(Right(None))
    else ifFound.map(_.map(Some(_)))(ExecutionContext.parasitic)
}
