package com.thatdot.quine.app.v2api.definitions

import java.nio.file.{FileAlreadyExistsException, FileSystemException, InvalidPathException}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try
import scala.util.control.NonFatal

import org.apache.pekko.util.Timeout

import shapeless.{:+:, CNil, Coproduct}

import com.thatdot.api.v2.ErrorResponse.{BadRequest, ServerError}
import com.thatdot.api.v2.ErrorResponseHelpers.toServerError
import com.thatdot.api.v2.ErrorType
import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.app.routes.AlgorithmMethods
import com.thatdot.quine.app.v2api.endpoints.V2AlgorithmEndpointEntities.TSaveLocation
import com.thatdot.quine.graph.cypher.CypherException
import com.thatdot.quine.graph.{AlgorithmGraph, BaseGraph, NamespaceId}
import com.thatdot.quine.model.Milliseconds

trait AlgorithmApiMethods extends AlgorithmMethods {
  val graph: BaseGraph with AlgorithmGraph

  implicit def timeout: Timeout

  /** Note: Duplicate implementation of [[AlgorithmRoutesImpl.algorithmSaveRandomWalksRoute]] */
  def algorithmSaveRandomWalks(
    lengthOpt: Option[Int],
    countOpt: Option[Int],
    queryOpt: Option[String],
    returnParamOpt: Option[Double],
    inOutParamOpt: Option[Double],
    seedOpt: Option[String],
    namespaceId: NamespaceId,
    atTime: Option[Milliseconds],
    parallelism: Int,
    saveLocation: TSaveLocation,
  ): Either[ServerError :+: BadRequest :+: CNil, Option[String]] = {

    graph.requiredGraphIsReady()
    if (!graph.getNamespaces.contains(namespaceId)) Right(None)
    else {
      val defaultFileName =
        generateDefaultFileName(atTime, lengthOpt, countOpt, queryOpt, returnParamOpt, inOutParamOpt, seedOpt)
      val fileName = saveLocation.fileName(defaultFileName)
      Try {
        require(!lengthOpt.exists(_ < 1), "walk length cannot be less than one.")
        require(!countOpt.exists(_ < 0), "walk count cannot be less than zero.")
        require(!inOutParamOpt.exists(_ < 0d), "in-out parameter cannot be less than zero.")
        require(!returnParamOpt.exists(_ < 0d), "return parameter cannot be less than zero.")
        require(parallelism >= 1, "parallelism cannot be less than one.")
        val saveSink = saveLocation.toSink(fileName)
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
            namespaceId,
            atTime,
            parallelism,
          )
        Some(fileName)
      }.toEither
        .left
        .map {
          case _: InvalidPathException | _: FileAlreadyExistsException | _: SecurityException |
              _: FileSystemException =>
            Coproduct[ServerError :+: BadRequest :+: CNil](
              BadRequest(s"Invalid file name: $fileName"),
            ) // Return a Bad Request Error
          case e: CypherException =>
            Coproduct[ServerError :+: BadRequest :+: CNil](
              BadRequest(ErrorType.CypherError(s"Invalid query: ${e.getMessage}")),
            )
          case e: IllegalArgumentException =>
            Coproduct[ServerError :+: BadRequest :+: CNil](BadRequest(e.getMessage))
          case NonFatal(e) =>
            Coproduct[ServerError :+: BadRequest :+: CNil](
              toServerError(e),
            ) // Return an Internal Server Error
          case other =>
            Coproduct[ServerError :+: BadRequest :+: CNil](
              toServerError(other),
            ) // This might expose more than we want
        }
    }
  }

  /** Note: Duplicate implementation of [[AlgorithmRoutesImpl.algorithmRandomWalkRoute]] */
  def algorithmRandomWalk(
    qid: QuineId,
    lengthOpt: Option[Int],
    queryOpt: Option[String],
    returnParamOpt: Option[Double],
    inOutParamOpt: Option[Double],
    seedOpt: Option[String],
    namespaceId: NamespaceId,
    atTime: Option[Milliseconds],
  ): Future[Either[ServerError :+: BadRequest :+: CNil, List[String]]] = {

    val errors: Either[ServerError :+: BadRequest :+: CNil, List[String]] = Try {
      require(!lengthOpt.exists(_ < 1), "walk length cannot be less than one.")
      require(!inOutParamOpt.exists(_ < 0d), "in-out parameter cannot be less than zero.")
      require(!returnParamOpt.exists(_ < 0d), "return parameter cannot be less than zero.")
      Nil
    }.toEither.left
      .map {
        case e: CypherException =>
          Coproduct[ServerError :+: BadRequest :+: CNil](BadRequest(s"Invalid query: ${e.getMessage}"))
        case e: IllegalArgumentException =>
          Coproduct[ServerError :+: BadRequest :+: CNil](BadRequest(e.getMessage))
        case NonFatal(e) =>
          Coproduct[ServerError :+: BadRequest :+: CNil](
            toServerError(e),
          ) // Return an Internal Server Error
        case other =>
          Coproduct[ServerError :+: BadRequest :+: CNil](
            toServerError(other),
          ) // this might expose more than we want
      }
    if (errors.isLeft)
      Future.successful[Either[ServerError :+: BadRequest :+: CNil, List[String]]](errors)
    else {

      graph.requiredGraphIsReady()

      graph.algorithms
        .randomWalk(
          qid,
          compileWalkQuery(queryOpt),
          lengthOpt.getOrElse(AlgorithmGraph.defaults.walkLength),
          returnParamOpt.getOrElse(AlgorithmGraph.defaults.returnParam),
          inOutParamOpt.getOrElse(AlgorithmGraph.defaults.inOutParam),
          None,
          seedOpt,
          namespaceId,
          atTime,
        )
        .map(w => Right(w.acc))(ExecutionContext.parasitic)

    }
  }
}
