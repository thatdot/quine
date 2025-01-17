package com.thatdot.quine.app.ingest.serialization

import java.net.URL

import scala.util.Try

import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.Timeout

import com.google.protobuf.InvalidProtocolBufferException

import com.thatdot.common.logging.Log.{LazySafeLogging, LogConfig, Safe, SafeLoggableInterpolator}
import com.thatdot.common.logging.Pretty._
import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.app.serialization.ProtobufSchemaCache
import com.thatdot.quine.graph.cypher.{
  Expr,
  Parameters,
  ProcedureExecutionLocation,
  QueryContext,
  Type,
  UserDefinedProcedure,
  UserDefinedProcedureSignature,
  Value,
}
import com.thatdot.quine.util.StringInput.filenameOrUrl

/** Parse a protobuf message into a Cypher map according to a schema provided by a schema cache.
  * Because loading the schema is asynchronous, this must be a procedure rather than a function.
  */
class CypherParseProtobuf(private val cache: ProtobufSchemaCache) extends UserDefinedProcedure with LazySafeLogging {
  def name: String = "parseProtobuf"

  def canContainUpdates: Boolean = false

  def isIdempotent: Boolean = true

  def canContainAllNodeScan: Boolean = false

  def call(context: QueryContext, arguments: Seq[Value], location: ProcedureExecutionLocation)(implicit
    parameters: Parameters,
    timeout: Timeout,
    logConfig: LogConfig,
  ): Source[Vector[Value], _] = {
    implicit val prettyId: Pretty[QuineId] = location.idProvider
    val (bytes, schemaUrl, typeName): (Array[Byte], URL, String) = arguments match {
      case Seq(Expr.Bytes(bytes, bytesRepresentId), Expr.Str(schemaUrl), Expr.Str(typeName)) =>
        if (bytesRepresentId)
          logger.info(
            safe"""Received an ID (${Safe(QuineId(bytes).pretty)}) as a source of
                 |bytes to parse a protobuf value of type: ${Safe(typeName)}.""".cleanLines,
          )
        (bytes, filenameOrUrl(schemaUrl), typeName)
      case _ =>
        throw wrongSignature(arguments)
    }
    Source
      .future(cache.getMessageDescriptor(schemaUrl, typeName, flushOnFail = true))
      .map(new ProtobufParser(_))
      .map { parser =>
        val result = Try[Value](parser.parseBytes(bytes))
          // Ideally, this [[recover]] would match the configuration of the context in which the query was
          // run (eg, default to erroring in an ad-hoc query but default to returning null in an ingest, unless the
          // ingest is set to halt on error). However, we don't have that information here, so we default to
          // returning null.
          .recover {
            case e if e.isInstanceOf[ClassCastException] || e.isInstanceOf[InvalidProtocolBufferException] =>
              logger.warn(
                log"${Safe(name)} procedure received corrupted protobuf record -- returning null" withException e,
              )
              Expr.Null
          }.get
        Vector(result)
      }
  }

  def signature: UserDefinedProcedureSignature = UserDefinedProcedureSignature(
    arguments = Seq("bytes" -> Type.Bytes, "schemaUrl" -> Type.Str, "typeName" -> Type.Str),
    outputs = Seq("value" -> Type.Map),
    description =
      "Parses a protobuf message into a Cypher map value, or null if the bytes are not parseable as the requested type",
  )
}
