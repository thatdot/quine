package com.thatdot.quine.app.ingest.serialization

import java.net.URL

import scala.util.Try

import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.Timeout

import cats.implicits.toFunctorOps

import com.thatdot.quine.app.serialization.{ProtobufSchemaCache, QuineValueToProtobuf}
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
import com.thatdot.quine.model.QuineValue
import com.thatdot.quine.util.Log._
import com.thatdot.quine.util.MonadHelpers._
import com.thatdot.quine.util.StringInput.filenameOrUrl

class CypherToProtobuf(private val cache: ProtobufSchemaCache) extends UserDefinedProcedure with LazySafeLogging {
  def name: String = "toProtobuf"

  def canContainUpdates: Boolean = false

  def isIdempotent: Boolean = true

  def canContainAllNodeScan: Boolean = false

  def call(context: QueryContext, arguments: Seq[Value], location: ProcedureExecutionLocation)(implicit
    parameters: Parameters,
    timeout: Timeout,
    logConfig: LogConfig,
  ): Source[Vector[Value], _] = {
    val (value, schemaUrl, typeName): (Map[String, QuineValue], URL, String) = arguments match {
      case Seq(Expr.Map(value), Expr.Str(schemaUrl), Expr.Str(typeName)) =>
        (value.fmap(Expr.toQuineValue(_).getOrThrow), filenameOrUrl(schemaUrl), typeName)
      case _ =>
        throw wrongSignature(arguments)
    }

    Source
      .future(cache.getMessageDescriptor(schemaUrl, typeName, flushOnFail = true))
      .map(new QuineValueToProtobuf(_))
      .map { serializer =>
        val result: Value = Try(serializer.toProtobufBytes(value))
          .map {
            case Left(conversionFailures @ _) => Expr.Null
            case Right(value) => Expr.Bytes(value, representsId = false)
          }
          .recover { case _: IllegalArgumentException =>
            Expr.Null
          }
          .get
        Vector(result)
      }
  }

  def signature: UserDefinedProcedureSignature = UserDefinedProcedureSignature(
    arguments = Seq("value" -> Type.Map, "schemaUrl" -> Type.Str, "typeName" -> Type.Str),
    outputs = Seq("protoBytes" -> Type.Bytes),
    description = """Serializes a Cypher value into bytes, according to a protobuf schema.
                    |Returns null if the value is not serializable as the requested type
                    |""".stripMargin.replace('\n', ' ').trim,
  )
}
