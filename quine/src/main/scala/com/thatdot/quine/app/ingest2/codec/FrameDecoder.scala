package com.thatdot.quine.app.ingest2.codec

import java.nio.charset.StandardCharsets

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.{Success, Try}

import com.google.protobuf.{Descriptors, DynamicMessage}
import io.circe.{Json, parser}

import com.thatdot.quine.app.ingest2.core.{DataFoldableFrom, DataFolderTo}
import com.thatdot.quine.app.serialization.ProtobufSchemaCache
import com.thatdot.quine.graph.cypher
import com.thatdot.quine.graph.cypher.Value
import com.thatdot.quine.routes._
import com.thatdot.quine.util.StringInput.filenameOrUrl

trait FrameDecoder[A] {
  val foldable: DataFoldableFrom[A]

  def decode(bytes: Array[Byte]): Try[A]
}

object CypherStringDecoder extends FrameDecoder[cypher.Value] {
  val foldable: DataFoldableFrom[Value] = DataFoldableFrom.cypherValueDataFoldable

  def decode(bytes: Array[Byte]): Try[cypher.Value] =
    Success(cypher.Expr.Str(new String(bytes, StandardCharsets.UTF_8)))
}

object StringDecoder extends FrameDecoder[String] {
  val foldable: DataFoldableFrom[String] = DataFoldableFrom.stringDataFoldable

  def decode(bytes: Array[Byte]): Try[String] =
    Success(new String(bytes, StandardCharsets.UTF_8))
}

object CypherRawDecoder extends FrameDecoder[cypher.Value] {
  val foldable: DataFoldableFrom[Value] = DataFoldableFrom.cypherValueDataFoldable

  def decode(bytes: Array[Byte]): Try[cypher.Value] =
    Success(cypher.Expr.Bytes(bytes))
}

object JsonDecoder extends FrameDecoder[Json] {
  val foldable: DataFoldableFrom[Json] = DataFoldableFrom.jsonDataFoldable

  def decode(bytes: Array[Byte]): Try[Json] = {
    val decoded = new String(bytes, StandardCharsets.UTF_8)
    parser.parse(decoded).toTry
  }
}

object DropDecoder extends FrameDecoder[Any] {
  val foldable: DataFoldableFrom[Any] = new DataFoldableFrom[Any] {
    def fold[B](value: Any, folder: DataFolderTo[B]): B = folder.nullValue
  }

  def decode(bytes: Array[Byte]): Success[Any] = Success(())
}

case class ProtobufDecoder(query: String, parameter: String = "that", schemaUrl: String, typeName: String)(implicit
  protobufSchemaCache: ProtobufSchemaCache
) extends FrameDecoder[DynamicMessage] {

  // this is a blocking call, but it should only actually block until the first time a type is successfully
  // loaded.
  //
  // This was left as blocking because lifting the effect to a broader context would mean either:
  // - making ingest startup async, which would require extensive changes to QuineApp, startup, and potentially
  //   clustering protocols, OR
  // - making the decode bytes step of ingest async, which violates the Kafka APIs expectation that a
  //   `org.apache.kafka.common.serialization.Deserializer` is synchronous.
  val messageDescriptor: Descriptors.Descriptor = Await.result(
    protobufSchemaCache.getMessageDescriptor(filenameOrUrl(schemaUrl), typeName, flushOnFail = true),
    Duration.Inf
  )

  val foldable: DataFoldableFrom[DynamicMessage] = DataFoldableFrom.protobufDataFoldable

  def decode(bytes: Array[Byte]): Try[DynamicMessage] = Try(DynamicMessage.parseFrom(messageDescriptor, bytes))

}

object FrameDecoder {

  def apply(format: StreamedRecordFormat)(implicit protobufCache: ProtobufSchemaCache): FrameDecoder[_] =
    format match {
      case StreamedRecordFormat.CypherJson(_, _) => JsonDecoder
      case StreamedRecordFormat.CypherRaw(_, _) => CypherRawDecoder
      case StreamedRecordFormat.CypherProtobuf(query, parameter, schemaUrl, typeName) =>
        ProtobufDecoder(query, parameter, schemaUrl, typeName)
      case StreamedRecordFormat.Drop => DropDecoder
    }

  def apply(format: FileIngestFormat): FrameDecoder[_] =
    format match {
      case FileIngestFormat.CypherLine(_, _) => CypherStringDecoder
      case FileIngestFormat.CypherJson(_, _) => JsonDecoder
      case FileIngestFormat.CypherCsv(_, _, _, _, _, _) =>
        throw new UnsupportedOperationException(
          "Csv is parsed directly from files into cypher. Not supported for other types ATM."
        )

    }

}
