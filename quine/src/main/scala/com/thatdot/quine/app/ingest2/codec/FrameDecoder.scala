package com.thatdot.quine.app.ingest2.codec

import java.io.StringReader
import java.nio.charset.{Charset, StandardCharsets}

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._
import scala.util.{Success, Try}

import com.google.protobuf.{Descriptors, DynamicMessage}
import io.circe.{Json, parser}
import org.apache.avro.Schema
import org.apache.avro.file.SeekableByteArrayInput
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory
import org.apache.commons.csv.CSVFormat

import com.thatdot.language.ast
import com.thatdot.quine.app.ingest2.V2IngestEntities.{FileFormat, IngestFormat => V2IngestFormat, StreamingFormat}
import com.thatdot.quine.app.ingest2.core.{DataFoldableFrom, DataFolderTo}
import com.thatdot.quine.app.ingest2.sources.DEFAULT_CHARSET
import com.thatdot.quine.app.serialization.{AvroSchemaCache, ProtobufSchemaCache}
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

object QuinePatternStringDecoder extends FrameDecoder[ast.Value] {
  val foldable: DataFoldableFrom[com.thatdot.language.ast.Value] = new DataFoldableFrom[ast.Value] {
    override def fold[B](value: ast.Value, folder: DataFolderTo[B]): B =
      value match {
        case ast.Value.Null => folder.nullValue
        case ast.Value.True => folder.trueValue
        case ast.Value.False => folder.falseValue
        case ast.Value.Integer(n) => folder.integer(n)
        case ast.Value.Real(d) => folder.floating(d)
        case ast.Value.Text(str) => folder.string(str)
        case ast.Value.DateTime(zdt) => folder.zonedDateTime(zdt)
        case ast.Value.List(values) => ???
        case ast.Value.Map(values) => ???
        case ast.Value.NodeId(id) => ???
        case ast.Value.Node(_, _, _) => ???
      }
  }

  def decode(bytes: Array[Byte]): Try[com.thatdot.language.ast.Value] =
    Success(ast.Value.Text(new String(bytes, StandardCharsets.UTF_8)))
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

case class ProtobufDecoder(schemaUrl: String, typeName: String)(implicit
  protobufSchemaCache: ProtobufSchemaCache,
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
    Duration.Inf,
  )

  val foldable: DataFoldableFrom[DynamicMessage] = DataFoldableFrom.protobufDataFoldable

  def decode(bytes: Array[Byte]): Try[DynamicMessage] = Try(DynamicMessage.parseFrom(messageDescriptor, bytes))

}

case class AvroDecoder(schemaUrl: String)(implicit schemaCache: AvroSchemaCache) extends FrameDecoder[GenericRecord] {

  // this is a blocking call, but it should only actually block until the first time a type is successfully
  // loaded.
  //
  // This was left as blocking because lifting the effect to a broader context would mean either:
  // - making ingest startup async, which would require extensive changes to QuineApp, startup, and potentially
  //   clustering protocols, OR
  // - making the decode bytes step of ingest async, which violates the Kafka APIs expectation that a
  //   `org.apache.kafka.common.serialization.Deserializer` is synchronous.
  val schema: Schema = Await.result(
    schemaCache.getSchema(filenameOrUrl(schemaUrl)),
    Duration.Inf,
  )

  val foldable: DataFoldableFrom[GenericRecord] = DataFoldableFrom.avroDataFoldable

  def decode(bytes: Array[Byte]): Try[GenericRecord] = Try {
    val datumReader = new GenericDatumReader[GenericRecord](schema)
    val inputStream = new SeekableByteArrayInput(bytes)
    val decoder = DecoderFactory.get.binaryDecoder(inputStream, null)
    datumReader.read(null, decoder)
  }

}

case class CsvVecDecoder(delimiterChar: Char, quoteChar: Char, escapeChar: Char, charset: Charset = DEFAULT_CHARSET)
    extends FrameDecoder[Iterable[String]] {

  val csvFormat: CSVFormat =
    CSVFormat.Builder
      .create()
      .setQuote(quoteChar)
      .setDelimiter(delimiterChar)
      .setEscape(escapeChar)
      .setHeader()
      .get()

  override val foldable: DataFoldableFrom[Iterable[String]] = DataFoldableFrom.stringIterableDataFoldable
  override def decode(bytes: Array[Byte]): Try[Iterable[String]] =
    Try(csvFormat.parse(new StringReader(new String(bytes, charset))).getHeaderNames.asScala)
}

case class CsvMapDecoder(
  keys: Option[Iterable[String]],
  delimiterChar: Char,
  quoteChar: Char,
  escapeChar: Char,
  charset: Charset = DEFAULT_CHARSET,
) extends FrameDecoder[Map[String, String]] {

  //if the keys are not passed in the first read values are the keys
  var headers: Option[Iterable[String]] = keys

  val vecDecoder: CsvVecDecoder = CsvVecDecoder(delimiterChar, quoteChar, escapeChar, charset)

  override val foldable: DataFoldableFrom[Map[String, String]] = DataFoldableFrom.stringMapDataFoldable
  override def decode(bytes: Array[Byte]): Try[Map[String, String]] =
    vecDecoder
      .decode(bytes)
      .map((csv: Iterable[String]) =>
        headers match {
          case Some(value) => value.zip(csv).toMap
          case None => throw new Exception("Headers are empty")
        },
      )

}
object FrameDecoder {

  def apply(
    format: V2IngestFormat,
  )(implicit protobufCache: ProtobufSchemaCache, avroSchemaCache: AvroSchemaCache): FrameDecoder[_] = format match {
    case FileFormat.LineFormat => CypherStringDecoder
    case FileFormat.JsonFormat | StreamingFormat.JsonFormat => JsonDecoder
    case FileFormat.CsvFormat(headers, delimiter, quoteChar, escapeChar) =>
      headers match {
        case Left(false) =>
          CsvVecDecoder(delimiter.byte.toChar, quoteChar.byte.toChar, escapeChar.byte.toChar) // no headers
        case Left(true) =>
          CsvMapDecoder(
            None,
            delimiter.byte.toChar,
            quoteChar.byte.toChar,
            escapeChar.byte.toChar,
          ) // first line as header
        case Right(values) =>
          CsvMapDecoder(
            Some(values),
            delimiter.byte.toChar,
            quoteChar.byte.toChar,
            escapeChar.byte.toChar,
          )
      }

    case StreamingFormat.RawFormat => CypherRawDecoder
    case StreamingFormat.ProtobufFormat(schemaUrl, typeName) =>
      ProtobufDecoder(schemaUrl, typeName)
    case StreamingFormat.AvroFormat(schemaUrl) =>
      AvroDecoder(schemaUrl)
    case StreamingFormat.DropFormat => DropDecoder

  }

  def apply(v1Format: StreamedRecordFormat)(implicit protobufCache: ProtobufSchemaCache): FrameDecoder[_] =
    v1Format match {
      case StreamedRecordFormat.CypherJson(_, _) => JsonDecoder
      case StreamedRecordFormat.CypherRaw(_, _) => CypherRawDecoder
      case StreamedRecordFormat.CypherProtobuf(_, _, schemaUrl, typeName) =>
        ProtobufDecoder(schemaUrl, typeName)
      case StreamedRecordFormat.Drop => DropDecoder
      //note: V1 format does not support avro
    }

  def apply(v1Format: FileIngestFormat): FrameDecoder[_] =
    v1Format match {
      case FileIngestFormat.CypherLine(_, _) => CypherStringDecoder
      case FileIngestFormat.QuinePatternLine(_, _) => QuinePatternStringDecoder
      case FileIngestFormat.CypherJson(_, _) => JsonDecoder
      case FileIngestFormat.QuinePatternJson(_, _) => JsonDecoder
      case FileIngestFormat.CypherCsv(_, _, headers, delimiter, quote, escape) =>
        headers match {
          case Left(false) => CsvVecDecoder(delimiter.byte.toChar, quote.byte.toChar, escape.byte.toChar) // no headers
          case Left(true) =>
            CsvMapDecoder(None, delimiter.byte.toChar, quote.byte.toChar, escape.byte.toChar) // first line as header
          case Right(values) =>
            CsvMapDecoder(
              Some(values),
              delimiter.byte.toChar,
              quote.byte.toChar,
              escape.byte.toChar,
            ) // map values provided
        }
    }

}
