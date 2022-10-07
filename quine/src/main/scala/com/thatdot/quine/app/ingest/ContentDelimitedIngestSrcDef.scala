package com.thatdot.quine.app.ingest

import scala.util.Success

import akka.NotUsed
import akka.stream.alpakka.csv.scaladsl.{CsvParsing, CsvToMap}
import akka.stream.contrib.SwitchMode
import akka.stream.scaladsl.{Flow, Framing, Source}
import akka.util.ByteString

import com.thatdot.quine.app.ingest.serialization.{
  CypherImportFormat,
  CypherJsonInputFormat,
  CypherRawInputFormat,
  CypherStringInputFormat
}
import com.thatdot.quine.graph.cypher.Value
import com.thatdot.quine.graph.{CypherOpsGraph, cypher}
import com.thatdot.quine.routes.FileIngestFormat
import com.thatdot.quine.routes.FileIngestFormat.{CypherCsv, CypherJson, CypherLine}

abstract class ContentDelimitedIngestSrcDef(
  initialSwitchMode: SwitchMode,
  format: CypherImportFormat,
  src: Source[ByteString, NotUsed],
  parallelism: Int,
  maximumLineSize: Int,
  startAtOffset: Long,
  ingestLimit: Option[Long],
  maxPerSecond: Option[Int],
  name: String
)(implicit graph: CypherOpsGraph)
    extends RawValuesIngestSrcDef(format, initialSwitchMode, parallelism, maxPerSecond, name) {

  type InputType = ByteString

  def bounded[A]: Flow[A, A, NotUsed] = ingestLimit match {
    case None => Flow[A].drop(startAtOffset)
    case Some(limit) => Flow[A].drop(startAtOffset).take(limit)
  }

  val newLineDelimited: Flow[ByteString, ByteString, NotUsed] = Framing
    .delimiter(ByteString("\n"), maximumLineSize, allowTruncation = true)
    .map(line => if (!line.isEmpty && line.last == '\r') line.dropRight(1) else line)

  override def rawBytes(value: ByteString): Array[Byte] = value.toArray

}

case class CsvIngestSrcDef(
  initialSwitchMode: SwitchMode,
  format: FileIngestFormat.CypherCsv,
  src: Source[ByteString, NotUsed],
  charset: String,
  parallelism: Int,
  maximumLineSize: Int,
  startAtOffset: Long,
  ingestLimit: Option[Long],
  maxPerSecond: Option[Int],
  override val name: String
)(implicit graph: CypherOpsGraph)
    extends ContentDelimitedIngestSrcDef(
      initialSwitchMode,
      new CypherRawInputFormat(format.query, format.parameter),
      src,
      parallelism,
      maximumLineSize,
      startAtOffset,
      ingestLimit,
      maxPerSecond,
      name
    ) {

  override def source(): Source[ByteString, NotUsed] = src.via(bounded)

  def csvHeadersFlow(headerDef: Either[Boolean, List[String]]): Flow[List[ByteString], Value, NotUsed] =
    headerDef match {
      case Right(h) =>
        CsvToMap
          .withHeaders(h: _*)
          .map(m => cypher.Expr.Map(m.mapValues(bs => cypher.Expr.Str(bs.decodeString(charset)))))
      case Left(true) =>
        CsvToMap
          .toMap()
          .map(m => cypher.Expr.Map(m.mapValues(bs => cypher.Expr.Str(bs.decodeString(charset)))))
      case Left(false) =>
        Flow[List[ByteString]]
          .map(l => cypher.Expr.List(l.map(bs => cypher.Expr.Str(bs.decodeString(charset))).toVector))
    }

  override val deserializeAndMeter: Flow[ByteString, TryDeserialized, NotUsed] =
    Flow[ByteString]
      .wireTap(bs => meter.mark(bs.length))
      .via(
        CsvParsing.lineScanner(format.delimiter.char, format.quoteChar.char, format.escapeChar.char, maximumLineSize)
      )
      .via(csvHeadersFlow(format.headers))
      // Here the empty bytestring is a placeholder the original
      // value in the TryDeserialized response value. Since this
      // is only used in errors and this is a success response,
      // it's not necessary to populate it.
      .map((t: Value) => (Success(t), ByteString.empty))
}

case class StringIngestSrcDef(
  initialSwitchMode: SwitchMode,
  format: CypherStringInputFormat,
  src: Source[ByteString, NotUsed],
  parallelism: Int,
  maximumLineSize: Int,
  startAtOffset: Long,
  ingestLimit: Option[Long],
  maxPerSecond: Option[Int],
  override val name: String
)(implicit graph: CypherOpsGraph)
    extends ContentDelimitedIngestSrcDef(
      initialSwitchMode,
      format,
      src,
      parallelism,
      maximumLineSize,
      startAtOffset,
      ingestLimit,
      maxPerSecond,
      name
    ) {

  override def source(): Source[ByteString, NotUsed] = src
    .via(bounded)
    .via(newLineDelimited)

}

case class JsonIngestSrcDef(
  initialSwitchMode: SwitchMode,
  format: CypherJsonInputFormat,
  src: Source[ByteString, NotUsed],
  encodingString: String,
  parallelism: Int,
  maximumLineSize: Int,
  startAtOffset: Long,
  ingestLimit: Option[Long],
  maxPerSecond: Option[Int],
  override val name: String
)(implicit graph: CypherOpsGraph)
    extends ContentDelimitedIngestSrcDef(
      initialSwitchMode,
      format,
      src,
      parallelism,
      maximumLineSize,
      startAtOffset,
      ingestLimit,
      maxPerSecond,
      name
    ) {

  val (charset, transcode) = IngestSrcDef.getTranscoder(encodingString)

  def source(): Source[ByteString, NotUsed] = src
    .via(bounded)
    .via(transcode)
    .via(newLineDelimited)

  override def rawBytes(value: ByteString): Array[Byte] = value.toArray

}

object ContentDelimitedIngestSrcDef {

  def apply(
    initialSwitchMode: SwitchMode,
    format: FileIngestFormat,
    src: Source[ByteString, NotUsed],
    encodingString: String,
    parallelism: Int,
    maximumLineSize: Int,
    startAtOffset: Long,
    ingestLimit: Option[Long],
    maxPerSecond: Option[Int],
    name: String
  )(implicit graph: CypherOpsGraph): ContentDelimitedIngestSrcDef =
    format match {
      case CypherLine(query, parameter) =>
        StringIngestSrcDef(
          initialSwitchMode,
          new CypherStringInputFormat(query, parameter, encodingString),
          src,
          parallelism,
          maximumLineSize,
          startAtOffset,
          ingestLimit,
          maxPerSecond,
          name
        )
      case CypherJson(query, parameter) =>
        JsonIngestSrcDef(
          initialSwitchMode,
          new CypherJsonInputFormat(query, parameter),
          src,
          encodingString,
          parallelism,
          maximumLineSize,
          startAtOffset,
          ingestLimit,
          maxPerSecond,
          name
        )

      case cv @ CypherCsv(_, _, _, _, _, _) =>
        CsvIngestSrcDef(
          initialSwitchMode,
          cv,
          src,
          encodingString,
          parallelism,
          maximumLineSize,
          startAtOffset,
          ingestLimit,
          maxPerSecond,
          name
        )
    }

}
