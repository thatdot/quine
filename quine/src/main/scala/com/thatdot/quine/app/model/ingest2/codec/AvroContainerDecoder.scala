package com.thatdot.quine.app.model.ingest2.codec

import scala.concurrent.Await
import scala.concurrent.duration.{Duration, DurationInt}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.ActorAttributes
import org.apache.pekko.stream.scaladsl.{Flow, Source, StreamConverters}
import org.apache.pekko.util.ByteString

import org.apache.avro.file.DataFileStream
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}

import com.thatdot.data.DataFoldableFrom
import com.thatdot.quine.serialization.AvroSchemaCache
import com.thatdot.quine.util.StringInput.filenameOrUrl

/** Wraps a `DataFileStream` with error handling that varies based on whether any record
  * has been successfully read.
  *
  * Before first success: failures propagate as exceptions, crashing the stream so any
  * wrapping retry mechanism can re-materialize the upstream byte source (re-fetching the
  * schema URL, re-opening the file). This handles the case where a stale or incorrect
  * schema was used.
  *
  * After first success: the first failure is wrapped in `Try` and emitted as a stream
  * element for DLQ routing, then the iterator terminates.
  */
private[codec] class SafeAvroIterator(dfs: DataFileStream[GenericRecord]) {
  private var hasSucceeded = false
  private var terminated = false

  def next(): Option[Try[GenericRecord]] =
    if (terminated) None
    else
      Try(dfs.hasNext) match {
        case Success(true) =>
          Try(dfs.next()) match {
            case s @ Success(_) =>
              hasSucceeded = true
              Some(s)
            case Failure(e) if !hasSucceeded => throw e
            case f @ Failure(_) =>
              terminated = true
              Some(f)
          }
        case Success(false) => None
        case Failure(e) if !hasSucceeded => throw e
        case Failure(e) =>
          terminated = true
          Some(Failure(e))
      }

  def close(): Unit =
    try dfs.close()
    catch { case NonFatal(_) => }
}

/** A [[StreamingDecoder]] for Apache Avro Object Container Files.
  *
  * @param readerSchemaUrl optional URL of a reader schema to use for schema evolution /
  *                         projection. If absent, the writer schema embedded in the file
  *                         header is used.
  */
final class AvroContainerDecoder(readerSchemaUrl: Option[String])(implicit
  schemaCache: AvroSchemaCache,
) extends StreamingDecoder[GenericRecord] {

  override val dataFoldableFrom: DataFoldableFrom[GenericRecord] = DataFoldableFrom.avroDataFoldable

  override def decodeFlow: Flow[ByteString, Try[GenericRecord], NotUsed] =
    Flow
      .fromMaterializer { (mat, _) =>
        val (inputStream, byteSink) =
          StreamConverters.asInputStream(readTimeout = 1.minute).preMaterialize()(mat)

        val recordSource: Source[Try[GenericRecord], NotUsed] =
          Source
            .unfoldResource[Try[GenericRecord], SafeAvroIterator](
              create = () => {
                val reader = makeDatumReader()
                val dfs =
                  try new DataFileStream(inputStream, reader)
                  catch {
                    case NonFatal(e) =>
                      try inputStream.close()
                      catch { case NonFatal(_) => }
                      throw e
                  }
                new SafeAvroIterator(dfs)
              },
              read = _.next(),
              close = _.close(),
            )
            .withAttributes(ActorAttributes.dispatcher("pekko.actor.default-blocking-io-dispatcher"))

        // Uncoupled, NOT Coupled, because Coupled completes the source side as soon as the sink
        // side completes (upstream EOF). That would cut off record emission while the decoder
        // is still draining the InputStream's already-buffered bytes. With uncoupled completion,
        // the byteSink completes → asInputStream's queue drains and signals EOF → DataFileStream
        // finishes naturally and the recordSource then completes on its own.
        Flow.fromSinkAndSource(byteSink, recordSource)
      }
      .mapMaterializedValue(_ => NotUsed)

  private def makeDatumReader(): GenericDatumReader[GenericRecord] =
    readerSchemaUrl match {
      case Some(url) =>
        val schema = Await.result(schemaCache.getSchema(filenameOrUrl(url)), Duration.Inf)
        new GenericDatumReader[GenericRecord](schema)
      case None =>
        new GenericDatumReader[GenericRecord]()
    }
}
