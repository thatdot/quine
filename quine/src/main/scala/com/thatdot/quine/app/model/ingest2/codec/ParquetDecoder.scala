package com.thatdot.quine.app.model.ingest2.codec

import scala.util.Try

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.ActorAttributes
import org.apache.pekko.stream.scaladsl.Source

import com.github.mjakubowski84.parquet4s.{ParquetIterable, ParquetReader, RowParquetRecord}
import org.apache.parquet.io.{
  DelegatingSeekableInputStream,
  InputFile,
  SeekableInputStream => ParquetSeekableInputStream,
}

import com.thatdot.data.DataFoldableFrom

/** A [[RandomAccessDecoder]] for Apache Parquet files.
  *
  * Reads the file footer once to obtain the column schema, then iterates rows lazily,
  * pairing each row with its schema descriptors as a [[ParquetRecord]]. Downstream consumers
  * can fold each record through any [[com.thatdot.data.DataFolderTo]] without going through
  * a JSON intermediate — typed Parquet values (Long, bytes, dates, timestamps, etc.) land
  * directly on the folder's typed methods.
  *
  * The iteration runs on the blocking-io dispatcher since parquet-mr's reader does blocking
  * I/O against the input.
  */
final class ParquetDecoder extends RandomAccessDecoder[ParquetRecord] {

  override val dataFoldableFrom: DataFoldableFrom[ParquetRecord] = ParquetRecord.foldable

  override def decode(input: SeekableInput): Source[Try[ParquetRecord], NotUsed] = {
    val inputFile = ParquetDecoder.toParquetInputFile(input)
    val columnTypes = ParquetRecord.readColumnTypes(input)

    Source
      .unfoldResource[Try[ParquetRecord], (ParquetIterable[RowParquetRecord], Iterator[RowParquetRecord])](
        create = () => {
          val iterable = ParquetReader.generic.read(inputFile)
          (iterable, iterable.iterator)
        },
        read = { case (_, iter) =>
          if (iter.hasNext) Some(Try(ParquetRecord(iter.next(), columnTypes)))
          else None
        },
        close = { case (iterable, _) => iterable.close() },
      )
      .withAttributes(ActorAttributes.dispatcher("pekko.actor.default-blocking-io-dispatcher"))
  }
}

object ParquetDecoder {

  /** Adapt one of our [[SeekableInput]]s to parquet-mr's `InputFile`. Uses parquet-mr's
    * `DelegatingSeekableInputStream` to layer the convenience methods (`readFully` variants,
    * `ByteBuffer` reads) over our minimal `InputStream + getPos + seek` surface.
    */
  private[codec] def toParquetInputFile(input: SeekableInput): InputFile = new InputFile {
    override def getLength: Long = input.length
    override def newStream(): ParquetSeekableInputStream = {
      val inner = input.newStream()
      new DelegatingSeekableInputStream(inner) {
        override def getPos: Long = inner.position
        override def seek(newPos: Long): Unit = inner.seek(newPos)
      }
    }
  }
}
