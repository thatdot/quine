package com.thatdot.quine.app.model.ingest2.codec

import com.github.mjakubowski84.parquet4s.RowParquetRecord
import io.circe.Json

import com.thatdot.data.DataFolderTo

/** Thin convenience wrappers for callers that specifically want a Parquet record as JSON
  * (e.g. legacy DLQ formats, downstream HTTP responses).
  *
  * The canonical representation is [[ParquetRecord]], whose [[ParquetRecord.foldable]] is
  * how most call sites should consume Parquet data — folding through a destination-specific
  * `DataFolderTo` preserves typed values (Long, bytes, dates, timestamps) that the JSON
  * intermediate flattens. BINARY values (raw and BSON) fold through `DataFolderTo[Json].bytes`,
  * which emits standard base64.
  */
object ParquetRecordToJson {

  /** Read the Parquet file schema and return a map from column name to its type descriptor. */
  def readColumnTypes(input: SeekableInput): Map[String, ParquetRecord.ColumnDescriptor] =
    ParquetRecord.readColumnTypes(input)

  /** Fold a Parquet record to a circe `Json` using its schema-derived type descriptors. */
  def recordToJson(record: RowParquetRecord, columnTypes: Map[String, ParquetRecord.ColumnDescriptor]): Json =
    ParquetRecord.foldable.fold(ParquetRecord(record, columnTypes), DataFolderTo[Json])
}
