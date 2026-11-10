# Parquet test files

These files were generated for testing purposes using [parquet-java](https://github.com/apache/parquet-java) 1.18.1, which is the mainstream Parquet file writer for Java.

- `simple.parquet`: a small Parquet file for testing that Quine can read Parquet at all.
- `all-types.parquet`: a complex Parquet file attempting to maximally exercise the Parquet decoder.
- `codec-{compression_technique}.parquet`: files compressed with the supported `compression_technique`s.
- `malformed-map-key.parquet`: a malformed file containing a STRING map key whose bytes are not valid UTF-8. This is technically invalid according to the Parquet specification, though Hardwood decodes it with no issue, and Quine fails to "fold" the data into other formats like JSON.
- `corrupt-first-page.parquet`: a malformed file consisting of one row whose only row group is overwritten with `0xFF` bytes.
- `corrupt-later-page.parquet`: a malformed file that tests Hardwood's and Quine's ability to ingest some valid rows before failing at invalid data. It holds 200,000 rows of one wide (384-byte) column, and only the last 10,000 rows are invalid. The data must be big to prevent Hardwood from "reading ahead" to the invalid rows before handing any valid rows to Quine. ZSTD compression is used to keep the file size small.

`ParquetDecoderSpec` and `DeltaKernelReaderSpec` use these files as test data.