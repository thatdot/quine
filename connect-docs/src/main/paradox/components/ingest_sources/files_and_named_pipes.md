# Files and Named Pipes

Files are a stream of data. It's easy to think of a file as a singular chunk, but on disk it is a linear sequence of bits. Reading data from a file is the process of starting at the beginning, and reading each bit in sequence, until you reach the end. Programs usually do this under the hood and deliver a single result when finished, but that data can be handled by a program before the end of file is reached. Reading a file until a particular sequence is found (the "delimiter") will yield a sequence of bytes which can be handled as a single event. This is how data is streamed into Quine from a file—making it a very natural and convenient way to load data into Quine.

## File Ingest Format

Each file has one special feature which distinguishes it from a normal stream of data: it has an end. When ingesting data from one of these file types, the stream will continue until the end-of-file signal is reached. When the end-of-file signal is reached, the stream will finish and be marked by Quine as `COMPLETED`.

### JSON[L]

`"type" : "CypherJson"`

JSON data is ubiquitous; it is a very natural source of a data fora  streaming system. The typical approach is to save records as JSON objects to a file, separated by new-lines: `\n`. It is very common to call that format a `.json` file. That format is not technically valid JSON, and so sometimes it is referred to as "@link:[JSON Lines](https://jsonlines.org)" and given the `.jsonl` file extension.

Quine reads these `.json` or `.jsonl` files one line at a time, passing each parsed JSON object to the Cypher ingest query as `$that` to be used in the rest of the ingest query.

### CSV

`"type" : "CypherCsv"`

Comma-Separated Values are a convenient way to store tabular data. Quine reads CSV files natively, passing each parsed line into a Cypher query as: `$that`. Fields from each row are accessible by numeric index or by key, depending on whether `headers` are provided. The `headers` field can either be read from the first line of the CSV, provided manually as a list of strings, or disabled entirely. See the @ref[REST API reference](../../reference/rest_api.md) for details.

### Text

`"type" : "CypherLine"`

Any text file can be used as as stream of input to Quine. Using the `CypherLine` ingest option will read in the specified file, passing each individual line as a string to the provided Cypher query, where it can be used directly as a string value, or parsed by hand into a more meaningful structure.

## Named Pipe

A named pipe is an operating-system/file-system abstraction typically used for inter-process communication. A named pipe has a file-like representation in the filesystem—so it looks like a file to most programs—but it does not persist data. The operating system uses this object that looks like a file as a communication channel. One program can write to a named pipe, and if another program is actively reading from the same named pipe, then the second program receives the data written by the first program.

Quine supports named pipes on Unix-like operating systems (MacOS, Linux, etc), but not on Windows. On most Unix-like operating systems, Quine will automatically detect whether the provided path refers to a regular file or a named pipe. Automatic detection can be overridden by setting the `fileIngestMode` setting in the ingest API.

When Quine has an active stream ingesting from a named pipe, it will consume data from that pipe until the stream is cancelled, or it encounters an error. The format of the incoming data from the named pipe can be any of the @ref[File Ingest Formats](#file-ingest-format) listed above. Data written to the named pipe will be separated by new-line characters (`\n`) and passed to the provided Cypher query exactly as described above.
