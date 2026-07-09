package com.thatdot.quine.language.functions

/** Documentation for one Cypher function Quine adds to the language.
  *
  * @param name the function's canonical (mixed-case) name, exactly as the runtime declares it
  * @param signature human-readable signature line(s) in the quine.io reference style
  *                  (`name(arg :: TYPE, ...) :: RESULTTYPE`), one overload per line
  * @param description what the function does, in Markdown
  * @param docsUrl canonical quine.io documentation URL for the function
  */
final case class FunctionDoc(name: String, signature: String, description: String, docsUrl: String) {

  /** The hover body for this function: the signature in a fenced code block, the description,
    * and a link to the quine.io documentation.
    */
  def markdown: String =
    s"```cypher\n$signature\n```\n\n$description\n\n[Documentation]($docsUrl)"
}

/** Registry of hover documentation for the Cypher functions Quine adds to the language, keyed
  * by function name.
  *
  * == Where the truth lives ==
  *
  * The functions themselves are declared and registered in quine-cypher
  * (`resolveFunctions.additionalFeatures` in `com.thatdot.quine.compiler.cypher.Functions`),
  * which depends on this module, so the names and signatures are mirrored here.
  * `FunctionDocRegistrySyncTest` in quine-cypher asserts name-by-name that this registry
  * matches the runtime registrations, so a function added, removed, or renamed there fails
  * that test until it is mirrored here. The descriptions follow the runtime declarations and
  * the quine.io reference pages the entries link to (the Quine Cypher function reference, the
  * temporal-functions reference, the Cypher-enhancements page, and the ID-provider page for
  * `idFrom`).
  *
  * Standard openCypher functions (`id`, `count`, `toInteger`, ...) are deliberately absent:
  * this registry documents only Quine's additions.
  *
  * == Name resolution ==
  *
  * The runtime registry stores user-defined functions under lowercased names
  * (`Func.userDefinedFunctions` in quine-core, whose keys "must be lowercase!") and lowercases
  * the invoked name on resolution, so [[lookup]] lowercases the requested name the same way.
  */
object FunctionDocRegistry {

  /** Looks up a function's documentation by name, case-insensitively. */
  def lookup(name: String): Option[FunctionDoc] =
    byLowerCaseName.get(name.toLowerCase)

  private val functionsReference = "https://quine.io/reference/cypher/cypher-functions/#custom-cypher-functions"
  private val temporalReference = "https://quine.io/reference/cypher/temporal-functions/"
  private val castingReference = "https://quine.io/reference/cypher/advanced-cypher/#casting-property-types"
  private val idFromReference = "https://quine.io/core-concepts/id-provider/#idfrom"

  /** The value types castable with the `castOrThrow.*` / `castOrNull.*` families: function-name
    * segment -> the result type as the quine.io reference writes it.
    */
  private val castableTypes: Vector[(String, String)] = Vector(
    "boolean" -> "BOOLEAN",
    "bytes" -> "BYTES",
    "datetime" -> "DATETIME",
    "duration" -> "DURATION",
    "float" -> "FLOAT",
    "integer" -> "INTEGER",
    "list" -> "LIST OF ANY",
    "localdatetime" -> "LOCALDATETIME",
    "map" -> "MAP",
    "node" -> "NODE",
    "path" -> "PATH",
    "relationship" -> "RELATIONSHIP",
    "string" -> "STRING",
  )

  /** The value types generable with the `gen.*.from` family. */
  private val generatableTypes: Vector[(String, String)] = Vector(
    "boolean" -> "BOOLEAN",
    "bytes" -> "BYTES",
    "float" -> "FLOAT",
    "integer" -> "INTEGER",
    "node" -> "NODE",
    "string" -> "STRING",
  )

  private val castOrThrowDocs: Vector[FunctionDoc] = castableTypes.map { case (segment, resultType) =>
    FunctionDoc(
      name = s"castOrThrow.$segment",
      signature = s"castOrThrow.$segment(value :: ANY) :: $resultType",
      description =
        s"Adds a runtime assertion that `value` is already a $resultType value, failing the query if it is not. " +
        "This recovers type information in cases where the Cypher compiler is unable to fully track types on " +
        "its own — most commonly when dealing with lists, due to Cypher's limited support for higher-kinded " +
        s"types. Use `castOrNull.$segment` to get `null` instead of an error on a type mismatch.",
      docsUrl = castingReference,
    )
  }

  private val castOrNullDocs: Vector[FunctionDoc] = castableTypes.map { case (segment, resultType) =>
    FunctionDoc(
      name = s"castOrNull.$segment",
      signature = s"castOrNull.$segment(value :: ANY) :: $resultType",
      description =
        s"Casts `value` to the type $resultType, returning `null` when the value is not already an instance of " +
        "that type. This recovers type information in cases where the Cypher compiler is unable to fully track " +
        "types on its own — most commonly when dealing with lists, due to Cypher's limited support for " +
        s"higher-kinded types. For functions that convert between types, see `toInteger` et al.; use " +
        s"`castOrThrow.$segment` to fail the query on a type mismatch instead.",
      docsUrl = castingReference,
    )
  }

  private val genFromDocs: Vector[FunctionDoc] = generatableTypes.map { case (segment, resultType) =>
    FunctionDoc(
      name = s"gen.$segment.from",
      signature = s"gen.$segment.from(fromValue :: ANY) :: $resultType\n" +
        s"gen.$segment.from(fromValue :: ANY, withSize :: INTEGER) :: $resultType",
      description =
        s"Deterministically generates a pseudo-random $resultType value from the provided input: the same " +
        "`fromValue` always generates the same result. The optional `withSize` bounds the size of the " +
        "generated value. Intended for generating sample or test data.",
      docsUrl = functionsReference,
    )
  }

  /** Every Quine-specific function's documentation, mirroring the runtime registrations cited
    * in the object documentation.
    */
  val all: Vector[FunctionDoc] = Vector(
    // IDs and hashing
    FunctionDoc(
      name = "idFrom",
      signature = "idFrom(input :: ANY, ...) :: ANY",
      description =
        "Deterministically produces a node ID from any number of argument values. Like a consistent-hashing " +
        "strategy, the same arguments always yield the same ID — but the result is always a valid ID in the " +
        "space of the configured ID provider (`long`, `uuid`, ...). Use it to locate or create the node " +
        "identified by your data without scanning the graph, e.g. " +
        "`MATCH (n) WHERE id(n) = idFrom('person', $name) RETURN n`.",
      docsUrl = idFromReference,
    ),
    FunctionDoc(
      name = "locIdFrom",
      signature = "locIdFrom(positionIdx :: INTEGER, input :: ANY, ...) :: ANY",
      description = "Generates a consistent ID from a hash of the `input` arguments, like `idFrom`, with the cluster " +
        "member that will manage the ID chosen by the provided position index given the cluster topology. " +
        "Requires a position-aware ID provider (`quine.id.partitioned = true`).",
      docsUrl = functionsReference,
    ),
    FunctionDoc(
      name = "quineId",
      signature = "quineId(input :: STRING) :: BYTES",
      description =
        "Returns the internal Quine ID (a bytes value) corresponding to the string representation of a node " +
        "ID, or `null` when the string is not a valid ID for the configured ID provider. The inverse of `strId`.",
      docsUrl = functionsReference,
    ),
    FunctionDoc(
      name = "strId",
      signature = "strId(node :: NODE) :: STRING",
      description = "Returns the string representation of the node's ID, in the format of the configured ID provider " +
        "(for example, the UUID string under the default `uuid` provider). Useful for displaying an ID or " +
        "passing it back into a query parameter.",
      docsUrl = functionsReference,
    ),
    FunctionDoc(
      name = "hash",
      signature = "hash(input :: ANY, ...) :: INTEGER",
      description =
        "Hashes any number of argument values into a single integer. Unlike `idFrom`, the result is a plain " +
        "integer rather than an ID in the configured ID provider's space.",
      docsUrl = functionsReference,
    ),
    FunctionDoc(
      name = "kafkaHash",
      signature = "kafkaHash(partitionKey :: STRING) :: INTEGER\nkafkaHash(partitionKey :: BYTES) :: INTEGER",
      description =
        "Hashes a string or bytes value to a 32-bit integer using the same murmur2-based algorithm Apache " +
        "Kafka's DefaultPartitioner uses, so the partition a record key lands on can be predicted from Cypher.",
      docsUrl = functionsReference,
    ),
    FunctionDoc(
      name = "getHost",
      signature = "getHost(node :: NODE) :: INTEGER\n" +
        "getHost(nodeIdStr :: STRING) :: INTEGER\n" +
        "getHost(nodeIdBytes :: BYTES) :: INTEGER",
      description = "Computes which cluster host a node — given directly, or as a node ID in string or bytes form — " +
        "should be assigned to. Returns `null` when the answer is not knowable without contacting the graph.",
      docsUrl = functionsReference,
    ),
    // Bytes and JSON
    FunctionDoc(
      name = "bytes",
      signature = "bytes(input :: STRING) :: BYTES",
      description =
        "Returns the bytes represented by a hexadecimal string (whitespace is ignored). Returns `null` when " +
        "the string contains characters that are not hexadecimal digits.",
      docsUrl = functionsReference,
    ),
    FunctionDoc(
      name = "convert.stringToBytes",
      signature = "convert.stringToBytes(input :: STRING, encoding :: STRING) :: BYTES",
      description =
        "Encodes a string into bytes according to the specified encoding: `UTF-8`, `UTF-16`, or `ISO-8859-1`. " +
        "Other encodings yield `null`.",
      docsUrl = functionsReference,
    ),
    FunctionDoc(
      name = "toJson",
      signature = "toJson(x :: ANY) :: STRING",
      description = "Returns the argument encoded as a JSON string. The inverse of `parseJson`.",
      docsUrl = functionsReference,
    ),
    FunctionDoc(
      name = "parseJson",
      signature = "parseJson(jsonStr :: STRING) :: ANY",
      description =
        "Parses a JSON string into the corresponding Cypher value: objects become maps, arrays become lists, " +
        "and JSON primitives become the matching scalar values. Handy for unpacking JSON carried in string " +
        "properties or ingested events.",
      docsUrl = functionsReference,
    ),
    FunctionDoc(
      name = "text.utf8Decode",
      signature = "text.utf8Decode(bytes :: BYTES) :: STRING",
      description = "Decodes a bytes value as a UTF-8 string. Invalid byte sequences are replaced with the Unicode " +
        "replacement character rather than failing.",
      docsUrl = functionsReference,
    ),
    FunctionDoc(
      name = "text.utf8Encode",
      signature = "text.utf8Encode(string :: STRING) :: BYTES",
      description = "Encodes a string as UTF-8 bytes.",
      docsUrl = functionsReference,
    ),
    // Maps
    FunctionDoc(
      name = "map.fromPairs",
      signature = "map.fromPairs(entries :: LIST OF LIST OF ANY) :: MAP",
      description =
        "Constructs a map from a list of `[key, value]` entries. Useful because Cypher map literals cannot " +
        "have dynamically computed keys.",
      docsUrl = functionsReference,
    ),
    FunctionDoc(
      name = "map.sortedProperties",
      signature = "map.sortedProperties(map :: MAP) :: LIST OF LIST OF ANY",
      description = "Extracts from a map the list of its `[key, value]` entries, sorted by key.",
      docsUrl = functionsReference,
    ),
    FunctionDoc(
      name = "map.removeKey",
      signature = "map.removeKey(map :: MAP, key :: STRING) :: MAP",
      description = "Returns the map with the entry for the given key removed.",
      docsUrl = functionsReference,
    ),
    FunctionDoc(
      name = "map.merge",
      signature = "map.merge(first :: MAP, second :: MAP) :: MAP",
      description = "Merges two maps; on duplicate keys, entries of `second` win.",
      docsUrl = functionsReference,
    ),
    FunctionDoc(
      name = "map.dropNullValues",
      signature = "map.dropNullValues(argument :: MAP) :: MAP",
      description = "Returns the map keeping only the entries whose values are not `null`.",
      docsUrl = functionsReference,
    ),
    // Text
    FunctionDoc(
      name = "text.split",
      signature = "text.split(text :: STRING, regex :: STRING) :: LIST OF STRING\n" +
        "text.split(text :: STRING, regex :: STRING, limit :: INTEGER) :: LIST OF STRING",
      description = "Splits the string around matches of the regular expression, optionally around only the first " +
        "`limit` matches.",
      docsUrl = functionsReference,
    ),
    FunctionDoc(
      name = "text.regexFirstMatch",
      signature = "text.regexFirstMatch(text :: STRING, regex :: STRING) :: LIST OF STRING",
      description =
        "Parses the string `text` using the regular expression `regex` and returns the first set of capture " +
        "group matches: element 0 is the whole matched text, element 1 the first capture group, and so on.",
      docsUrl = functionsReference,
    ),
    FunctionDoc(
      name = "text.regexGroups",
      signature = "text.regexGroups(text :: STRING, regex :: STRING) :: LIST OF LIST OF STRING",
      description = "Returns all matches of the regular expression `regex` in `text`, each as its list of capture " +
        "group values.",
      docsUrl = functionsReference,
    ),
    FunctionDoc(
      name = "text.regexReplaceAll",
      signature = "text.regexReplaceAll(text :: STRING, regex :: STRING, replacement :: STRING) :: STRING",
      description = "Replaces all matches of the regular expression `regex` in `text` with the `replacement` string. " +
        "Numbered capture groups may be referenced in the replacement with `$1`, `$2`, etc.",
      docsUrl = functionsReference,
    ),
    FunctionDoc(
      name = "text.urlencode",
      signature = "text.urlencode(text :: STRING) :: STRING\n" +
        "text.urlencode(text :: STRING, usePlusForSpace :: BOOLEAN) :: STRING\n" +
        "text.urlencode(text :: STRING, encodeExtraChars :: STRING) :: STRING\n" +
        "text.urlencode(text :: STRING, usePlusForSpace :: BOOLEAN, encodeExtraChars :: STRING) :: STRING",
      description = "URL-encodes the provided string, percent-encoding quotes, angle brackets, and curly braces in " +
        "addition to the RFC 3986 reserved characters. Optionally uses `+` for spaces instead of `%20`, " +
        "and percent-encodes any extra characters enumerated in `encodeExtraChars`.",
      docsUrl = functionsReference,
    ),
    FunctionDoc(
      name = "text.urldecode",
      signature = "text.urldecode(text :: STRING) :: STRING\n" +
        "text.urldecode(text :: STRING, decodePlusAsSpace :: BOOLEAN) :: STRING",
      description =
        "URL-decodes the provided string. By default `+` decodes as a space (x-www-form-urlencoded); pass " +
        "`decodePlusAsSpace = false` for strict RFC 3986 decoding. Returns `null` for invalid encoded input.",
      docsUrl = functionsReference,
    ),
    // Temporal
    FunctionDoc(
      name = "datetime",
      signature = "datetime() :: DATETIME\n" +
        "datetime(options :: MAP) :: DATETIME\n" +
        "datetime(datetime :: STRING) :: DATETIME\n" +
        "datetime(datetime :: STRING, format :: STRING) :: DATETIME",
      description =
        "Returns the current date-time, constructs one from an options map (`year`, `month`, `day`, `hour`, " +
        "..., `timezone`, `epochMillis`, ...), or parses one from a string — ISO-8601 by default, or a custom " +
        "Java `DateTimeFormatter` pattern. A DATETIME is an absolute moment at a specific offset from UTC.",
      docsUrl = temporalReference,
    ),
    FunctionDoc(
      name = "localdatetime",
      signature = "localdatetime() :: LOCALDATETIME\n" +
        "localdatetime(options :: MAP) :: LOCALDATETIME\n" +
        "localdatetime(datetime :: STRING) :: LOCALDATETIME\n" +
        "localdatetime(datetime :: STRING, format :: STRING) :: LOCALDATETIME",
      description = "Returns the current local date-time, constructs one from an options map, or parses one from a " +
        "string — ISO-8601 by default, or a custom Java `DateTimeFormatter` pattern. A LOCALDATETIME is a " +
        "date and time with no specific offset from UTC.",
      docsUrl = temporalReference,
    ),
    FunctionDoc(
      name = "date",
      signature = "date() :: DATE\n" +
        "date(options :: MAP) :: DATE\n" +
        "date(date :: STRING) :: DATE\n" +
        "date(date :: STRING, format :: STRING) :: DATE",
      description = "Returns the current date, constructs one from an options map (`year`, `month`, `day`, ...), or " +
        "parses one from a string — ISO-8601 by default, or a custom Java `DateTimeFormatter` pattern. A DATE " +
        "is an unzoned date with no time or offset information.",
      docsUrl = temporalReference,
    ),
    FunctionDoc(
      name = "time",
      signature = "time() :: TIME\n" +
        "time(options :: MAP) :: TIME\n" +
        "time(time :: STRING) :: TIME\n" +
        "time(time :: STRING, format :: STRING) :: TIME",
      description = "Returns the current time, constructs one from an options map (`hour`, `minute`, `second`, ..., " +
        "`offsetSeconds`), or parses one from a string — ISO-8601 by default, or a custom Java " +
        "`DateTimeFormatter` pattern. A TIME is a time at a specific offset from UTC.",
      docsUrl = temporalReference,
    ),
    FunctionDoc(
      name = "localtime",
      signature = "localtime() :: LOCALTIME\n" +
        "localtime(options :: MAP) :: LOCALTIME\n" +
        "localtime(time :: STRING) :: LOCALTIME\n" +
        "localtime(time :: STRING, format :: STRING) :: LOCALTIME",
      description = "Returns the current local time, constructs one from an options map (`hour`, `minute`, `second`, " +
        "...), or parses one from a string — ISO-8601 by default, or a custom Java `DateTimeFormatter` " +
        "pattern. A LOCALTIME is a time with no specific offset from UTC.",
      docsUrl = temporalReference,
    ),
    FunctionDoc(
      name = "duration",
      signature = "duration(options :: MAP) :: DURATION\nduration(duration :: STRING) :: DURATION",
      description =
        "Constructs a duration from an options map of components (`years`, `months`, `days`, `hours`, ..., " +
        "`nanoseconds`) or parses one from an ISO-8601 duration string such as `\"P1DT2H\"`.",
      docsUrl = temporalReference,
    ),
    FunctionDoc(
      name = "duration.between",
      signature = "duration.between(date1 :: LOCALDATETIME, date2 :: LOCALDATETIME) :: DURATION\n" +
        "duration.between(date1 :: DATETIME, date2 :: DATETIME) :: DURATION",
      description = "Computes the duration between two temporal values of the same type.",
      docsUrl = temporalReference,
    ),
    FunctionDoc(
      name = "temporal.format",
      signature = "temporal.format(date :: DATETIME, format :: STRING) :: STRING\n" +
        "temporal.format(date :: LOCALDATETIME, format :: STRING) :: STRING",
      description = "Converts a date-time into a string using the given Java `DateTimeFormatter` pattern.",
      docsUrl = temporalReference,
    ),
    // Collections and introspection
    FunctionDoc(
      name = "coll.max",
      signature = "coll.max(value :: ANY, ...) :: ANY",
      description = "Computes the maximum value: of the elements of a single list argument, or of the arguments " +
        "themselves when more than one is given.",
      docsUrl = functionsReference,
    ),
    FunctionDoc(
      name = "coll.min",
      signature = "coll.min(value :: ANY, ...) :: ANY",
      description = "Computes the minimum value: of the elements of a single list argument, or of the arguments " +
        "themselves when more than one is given.",
      docsUrl = functionsReference,
    ),
    FunctionDoc(
      name = "meta.type",
      signature = "meta.type(value :: ANY) :: STRING",
      description = "Inspects a value and returns the name of its Cypher type.",
      docsUrl = functionsReference,
    ),
  ) ++ genFromDocs ++ castOrThrowDocs ++ castOrNullDocs

  private val byLowerCaseName: Map[String, FunctionDoc] =
    all.map(doc => doc.name.toLowerCase -> doc).toMap
}
