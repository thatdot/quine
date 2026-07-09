package com.thatdot.quine.language.procedures

/** Documentation for one Cypher procedure Quine ships.
  *
  * @param name the procedure's canonical (mixed-case) name, exactly as the runtime declares it
  * @param signature human-readable signature line in the quine.io reference style
  *                  (`name(arg :: TYPE, ...) :: (column :: TYPE, ...)`, or `:: VOID` for a
  *                  procedure with no output columns)
  * @param description what the procedure does, in Markdown
  * @param docsUrl canonical quine.io documentation URL for the procedure
  */
final case class ProcedureDoc(name: String, signature: String, description: String, docsUrl: String) {

  /** The hover body for this procedure: the signature in a fenced code block, the description,
    * and a link to the quine.io documentation.
    */
  def markdown: String =
    s"```cypher\n$signature\n```\n\n$description\n\n[Documentation]($docsUrl)"
}

/** Registry of hover documentation for the Cypher procedures Quine ships, keyed by procedure
  * name.
  *
  * == Where the truth lives ==
  *
  * The procedures themselves are declared in the runtime modules, which depend on this module
  * (quine-language is upstream of both), so the names, signatures, and descriptions are
  * mirrored here from the same sources [[ProcedureRegistry]] mirrors output signatures from:
  * `resolveCalls.builtInProcedures` in quine-cypher (each procedure's
  * `UserDefinedProcedureSignature` carries the `arguments`, `outputs`, and `description`
  * rendered here), the procedures the application registers at startup (`standing.wiretap`,
  * `parseProtobuf`, `toProtobuf`), and the QuinePattern procedure registry in quine-core
  * (`help.builtins`). `ProcedureDocRegistrySyncTest` in quine-cypher pins this registry's
  * names against the runtime declarations and [[ProcedureRegistry]] in both directions, so a
  * procedure added, removed, or renamed there fails that test until it is mirrored here. The
  * descriptions follow the runtime declarations and the quine.io reference pages the entries
  * link to (the Quine Cypher procedure reference and the time-reification reference for
  * `reify.time`).
  *
  * == Name resolution ==
  *
  * The runtime registries store and resolve procedures by lowercased name
  * (`resolveCalls.rewriteCall` in quine-cypher, `QuinePatternProcedureRegistry.get` in
  * quine-core), so [[lookup]] lowercases the requested name the same way.
  */
object ProcedureDocRegistry {

  /** Looks up a procedure's documentation by name, case-insensitively. */
  def lookup(name: String): Option[ProcedureDoc] =
    byLowerCaseName.get(name.toLowerCase)

  private val proceduresReference = "https://quine.io/reference/cypher/cypher-procedures/"
  private val reifyTimeReference = "https://quine.io/reference/cypher/reify-time/"

  /** A compatibility stub's documentation: Quine accepts the call so external tools written
    * for Neo4j keep working, but the procedure always yields no rows.
    */
  private def stubDoc(name: String, signature: String, wouldList: String): ProcedureDoc =
    ProcedureDoc(
      name = name,
      signature = signature,
      description = s"Compatibility stub for Neo4j's `$name`, which lists $wouldList. Quine accepts the " +
        "call so external tools that introspect the database keep working, but it always yields no rows.",
      docsUrl = proceduresReference,
    )

  /** Every shipped procedure's documentation, mirroring the runtime declarations cited in the
    * object documentation.
    */
  val all: Vector[ProcedureDoc] = Vector(
    // Stubs for compatibility with external systems (StubbedUserDefinedProcedure)
    stubDoc(
      "db.indexes",
      "db.indexes() :: (description :: ANY, indexName :: ANY, tokenNames :: ANY, properties :: ANY, " +
      "state :: ANY, type :: ANY, progress :: ANY, provider :: ANY, id :: ANY, failureMessage :: ANY)",
      "the database's indexes",
    ),
    stubDoc(
      "db.relationshipTypes",
      "db.relationshipTypes() :: (relationshipType :: ANY)",
      "the relationship types in use",
    ),
    stubDoc(
      "db.propertyKeys",
      "db.propertyKeys() :: (propertyKey :: ANY)",
      "the property keys in use",
    ),
    stubDoc(
      "dbms.labels",
      "dbms.labels() :: (label :: ANY)",
      "the node labels in use",
    ),
    // Introspection
    ProcedureDoc(
      name = "help.builtins",
      signature = "help.builtins() :: (name :: STRING, signature :: STRING, description :: STRING)",
      description = "Lists the built-in Cypher functions (`abs`, `coalesce`, `toInteger`, ...), yielding one row " +
        "per function with its name, signature, and description.",
      docsUrl = proceduresReference,
    ),
    ProcedureDoc(
      name = "help.functions",
      signature = "help.functions() :: (name :: STRING, signature :: STRING, description :: STRING)",
      description = "Lists the registered Cypher functions, including Quine's additions (`idFrom`, `parseJson`, " +
        "...), yielding one row per function with its name, signature, and description.",
      docsUrl = proceduresReference,
    ),
    ProcedureDoc(
      name = "help.procedures",
      signature = "help.procedures() :: (name :: STRING, signature :: STRING, description :: STRING, mode :: STRING)",
      description = "Lists the registered Cypher procedures, yielding one row per procedure with its name, " +
        "signature, description, and mode.",
      docsUrl = proceduresReference,
    ),
    ProcedureDoc(
      name = "debug.node",
      signature = "debug.node(node :: ANY) :: (atTime :: LOCALDATETIME, properties :: MAP, " +
        "edges :: LIST OF ANY, latestUpdateMillisAfterSnapshot :: INTEGER, subscribers :: STRING, " +
        "subscriptions :: STRING, multipleValuesStandingQueryStates :: LIST OF ANY, journal :: LIST OF ANY, " +
        "graphNodeHashCode :: INTEGER)",
      description = "Returns comprehensive internal state of a node, including its properties, edges, standing query " +
        "states, and event journal. Useful for debugging why standing queries match or don't match. The node " +
        "may be given directly or by its ID.",
      docsUrl = proceduresReference,
    ),
    ProcedureDoc(
      name = "subscribers",
      signature = "subscribers(node :: ANY) :: (queryId :: INTEGER, queryDepth :: INTEGER, " +
        "receiverId :: STRING, lastResult :: ANY)",
      description = "Returns the nodes subscribed to this node for standing query updates, one row per " +
        "subscriber with the query, depth, receiver, and last propagated result. Useful for tracing how a " +
        "standing query propagates through the graph; `subscriptions` answers the opposite direction.",
      docsUrl = proceduresReference,
    ),
    ProcedureDoc(
      name = "subscriptions",
      signature = "subscriptions(node :: ANY) :: (queryId :: INTEGER, queryDepth :: INTEGER, " +
        "receiverId :: STRING, lastResult :: ANY)",
      description = "Returns the nodes this node subscribes to for standing query updates, one row per " +
        "subscription with the query, depth, receiver, and last propagated result. Useful for tracing how a " +
        "standing query propagates through the graph; `subscribers` answers the opposite direction.",
      docsUrl = proceduresReference,
    ),
    // Nested-query execution
    ProcedureDoc(
      name = "do.when",
      signature = "do.when(condition :: BOOLEAN, ifQuery :: STRING, elseQuery :: STRING, params :: MAP) " +
        ":: (value :: MAP)",
      description = "Executes `ifQuery` when the condition is true and `elseQuery` otherwise. The queries are Cypher " +
        "source strings run with the given parameters; each result row of the executed query is yielded as a " +
        "`value` map.",
      docsUrl = proceduresReference,
    ),
    ProcedureDoc(
      name = "cypher.doIt",
      signature = "cypher.doIt(cypher :: STRING, params :: MAP) :: (value :: MAP)",
      description =
        "Executes a Cypher query given as a source string with the given parameters. Each result row of the " +
        "executed query is yielded as a `value` map. Useful for running a query that is assembled dynamically.",
      docsUrl = proceduresReference,
    ),
    ProcedureDoc(
      name = "cypher.do.case",
      signature = "cypher.do.case(conditionals :: LIST OF ANY, elseQuery :: STRING, params :: MAP) " +
        ":: (value :: MAP)",
      description = "Given a list of alternating condition/query pairs, executes the first query whose condition is " +
        "true, falling back to `elseQuery` when none is. The queries are Cypher source strings run with the " +
        "given parameters; each result row of the executed query is yielded as a `value` map.",
      docsUrl = proceduresReference,
    ),
    ProcedureDoc(
      name = "cypher.runTimeboxed",
      signature = "cypher.runTimeboxed(cypher :: STRING, params :: MAP, timeout :: INTEGER) :: (value :: MAP)",
      description =
        "Executes a Cypher query given as a source string with the given parameters, aborting after `timeout` " +
        "milliseconds. Each result row produced before the deadline is yielded as a `value` map.",
      docsUrl = proceduresReference,
    ),
    // Graph reads
    ProcedureDoc(
      name = "recentNodes",
      signature = "recentNodes(count :: INTEGER) :: (node :: NODE)",
      description =
        "Fetches the specified number of nodes from the in-memory cache — the nodes the graph has touched " +
        "most recently. A quick way to sample what is currently active, e.g. `CALL recentNodes(10)`.",
      docsUrl = proceduresReference,
    ),
    ProcedureDoc(
      name = "recentNodeIds",
      signature = "recentNodeIds(count :: INTEGER) :: (nodeId :: ANY)",
      description =
        "Fetches the specified number of node IDs from the in-memory cache — the IDs of the nodes the graph " +
        "has touched most recently. Like `recentNodes`, but yields the IDs instead of the nodes themselves.",
      docsUrl = proceduresReference,
    ),
    ProcedureDoc(
      name = "getFilteredEdges",
      signature = "getFilteredEdges(node :: ANY, edgeTypes :: LIST OF STRING, directions :: LIST OF STRING, " +
        "allowedNodes :: LIST OF ANY) :: (edge :: RELATIONSHIP)",
      description =
        "Gets the edges of a node filtered by edge type, direction, and/or allowed destination nodes. Each " +
        "filter is a list, and an empty list leaves that aspect unconstrained — e.g. " +
        "`CALL getFilteredEdges(n, [\"FRIEND\"], [], [])` yields every FRIEND edge of `n` in either direction.",
      docsUrl = proceduresReference,
    ),
    ProcedureDoc(
      name = "random.walk",
      signature = "random.walk(start :: ANY, depth :: INTEGER, return :: FLOAT, in-out :: FLOAT, " +
        "seed :: STRING) :: (walk :: LIST OF STRING)",
      description =
        "Randomly walks edges from a starting node for a chosen depth, yielding the list of node IDs in the " +
        "order they were encountered. The `return` and `in-out` parameters bias the walk like node2vec's p " +
        "and q, and a `seed` makes the walk reproducible.",
      docsUrl = proceduresReference,
    ),
    // Graph writes
    ProcedureDoc(
      name = "create.relationship",
      signature = "create.relationship(from :: NODE, relType :: STRING, props :: MAP, to :: NODE) " +
        ":: (rel :: RELATIONSHIP)",
      description = "Creates a relationship whose type is computed at runtime, yielding the created relationship. " +
        "Cypher's CREATE clause only accepts literal relationship types, so use this when the type comes " +
        "from data.",
      docsUrl = proceduresReference,
    ),
    ProcedureDoc(
      name = "create.setProperty",
      signature = "create.setProperty(node :: NODE, key :: STRING, value :: ANY) :: VOID",
      description = "Sets the property with the provided key on the specified node. Cypher's SET clause only accepts " +
        "literal property keys, so use this when the key comes from data. Yields no rows.",
      docsUrl = proceduresReference,
    ),
    ProcedureDoc(
      name = "create.setLabels",
      signature = "create.setLabels(node :: NODE, labels :: LIST OF STRING) :: VOID",
      description =
        "Sets the labels on the specified node to exactly the provided list, overriding any previously set " +
        "labels. Yields no rows.",
      docsUrl = proceduresReference,
    ),
    ProcedureDoc(
      name = "reify.time",
      signature = "reify.time(timestamp :: DATETIME, periods :: LIST OF STRING) :: (node :: NODE)",
      description = "Reifies the timestamp into a graph of time nodes, where each node represents one period at the " +
        "granularity of a period specifier: `year`, `month`, `day`, `hour`, `minute`, or `second`. Each time " +
        "node records the start of its period, links to the node of the containing (coarser) period, and " +
        "links to the next node in time at its own granularity, so events anchored to time nodes can be " +
        "navigated by time. Both arguments are optional — `timestamp` defaults to the current moment and " +
        "`periods` to all six granularities — and the yielded `node` rows are the reified nodes of the " +
        "finest granularity requested, e.g. " +
        "`CALL reify.time(datetime(\"2022-04-11T11:06:12Z\"), [\"month\", \"day\"]) YIELD node`.",
      docsUrl = reifyTimeReference,
    ),
    ProcedureDoc(
      name = "incrementCounter",
      signature = "incrementCounter(node :: NODE, key :: STRING, amount :: INTEGER) :: (count :: INTEGER)",
      description =
        "Atomically increments an integer property on a node by the given amount (1 when omitted), yielding " +
        "the resulting value. The read and write happen as one step on the node, so concurrent increments " +
        "never lose updates.",
      docsUrl = proceduresReference,
    ),
    ProcedureDoc(
      name = "int.add",
      signature = "int.add(node :: NODE, key :: STRING, add :: INTEGER) :: (result :: INTEGER)",
      description =
        "Atomically adds to an integer property on a node by the given amount (1 when omitted), yielding the " +
        "resulting value. The read and write happen as one step on the node, so concurrent additions never " +
        "lose updates.",
      docsUrl = proceduresReference,
    ),
    ProcedureDoc(
      name = "float.add",
      signature = "float.add(node :: NODE, key :: STRING, add :: FLOAT) :: (result :: FLOAT)",
      description = "Atomically adds to a floating-point property on a node by the given amount (1.0 when omitted), " +
        "yielding the resulting value. The read and write happen as one step on the node, so concurrent " +
        "additions never lose updates.",
      docsUrl = proceduresReference,
    ),
    ProcedureDoc(
      name = "set.insert",
      signature = "set.insert(node :: NODE, key :: STRING, add :: ANY) :: (result :: LIST OF ANY)",
      description =
        "Atomically adds an element to a list property treated as a set, yielding the resulting list. If one " +
        "or more instances of `add` are already present in the list at `node[key]`, the call has no effect.",
      docsUrl = proceduresReference,
    ),
    ProcedureDoc(
      name = "set.union",
      signature = "set.union(node :: NODE, key :: STRING, add :: LIST OF ANY) :: (result :: LIST OF ANY)",
      description =
        "Atomically adds a set of elements to a list property treated as a set, yielding the resulting list. " +
        "The elements of `add` are deduplicated, and any not yet present at `node[key]` are stored; if the " +
        "list already contains them all, the call has no effect.",
      docsUrl = proceduresReference,
    ),
    ProcedureDoc(
      name = "purgeNode",
      signature = "purgeNode(node :: ANY) :: VOID",
      description = "Purges a node from history: the node and its historical events are deleted from the " +
        "graph. Yields no rows.",
      docsUrl = proceduresReference,
    ),
    // Utilities
    ProcedureDoc(
      name = "loadJsonLines",
      signature = "loadJsonLines(url :: STRING) :: (value :: ANY)",
      description = "Loads a line-based JSON (JSONL) file from the given URL, yielding one `value` per line. " +
        "Each line is parsed as a standalone JSON document.",
      docsUrl = proceduresReference,
    ),
    ProcedureDoc(
      name = "log",
      signature = "log(level :: STRING, value :: ANY) :: (log :: STRING)",
      description = "Logs a value to the system console during query execution, yielding the logged text. Supported " +
        "levels: `error`, `warn`, `info`, `debug`, `trace`.",
      docsUrl = proceduresReference,
    ),
    ProcedureDoc(
      name = "util.sleep",
      signature = "util.sleep(duration :: INTEGER) :: VOID",
      description = "Sleeps for the given number of milliseconds before completing. Yields no rows; useful " +
        "for pacing or testing query behavior under delay.",
      docsUrl = proceduresReference,
    ),
    ProcedureDoc(
      name = "debug.sleep",
      signature = "debug.sleep(node :: ANY) :: VOID",
      description = "Requests that the given node go to sleep: its state is persisted and it is unloaded " +
        "from the in-memory cache. Yields no rows.",
      docsUrl = proceduresReference,
    ),
    // Registered at application startup rather than in resolveCalls.builtInProcedures
    ProcedureDoc(
      name = "standing.wiretap",
      signature = "standing.wiretap(options :: MAP) :: (data :: MAP, meta :: MAP)",
      description = "Streams live results from a running standing query, yielding the `data` and `meta` maps of each " +
        "match as it happens. The options map identifies the standing query by exactly one of `name` or " +
        "`id`, e.g. `CALL standing.wiretap({ name: \"my-standing-query\" }) YIELD data`. The stream does not " +
        "end on its own — pair it with a consumer or time-box the query.",
      docsUrl = proceduresReference,
    ),
    ProcedureDoc(
      name = "parseProtobuf",
      signature = "parseProtobuf(bytes :: BYTES, schemaUrl :: STRING, typeName :: STRING) :: (value :: MAP)",
      description = "Parses a protobuf message into a Cypher map value, using the message type `typeName` from the " +
        "schema at `schemaUrl`. Yields `null` when the bytes are not parseable as the requested type. The " +
        "inverse of `toProtobuf`.",
      docsUrl = proceduresReference,
    ),
    ProcedureDoc(
      name = "toProtobuf",
      signature = "toProtobuf(value :: MAP, schemaUrl :: STRING, typeName :: STRING) :: (protoBytes :: BYTES)",
      description = "Serializes a Cypher map value into protobuf bytes, using the message type `typeName` from the " +
        "schema at `schemaUrl`. Yields `null` when the value is not serializable as the requested type. The " +
        "inverse of `parseProtobuf`.",
      docsUrl = proceduresReference,
    ),
  )

  private val byLowerCaseName: Map[String, ProcedureDoc] =
    all.map(doc => doc.name.toLowerCase -> doc).toMap
}
