package com.thatdot.quine.webapp.v2api

import scala.scalajs.js

import io.circe.{Decoder, Encoder, HCursor, Json}

/** Minimal types for decoding V2 API responses in the browser.
  *
  * The V2 API types live in JVM-only modules (`public/api` and `public/quine`)
  * and are not cross-compiled to Scala.js. These lightweight mirrors decode only
  * the fields the browser UI needs.
  *
  * If the V2 API changes, update these types to match. Each type documents its
  * server-side source for easy lookup.
  */
object V2ApiTypes {

  /** Mirrors `com.thatdot.api.v2.Page` — the AIP-158 pagination envelope.
    *
    * `nextPageToken` is currently always empty server-side; we keep it as a field for
    * forward compatibility but the landing page just consumes `.items`.
    *
    * @see [[public/api/src/main/scala/com/thatdot/api/v2/Page.scala]]
    */
  final case class V2Page[A](items: List[A], nextPageToken: String)
  object V2Page {
    implicit def decoder[A: Decoder]: Decoder[V2Page[A]] = (c: HCursor) =>
      for {
        items <- c.downField("items").as[List[A]]
        token <- c.downField("nextPageToken").as[Option[String]].map(_.getOrElse(""))
      } yield V2Page(items, token)
  }

  /** Mirrors `com.thatdot.api.v2.RatesSummary` (fields subset).
    * @see [[public/api/src/main/scala/com/thatdot/api/v2/RatesSummary.scala]]
    */
  final case class V2RatesSummary(
    count: Long,
    oneMinute: Double,
  )
  object V2RatesSummary {
    implicit val decoder: Decoder[V2RatesSummary] = (c: HCursor) =>
      for {
        count <- c.downField("count").as[Long]
        oneMinute <- c.downField("oneMinute").as[Double]
      } yield V2RatesSummary(count, oneMinute)
  }

  /** Mirrors `com.thatdot.quine.app.v2api.definitions.ingest2.ApiIngest.IngestStreamStats` (fields subset).
    * @see [[public/quine/src/main/scala/com/thatdot/quine/app/v2api/definitions/ingest2/ApiIngest.scala]]
    */
  final case class V2IngestStats(
    ingestedCount: Long,
    rates: V2RatesSummary,
    /** Human-readable total runtime as reported by the server (e.g. "2h 3m 10s"). */
    totalRuntime: Option[String],
  )
  object V2IngestStats {
    implicit val decoder: Decoder[V2IngestStats] = (c: HCursor) =>
      for {
        count <- c.downField("ingestedCount").as[Long]
        rates <- c.downField("rates").as[V2RatesSummary]
        totalRuntime <- c.get[Option[String]]("totalRuntime")
      } yield V2IngestStats(count, rates, totalRuntime)
  }

  /** Mirrors `com.thatdot.quine.app.v2api.definitions.ingest2.ApiIngest.IngestStreamInfoWithName` (fields subset).
    *
    * `status` is the `IngestStreamStatus` value, normalized to PascalCase via [[V2IngestInfo.humanizeStatus]].
    * `sourceType` is extracted from the `"type"` discriminator of `ApiIngest.IngestSource`,
    * which lives at `settings.source` since `settings` is a full `QuineIngestConfiguration`.
    * `sourceId` is a best-effort human-readable identifier pulled from `settings.source`
    * (e.g. Kafka bootstrapServers, S3 bucket, file path). Falls back to the sourceType slug.
    *
    * @see [[public/quine/src/main/scala/com/thatdot/quine/app/v2api/definitions/ingest2/ApiIngest.scala]]
    */
  final case class V2IngestInfo(
    name: String,
    status: String,
    /** Server-reported failure detail; present when `status` is Failed. */
    message: Option[String],
    sourceType: String,
    sourceId: String,
    stats: V2IngestStats,
    /** Cluster position running this ingest; absent on single-node servers. */
    memberIdx: Option[Int],
    /** The complete wire payload, for full-fidelity display (the configuration viewer). */
    raw: Json,
  )
  object V2IngestInfo {
    // Fields in `settings` we'll look at (first non-empty wins) for a source identifier.
    private val sourceIdFields: List[String] =
      List(
        "bootstrapServers",
        "bucket",
        "streamName",
        "kinesisStreamName",
        "queueUrl",
        "url",
        "path",
        "applicationName",
      )

    /** SCREAMING_SNAKE_CASE → PascalCase, e.g. `"PAUSED"` → `"Paused"`. Pass-through for
      * non-uppercase input so unknown future values surface verbatim.
      */
    def humanizeStatus(wire: String): String =
      if (wire.nonEmpty && wire.forall(c => c.isUpper || c.isDigit || c == '_'))
        wire
          .split('_')
          .iterator
          .map { part =>
            if (part.isEmpty) part else part.head.toString + part.tail.toLowerCase
          }
          .mkString
      else wire

    implicit val decoder: Decoder[V2IngestInfo] = (c: HCursor) =>
      for {
        name <- c.downField("name").as[String]
        status <- c
          .downField("status")
          .downField("type")
          .as[String]
          .orElse(c.downField("status").as[String])
          .map(humanizeStatus)
        message <- c.get[Option[String]]("message")
        source = c.downField("settings").downField("source")
        sourceType <- source.downField("type").as[String]
        sourceId = sourceIdFields
          .flatMap(f => source.downField(f).as[String].toOption)
          .find(_.nonEmpty)
          .getOrElse(sourceType)
        stats <- c.downField("stats").as[V2IngestStats]
        memberIdx <- c.get[Option[Int]]("memberIdx")
      } yield V2IngestInfo(name, status, message, sourceType, sourceId, stats, memberIdx, c.value)
  }

  /** Mirrors `com.thatdot.quine.app.v2api.definitions.query.standing.StandingQueryStats` (fields subset).
    * @see [[public/quine/src/main/scala/com/thatdot/quine/app/v2api/definitions/query/standing/StandingQueryStats.scala]]
    */
  final case class V2StandingQueryStats(
    rates: V2RatesSummary,
  )
  object V2StandingQueryStats {
    implicit val decoder: Decoder[V2StandingQueryStats] = (c: HCursor) =>
      c.downField("rates").as[V2RatesSummary].map(V2StandingQueryStats(_))
  }

  /** Mirrors destination entries from `com.thatdot.quine.app.v2api.definitions.outputs.QuineDestinationSteps`.
    * Only the `"type"` discriminator is extracted.
    * @see [[public/quine/src/main/scala/com/thatdot/quine/app/v2api/definitions/outputs/QuineDestinationSteps.scala]]
    */
  final case class V2StandingQueryDestination(
    destinationType: String,
  )
  object V2StandingQueryDestination {
    implicit val decoder: Decoder[V2StandingQueryDestination] = (c: HCursor) =>
      c.downField("type").as[String].map(V2StandingQueryDestination(_))
  }

  /** Mirrors `com.thatdot.quine.app.v2api.definitions.query.standing.StandingQueryResultWorkflow` (fields subset).
    * @see [[public/quine/src/main/scala/com/thatdot/quine/app/v2api/definitions/query/standing/StandingQueryResultWorkflow.scala]]
    */
  final case class V2StandingQueryOutput(
    name: String,
    destinations: List[V2StandingQueryDestination],
    hasEnrichment: Boolean, // whether the output defines a `resultEnrichment` Cypher query
  )
  object V2StandingQueryOutput {
    implicit val decoder: Decoder[V2StandingQueryOutput] = (c: HCursor) =>
      for {
        name <- c.downField("name").as[String]
        destinations <- c.downField("destinations").as[List[V2StandingQueryDestination]]
        // `resultEnrichment` is an optional Cypher query; its presence is what makes the Pre and
        // Post tap points differ (Pre = before it runs, Post = after).
        enrichment <- c.downField("resultEnrichment").as[Option[Json]]
      } yield V2StandingQueryOutput(name, destinations, enrichment.exists(!_.isNull))
  }

  /** Mirrors `com.thatdot.quine.app.v2api.definitions.query.standing.StandingQueryPattern` (fields subset):
    * the Cypher text and mode shown in the Streams standing-query table. Both fields are
    * optional so a non-Cypher pattern variant decodes rather than failing the whole list.
    * @see [[public/quine/src/main/scala/com/thatdot/quine/app/v2api/definitions/query/standing/StandingQueryPattern.scala]]
    */
  final case class V2StandingQueryPattern(
    query: Option[String],
    mode: Option[String],
  )
  object V2StandingQueryPattern {
    implicit val decoder: Decoder[V2StandingQueryPattern] = (c: HCursor) =>
      for {
        query <- c.get[Option[String]]("query")
        mode <- c.get[Option[String]]("mode")
      } yield V2StandingQueryPattern(query, mode)
  }

  /** Mirrors `com.thatdot.quine.app.v2api.definitions.query.standing.StandingQuery.RegisteredStandingQuery` (fields subset).
    * @see [[public/quine/src/main/scala/com/thatdot/quine/app/v2api/definitions/query/standing/StandingQuery.scala]]
    */
  final case class V2StandingQueryInfo(
    name: String,
    outputs: List[V2StandingQueryOutput],
    stats: Map[String, V2StandingQueryStats],
    pattern: Option[V2StandingQueryPattern],
    /** The complete wire payload, for full-fidelity display (the configuration viewer). */
    raw: Json,
  )
  object V2StandingQueryInfo {
    implicit val decoder: Decoder[V2StandingQueryInfo] = (c: HCursor) =>
      for {
        name <- c.downField("name").as[String]
        outputs <- c.downField("outputs").as[Option[List[V2StandingQueryOutput]]].map(_.getOrElse(Nil))
        stats <- c.downField("stats").as[Option[Map[String, V2StandingQueryStats]]].map(_.getOrElse(Map.empty))
        pattern <- c.downField("pattern").as[Option[V2StandingQueryPattern]]
      } yield V2StandingQueryInfo(name, outputs, stats, pattern, c.value)
  }

  /** Mirrors the tap-query synthetic-edge wire shape.
    * `direction` is one of "OUT", "IN", "UNDIRECTED".
    * `nodeIdsFrom` is currently always "WIRETAP_MESSAGE" (defaults to it server-side too).
    */
  final case class V2SyntheticEdge(
    fromNode: String,
    toNode: String,
    label: String,
    direction: String,
    nodeIdsFrom: String,
  )
  object V2SyntheticEdge {
    implicit val decoder: Decoder[V2SyntheticEdge] = (c: HCursor) =>
      for {
        from <- c.downField("fromNode").as[String]
        to <- c.downField("toNode").as[String]
        label <- c.downField("label").as[String]
        direction <- c.downField("direction").as[String]
        nodeIdsFrom <- c.downField("nodeIdsFrom").as[Option[String]].map(_.getOrElse("WIRETAP_MESSAGE"))
      } yield V2SyntheticEdge(from, to, label, direction, nodeIdsFrom)

    implicit val encoder: Encoder[V2SyntheticEdge] = (e: V2SyntheticEdge) =>
      Json.obj(
        "fromNode" -> Json.fromString(e.fromNode),
        "toNode" -> Json.fromString(e.toNode),
        "label" -> Json.fromString(e.label),
        "direction" -> Json.fromString(e.direction),
        "nodeIdsFrom" -> Json.fromString(e.nodeIdsFrom),
      )
  }

  /** Mirrors the tap-query wire shape.
    * @see [[public/quine/src/main/scala/com/thatdot/quine/app/v2api/definitions/ApiUiStyling.scala]]
    */
  final case class V2TapQuery(
    name: String,
    description: Option[String],
    standingQueryName: String,
    outputName: Option[String],
    query: String,
    syntheticEdges: Vector[V2SyntheticEdge],
  )
  object V2TapQuery {
    implicit val decoder: Decoder[V2TapQuery] = (c: HCursor) =>
      for {
        name <- c.downField("name").as[String]
        description <- c.downField("description").as[Option[String]]
        sqName <- c.downField("standingQueryName").as[String]
        outputName <- c.downField("outputName").as[Option[String]]
        query <- c.downField("query").as[String]
        syntheticEdges <- c
          .downField("syntheticEdges")
          .as[Option[Vector[V2SyntheticEdge]]]
          .map(_.getOrElse(Vector.empty))
      } yield V2TapQuery(name, description, sqName, outputName, query, syntheticEdges)

    implicit val encoder: Encoder[V2TapQuery] = (t: V2TapQuery) =>
      Json.obj(
        "name" -> Json.fromString(t.name),
        "description" -> t.description.fold(Json.Null)(Json.fromString),
        "standingQueryName" -> Json.fromString(t.standingQueryName),
        "outputName" -> t.outputName.fold(Json.Null)(Json.fromString),
        "query" -> Json.fromString(t.query),
        "syntheticEdges" -> Json.arr(t.syntheticEdges.map(V2SyntheticEdge.encoder.apply): _*),
      )
  }

  /** Mirrors `ApiQuineHost` from the enterprise V2 admin status endpoint.
    * @see [[quine-enterprise/src/main/scala/com/thatdot/quine/app/v2api/endpoints/V2EnterpriseAdministrationEndpoints.scala]]
    */
  final case class V2QuineHost(address: String, port: Int, uid: Long)
  object V2QuineHost {
    implicit val decoder: Decoder[V2QuineHost] = (c: HCursor) =>
      for {
        address <- c.downField("address").as[String]
        port <- c.downField("port").as[Int]
        uid <- c.downField("uid").as[Long]
      } yield V2QuineHost(address, port, uid)
  }

  /** Mirrors `ApiClusterOperationStatus` (fields subset) from the enterprise V2 admin status endpoint.
    * @see [[quine-enterprise/src/main/scala/com/thatdot/quine/app/v2api/endpoints/V2EnterpriseAdministrationEndpoints.scala]]
    */
  final case class V2ClusterOperationStatus(
    clusterMembers: Map[String, V2QuineHost],
    hotSpares: List[V2QuineHost],
    targetSize: Int,
  ) {

    /** Member positions present in the cluster, parsed from the member keys and sorted ascending. */
    def memberIndices: Seq[Int] = clusterMembers.keys.flatMap(_.toIntOption).toSeq.sorted
  }
  object V2ClusterOperationStatus {
    implicit val decoder: Decoder[V2ClusterOperationStatus] = (c: HCursor) =>
      for {
        members <- c.downField("clusterMembers").as[Map[String, V2QuineHost]]
        spares <- c.downField("hotSpares").as[List[V2QuineHost]]
        targetSize <- c.downField("targetSize").as[Int]
      } yield V2ClusterOperationStatus(members, spares, targetSize)
  }

  /** Subset of `SystemConfigView`, the filtered config view exposed by `GET /api/v2/system/config`.
    * We only extract the persistor type (e.g. "cassandra", "rocksdb").
    * @see [[public/quine/src/main/scala/com/thatdot/quine/app/v2api/endpoints/V2QuineAdministrationEndpoints.scala]] (configE)
    */
  final case class V2QuineConfig(storeType: String)
  object V2QuineConfig {
    implicit val decoder: Decoder[V2QuineConfig] = (c: HCursor) =>
      c.downField("persistor").downField("persistorType").as[String].map(V2QuineConfig(_))
  }

  // ── Backpressure Snapshot ─────────────────────────────────────────────────
  // ── Backpressure API types (mirrors TBackpressureSnapshot and related types) ──

  final case class V2GlobalValve(isOpen: Boolean, closedCount: Int, oneMinuteClosures: Int)
  object V2GlobalValve {
    implicit val decoder: Decoder[V2GlobalValve] = (c: HCursor) =>
      for {
        isOpen <- c.downField("isOpen").as[Boolean]
        closedCount <- c.downField("closedCount").as[Int]
        oneMinuteClosures <- c.downField("oneMinuteClosures").as[Int]
      } yield V2GlobalValve(isOpen, closedCount, oneMinuteClosures)
  }

  final case class V2IngestStages(
    source: String,
    preGraphWrite: String,
    postGraphWrite: Option[String],
  )
  object V2IngestStages {
    implicit val decoder: Decoder[V2IngestStages] = (c: HCursor) =>
      for {
        source <- c.downField("source").as[Option[String]].map(_.getOrElse("FLOWING"))
        preGraphWrite <- c.downField("preGraphWrite").as[Option[String]].map(_.getOrElse("FLOWING"))
        postGraphWrite <- c.downField("postGraphWrite").as[Option[String]]
      } yield V2IngestStages(source, preGraphWrite, postGraphWrite)
  }

  final case class V2IngestSnapshot(
    name: String,
    namespace: String,
    sourceType: String,
    status: String,
    rateLimit: Option[Int],
    rate: Double,
    totalCount: Long,
    stages: V2IngestStages,
  )
  object V2IngestSnapshot {
    implicit val decoder: Decoder[V2IngestSnapshot] = (c: HCursor) =>
      for {
        name <- c.downField("name").as[String]
        namespace <- c.downField("namespace").as[String]
        sourceType <- c.downField("sourceType").as[String]
        status <- c.downField("status").as[String]
        rateLimit <- c.downField("rateLimit").as[Option[Int]]
        rate <- c.downField("rate").as[Double]
        totalCount <- c.downField("totalCount").as[Long]
        stages <- c.downField("stages").as[V2IngestStages]
      } yield V2IngestSnapshot(name, namespace, sourceType, status, rateLimit, rate, totalCount, stages)
  }

  final case class V2SqQueue(
    bufferCount: Int,
    backpressureThreshold: Int,
    maxSize: Int,
    thresholdRatio: Double,
    capacityRatio: Double,
    totalProduced: Long,
    totalCancellations: Long,
    totalDropped: Long,
    totalConsumed: Long,
    productionRate: Double,
    consumptionRate: Double,
  )
  object V2SqQueue {
    implicit val decoder: Decoder[V2SqQueue] = (c: HCursor) =>
      for {
        bufferCount <- c.downField("bufferCount").as[Int]
        threshold <- c.downField("backpressureThreshold").as[Int]
        maxSize <- c.downField("maxSize").as[Int]
        thresholdRatio <- c.downField("thresholdRatio").as[Double]
        capacityRatio <- c.downField("capacityRatio").as[Double]
        totalProduced <- c.downField("totalProduced").as[Long]
        totalCancellations <- c.downField("totalCancellations").as[Long]
        totalDropped <- c.downField("totalDropped").as[Long]
        totalConsumed <- c.downField("totalConsumed").as[Long]
        productionRate <- c.downField("productionRate").as[Double]
        consumptionRate <- c.downField("consumptionRate").as[Double]
      } yield V2SqQueue(
        bufferCount,
        threshold,
        maxSize,
        thresholdRatio,
        capacityRatio,
        totalProduced,
        totalCancellations,
        totalDropped,
        totalConsumed,
        productionRate,
        consumptionRate,
      )
  }

  final case class V2Destination(`type`: String, state: String)
  object V2Destination {
    implicit val decoder: Decoder[V2Destination] = (c: HCursor) =>
      for {
        t <- c.downField("type").as[String]
        state <- c.downField("state").as[String]
      } yield V2Destination(t, state)
  }

  final case class V2SqOutput(
    name: String,
    rate: Double,
    totalCount: Long,
    hasEnrichment: Boolean,
    enrichmentState: Option[String],
    destinations: Seq[V2Destination],
  )
  object V2SqOutput {
    implicit val decoder: Decoder[V2SqOutput] = (c: HCursor) =>
      for {
        name <- c.downField("name").as[String]
        rate <- c.downField("rate").as[Double]
        totalCount <- c.downField("totalCount").as[Long]
        hasEnrichment <- c.downField("hasEnrichment").as[Option[Boolean]].map(_.getOrElse(false))
        enrichmentState <- c.downField("enrichmentState").as[Option[String]]
        destinations <- c.downField("destinations").as[Seq[V2Destination]]
      } yield V2SqOutput(name, rate, totalCount, hasEnrichment, enrichmentState, destinations)
  }

  final case class V2StandingQuery(
    name: String,
    namespace: String,
    queue: V2SqQueue,
    outputs: Seq[V2SqOutput],
  )
  object V2StandingQuery {
    implicit val decoder: Decoder[V2StandingQuery] = (c: HCursor) =>
      for {
        name <- c.downField("name").as[String]
        namespace <- c.downField("namespace").as[String]
        queue <- c.downField("queue").as[V2SqQueue]
        outputs <- c.downField("outputs").as[Seq[V2SqOutput]]
      } yield V2StandingQuery(name, namespace, queue, outputs)
  }

  final case class V2Persistor(`type`: String, writeLatencyMs: Double, readLatencyMs: Double)
  object V2Persistor {
    implicit val decoder: Decoder[V2Persistor] = (c: HCursor) =>
      for {
        t <- c.downField("type").as[String]
        w <- c.downField("writeLatencyMs").as[Double]
        r <- c.downField("readLatencyMs").as[Double]
      } yield V2Persistor(t, w, r)
  }

  final case class V2BackpressureSnapshot(
    timestamp: Double,
    globalValve: V2GlobalValve,
    ingests: Seq[V2IngestSnapshot],
    standingQueries: Seq[V2StandingQuery],
    persistor: V2Persistor,
  )
  object V2BackpressureSnapshot {
    implicit val decoder: Decoder[V2BackpressureSnapshot] = (c: HCursor) =>
      for {
        timestamp <- c.downField("timestamp").as[String].map(t => new js.Date(t).getTime())
        valve <- c.downField("globalValve").as[V2GlobalValve]
        ingests <- c.downField("ingests").as[Seq[V2IngestSnapshot]]
        sqs <- c.downField("standingQueries").as[Seq[V2StandingQuery]]
        persistor <- c.downField("persistor").as[V2Persistor]
      } yield V2BackpressureSnapshot(timestamp, valve, ingests, sqs, persistor)
  }

  /** Mirrors `ServiceStatus` (fields subset) from the enterprise V2 admin status endpoint.
    * @see [[quine-enterprise/src/main/scala/com/thatdot/quine/app/v2api/endpoints/V2EnterpriseAdministrationEndpoints.scala]]
    */
  final case class V2ServiceStatus(
    fullyUp: Boolean,
    cluster: V2ClusterOperationStatus,
  )
  object V2ServiceStatus {
    implicit val decoder: Decoder[V2ServiceStatus] = (c: HCursor) =>
      for {
        fullyUp <- c.downField("fullyUp").as[Boolean]
        cluster <- c.downField("cluster").as[V2ClusterOperationStatus]
      } yield V2ServiceStatus(fullyUp, cluster)
  }
}
