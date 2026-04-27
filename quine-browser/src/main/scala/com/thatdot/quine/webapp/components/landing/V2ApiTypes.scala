package com.thatdot.quine.webapp.components.landing

import io.circe.{Decoder, HCursor}

/** Minimal types for decoding V2 API responses in the browser.
  *
  * The V2 API types live in JVM-only modules (`public/api` and `public/quine`)
  * and are not cross-compiled to Scala.js. These lightweight mirrors decode only
  * the fields the landing page needs.
  *
  * If the V2 API changes, update these types to match. Each type documents its
  * server-side source for easy lookup.
  */
object V2ApiTypes {

  /** Mirrors `com.thatdot.api.v2.SuccessEnvelope.Ok`.
    * @see [[public/api/src/main/scala/com/thatdot/api/v2/SuccessEnvelope.scala]]
    */
  final case class V2Response[A](content: A)
  object V2Response {
    implicit def decoder[A: Decoder]: Decoder[V2Response[A]] = (c: HCursor) =>
      c.downField("content").as[A].map(V2Response(_))
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
  )
  object V2IngestStats {
    implicit val decoder: Decoder[V2IngestStats] = (c: HCursor) =>
      for {
        count <- c.downField("ingestedCount").as[Long]
        rates <- c.downField("rates").as[V2RatesSummary]
      } yield V2IngestStats(count, rates)
  }

  /** Mirrors `com.thatdot.quine.app.v2api.definitions.ingest2.ApiIngest.IngestStreamInfoWithName` (fields subset).
    *
    * `status` is extracted from the `"type"` discriminator of `ApiIngest.IngestStreamStatus`.
    * `sourceType` is extracted from the `"type"` discriminator of `ApiIngest.IngestSource`.
    * `sourceId` is a best-effort human-readable identifier pulled from the settings (e.g.
    * Kafka bootstrapServers, S3 bucket, file path). Falls back to the sourceType slug.
    *
    * @see [[public/quine/src/main/scala/com/thatdot/quine/app/v2api/definitions/ingest2/ApiIngest.scala]]
    */
  final case class V2IngestInfo(
    name: String,
    status: String,
    sourceType: String,
    sourceId: String,
    stats: V2IngestStats,
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

    implicit val decoder: Decoder[V2IngestInfo] = (c: HCursor) =>
      for {
        name <- c.downField("name").as[String]
        status <- c
          .downField("status")
          .downField("type")
          .as[String]
          .orElse(c.downField("status").as[String])
        settings = c.downField("settings")
        sourceType <- settings.downField("type").as[String]
        sourceId = sourceIdFields
          .flatMap(f => settings.downField(f).as[String].toOption)
          .find(_.nonEmpty)
          .getOrElse(sourceType)
        stats <- c.downField("stats").as[V2IngestStats]
      } yield V2IngestInfo(name, status, sourceType, sourceId, stats)
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
  )
  object V2StandingQueryOutput {
    implicit val decoder: Decoder[V2StandingQueryOutput] = (c: HCursor) =>
      for {
        name <- c.downField("name").as[String]
        destinations <- c.downField("destinations").as[List[V2StandingQueryDestination]]
      } yield V2StandingQueryOutput(name, destinations)
  }

  /** Mirrors `com.thatdot.quine.app.v2api.definitions.query.standing.StandingQuery.RegisteredStandingQuery` (fields subset).
    * @see [[public/quine/src/main/scala/com/thatdot/quine/app/v2api/definitions/query/standing/StandingQuery.scala]]
    */
  final case class V2StandingQueryInfo(
    name: String,
    outputs: List[V2StandingQueryOutput],
    stats: Map[String, V2StandingQueryStats],
  )
  object V2StandingQueryInfo {
    implicit val decoder: Decoder[V2StandingQueryInfo] = (c: HCursor) =>
      for {
        name <- c.downField("name").as[String]
        outputs <- c.downField("outputs").as[Option[List[V2StandingQueryOutput]]].map(_.getOrElse(Nil))
        stats <- c.downField("stats").as[Option[Map[String, V2StandingQueryStats]]].map(_.getOrElse(Map.empty))
      } yield V2StandingQueryInfo(name, outputs, stats)
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
  )
  object V2ClusterOperationStatus {
    implicit val decoder: Decoder[V2ClusterOperationStatus] = (c: HCursor) =>
      for {
        members <- c.downField("clusterMembers").as[Map[String, V2QuineHost]]
        spares <- c.downField("hotSpares").as[List[V2QuineHost]]
        targetSize <- c.downField("targetSize").as[Int]
      } yield V2ClusterOperationStatus(members, spares, targetSize)
  }

  /** Subset of the running config JSON exposed by `GET /api/v2/admin/config`.
    * We only extract the persistor store type (e.g. "cassandra", "rocksdb") and leave the
    * rest untouched since the full config is a dynamic JSON blob.
    * @see [[public/quine/src/main/scala/com/thatdot/quine/app/v2api/endpoints/V2QuineAdministrationEndpoints.scala]] (configE)
    */
  final case class V2QuineConfig(storeType: String)
  object V2QuineConfig {
    implicit val decoder: Decoder[V2QuineConfig] = (c: HCursor) =>
      c.downField("quine").downField("store").downField("type").as[String].map(V2QuineConfig(_))
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
