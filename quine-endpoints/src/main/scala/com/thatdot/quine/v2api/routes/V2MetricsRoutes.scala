package com.thatdot.quine.v2api.routes

import endpoints4s.algebra.Tag

import com.thatdot.quine.routes._

trait V2MetricsRoutes extends AdministrationRoutes with V2QuerySchemas {

  final protected val v2System: Path[Unit] = path / "api" / "v2" / "system"

  protected val v2MetricsTag: Tag = Tag("Administration V2")

  // The optional position selects a single cluster member (sent as the `Quine-Member-Idx` header,
  // matching the server). When omitted, the request resolves to the member serving it.
  val metricsV2: Endpoint[Option[String], Either[ClientErrors, Option[MetricsReport]]] =
    endpoint(
      request = get(v2System / "metrics", headers = optRequestHeader("Quine-Member-Idx")),
      response = customBadRequest("runtime error accessing metrics")
        .orElse(wheneverFound(ok(jsonResponse[MetricsReport]))),
    )

  // Same optional position selection as `metricsV2`.
  val shardSizesV2: Endpoint[Option[String], Either[ClientErrors, Option[Map[Int, ShardInMemoryLimit]]]] = {

    implicit val shardMapLimitSchema: JsonSchema[Map[Int, ShardInMemoryLimit]] = mapJsonSchema[ShardInMemoryLimit]
      .xmap[Map[Int, ShardInMemoryLimit]](
        _.map { case (k, v) => k.toInt -> v },
      )(
        _.map { case (k, v) => k.toString -> v },
      )

    endpoint(
      request = get(
        url = v2System / "shardSizeLimits",
        headers = optRequestHeader("Quine-Member-Idx"),
      ),
      response = customBadRequest("runtime error updating shard sizes")
        .orElse(wheneverFound(ok(jsonResponse[Map[Int, ShardInMemoryLimit]]))),
    )
  }
}
