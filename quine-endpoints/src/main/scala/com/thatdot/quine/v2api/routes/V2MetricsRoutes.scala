package com.thatdot.quine.v2api.routes

import endpoints4s.algebra.Tag

import com.thatdot.quine.routes._

trait V2MetricsRoutes extends AdministrationRoutes with V2QuerySchemas {

  final protected val v2Admin: Path[Unit] = path / "api" / "v2" / "admin"

  protected val v2MetricsTag: Tag = Tag("Administration V2")

  val metricsV2: Endpoint[Unit, Either[ClientErrors, Option[MetricsReport]]] =
    endpoint(
      request = get(v2Admin / "metrics"),
      response = customBadRequest("runtime error accessing metrics")
        .orElse(
          wheneverFound(
            ok(
              jsonResponse[V2SuccessResponse[MetricsReport]],
            ).xmap(response => response.content)(result => V2SuccessResponse(result)),
          ),
        ),
    )

  val shardSizesV2: Endpoint[Unit, Either[ClientErrors, Option[Map[Int, ShardInMemoryLimit]]]] = {

    implicit val shardMapLimitSchema: JsonSchema[Map[Int, ShardInMemoryLimit]] = mapJsonSchema[ShardInMemoryLimit]
      .xmap[Map[Int, ShardInMemoryLimit]](
        _.map { case (k, v) => k.toInt -> v },
      )(
        _.map { case (k, v) => k.toString -> v },
      )

    endpoint(
      request = get(
        url = v2Admin / "shards" / "size-limits",
      ),
      response = customBadRequest("runtime error updating shard sizes")
        .orElse(
          wheneverFound(
            ok(
              jsonResponse[V2SuccessResponse[Map[Int, ShardInMemoryLimit]]],
            ).xmap(response => response.content)(result => V2SuccessResponse(result)),
          ),
        ),
    )
  }
}
