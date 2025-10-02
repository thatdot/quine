package com.thatdot.quine.app.v2api.definitions

import sttp.tapir.{EndpointInput, query}

import com.thatdot.quine.routes.IngestRoutes

trait ParallelismParameter {
  // ------- parallelism -----------
  val parallelismParameter: EndpointInput.Query[Int] = query[Int](name = "parallelism")
    .description(s"Number of operations to execute simultaneously.")
    .default(IngestRoutes.defaultWriteParallelism)
}
