package com.thatdot.quine.app.ingest2.source

case class IngestBounds(startAtOffset: Long = 0L, ingestLimit: Option[Long] = None)
