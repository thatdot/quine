package com.thatdot.quine.webapp.dataservice

import com.raquo.airstream.core.{Observer, Signal}

import com.thatdot.quine.webapp.util.Pot
import com.thatdot.quine.webapp.v2api.V2ApiTypes.V2IngestInfo

/** Ingest-stream capability: the current graph's ingest-stream list. */
trait IngestStreamService {

  /** Entry point for ingest-stream commands; see [[NamespaceService.namespaceDispatch]]
    * for why each slice has its own dispatch.
    */
  def ingestStreamDispatch: Observer[IngestStreamService.Command]

  /** Ingest streams for the current graph namespace; `Pot` distinguishes loading and
    * fetch failure (with `FailedStale` keeping the last good list) from a truly empty list.
    */
  def ingestStreamsSignal: Signal[Pot[Seq[V2IngestInfo]]]
}

object IngestStreamService {

  /** A state-changing request to the ingest-stream capability, sent via
    * [[IngestStreamService.ingestStreamDispatch]].
    */
  sealed trait Command

  /** Request an immediate refetch of [[IngestStreamService.ingestStreamsSignal]], e.g.
    * after a mutation, instead of waiting out the poll interval.
    */
  case object RefreshIngestStreams extends Command
}
