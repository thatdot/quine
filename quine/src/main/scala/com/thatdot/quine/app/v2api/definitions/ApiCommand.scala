package com.thatdot.quine.app.v2api.definitions

sealed trait ApiCommand

case object GetNamespaces extends ApiCommand
case object CreateNamespace extends ApiCommand
case object DeleteNamespace extends ApiCommand

// standing queries
case object ListSQsApiCmd extends ApiCommand
case object PropagateSQsApiCmd extends ApiCommand
case object CreateSQApiCmd extends ApiCommand
case object DeleteSQApiCmd extends ApiCommand
case object GetSQApiCmd extends ApiCommand
case object CreateSQOutputApiCmd extends ApiCommand
case object DeleteSQOutputApiCmd extends ApiCommand

// admin endpoints
case object GetConfigApiCmd extends ApiCommand
case object GetBuildInfoApiCmd extends ApiCommand
case object GraphHashCodeApiCmd extends ApiCommand
case object GetLivenessApiCmd extends ApiCommand
case object GetReadinessApiCmd extends ApiCommand
case object GetMetaDataApiCmd extends ApiCommand
case object SleepNodeApiCmd extends ApiCommand
case object GetMetricsApiCmd extends ApiCommand
case object ShutdownApiCmd extends ApiCommand

// cypher endpoints
case object CypherPostApiCmd extends ApiCommand
case object CypherNodesPostApiCmd extends ApiCommand
case object CypherEdgesPostApiCmd extends ApiCommand

// algorithm endpoints
case object SaveRandomWalksApiCmd extends ApiCommand
case object GenerateRandomWalkApiCmd extends ApiCommand

// debug endpoints
case object DebugVerboseApiCmd extends ApiCommand
case object DebugEdgesGetApiCmd extends ApiCommand
case object DebugHalfEdgesGetApiCmd extends ApiCommand
case object DebugOpsGetApiCmd extends ApiCommand
case object DebugOpsPropertygetApiCmd extends ApiCommand

// ingest endpoints
case object PauseIngestApiCmd extends ApiCommand
case object CreateIngestApiCmd extends ApiCommand
case object DeleteIngestApiCmd extends ApiCommand
case object IngestStatusApiCmd extends ApiCommand
case object UnpauseIngestApiCmd extends ApiCommand
case object ListIngestApiCmd extends ApiCommand
