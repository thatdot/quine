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
