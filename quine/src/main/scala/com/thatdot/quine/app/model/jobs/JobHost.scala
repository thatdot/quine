package com.thatdot.quine.app.model.jobs

import java.util.UUID

import scala.concurrent.{ExecutionContext, Future}

/** The app-side collaborators the V2 job/background-query endpoints drive. Implemented by both
  * `QuineApp` and `QuineEnterpriseApp`, so the endpoint logic (`JobApiMethods`) is written once
  * against this interface instead of duplicated per app.
  */
trait JobHost {

  /** Per-execution background-query status records (shared-persistor-backed, readable cluster-wide). */
  def backgroundQueryRegistry: BackgroundQueryStatusRegistry

  /** Runs background-query executions locally on this host. */
  def backgroundQueryRunner: BackgroundQueryRunner

  /** Creates/inspects/deletes scheduled jobs — the local scheduler in OSS, the elected cluster
    * manager in enterprise.
    */
  def jobService: JobService

  /** Cancel an in-flight background-query execution wherever it is running (this host in OSS,
    * broadcast cluster-wide in enterprise); the executing host records the terminal
    * [[ExecutionAction.Cancelled]] state.
    */
  def cancelBackgroundQuery(executionId: UUID): Future[Unit]

  /** Dispatch one background-query execution now, on this host, under a freshly minted id. The
    * returned Future yields that id once the execution's `Started` record has been written, so a
    * caller that hands the id straight to a client cannot hand out an id that a `GET`, cancel, or
    * tap would 404 on. It does not wait for the query itself, which runs on past it.
    */
  final def runBackgroundQuery(backgroundQuery: BackgroundQuery): Future[UUID] = {
    val executionId = UUID.randomUUID()
    backgroundQueryRunner
      .run(executionId, jobName = None, backgroundQuery)
      .started
      .map(_ => executionId)(ExecutionContext.parasitic)
  }
}
