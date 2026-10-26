package com.thatdot.quine.app.llm

import scala.concurrent.{ExecutionContext, Future}

import com.thatdot.common.logging.Log.{LazySafeLogging, LogConfig, Safe, SafeLoggableInterpolator}

/** Trying a chain of clients in order, moving on only when a profile cannot serve the call. */
object LlmFallback extends LazySafeLogging {

  /** Run `run` against each client in turn, moving on only when [[LlmError.cannotServe]]. Any other error
    * surfaces at once, named with its profile when the chain has several; when none could serve, one error
    * names each profile and why it was passed over.
    */
  def tryEach[C <: LlmClient, A](procedureName: String, clients: List[C])(run: C => Future[A])(implicit
    ec: ExecutionContext,
    logConfig: LogConfig,
  ): Future[A] = {
    // Only the status is Safe: the message may hold response-body bytes, so it rides in the exception
    def statusText(e: LlmError): String = e.status.fold("")(code => s" (status $code)")

    // Warn once for a BadResponse that surfaces, since an operator must act; the stopped pool falls back and is
    // logged elsewhere, and a Query fault reaches the author in the response
    val logged: C => Future[A] = client =>
      run(client).recoverWith {
        case e: LlmError if !e.cannotServe && e.fault == LlmError.Fault.BadResponse =>
          logger.warn(
            log"${Safe(procedureName)}: profile ${Safe(client.name)} failed the call${Safe(statusText(e))}, an operator must act"
            withException e,
          )
          Future.failed(e)
      }

    def go(client: C, rest: List[C], failed: List[(String, LlmError)]): Future[A] = rest match {
      case next :: more =>
        logged(client).recoverWith {
          case e: LlmError if e.cannotServe =>
            logger.warn(
              log"${Safe(procedureName)}: profile ${Safe(client.name)} could not serve the call${Safe(statusText(e))}, trying profile ${Safe(next.name)}"
              withException e,
            )
            go(next, more, failed :+ (client.name -> e))
          case e: LlmError => Future.failed(named(client, e))
        }
      case Nil =>
        logged(client).recoverWith {
          // The chain has at least two clients here, so at least one earlier failure is on the list.
          // The warn keeps this profile's exception on the record: the aggregate drops it
          case e: LlmError if e.cannotServe =>
            logger.warn(
              log"${Safe(procedureName)}: profile ${Safe(client.name)} could not serve the call${Safe(statusText(e))}, no profiles left"
              withException e,
            )
            Future.failed(noneCouldServe(failed :+ (client.name -> e)))
          case e: LlmError => Future.failed(named(client, e))
        }
    }
    clients match {
      // One client is exactly the plain call: the query author named one profile, so its error needs no name
      case single :: Nil => logged(single)
      case head :: tail => go(head, tail, Nil)
      // Chains resolve non-empty before anything runs, so an empty one is a bug in the caller
      case Nil => Future.failed(new IllegalStateException(s"$procedureName: tryEach got an empty client chain"))
    }
  }

  /** In a chain the query author named several profiles, so an error says which one it came from.
    * The status and the cause ride along unchanged.
    */
  private def named(client: LlmClient, e: LlmError): LlmError =
    LlmError(s"""profile "${client.name}": ${e.getMessage}""", e.kind, e.fault, e.status)(e.cause)

  /** `Final`, since every profile is spent, with the fault the caller can act on: `Unavailable` if any profile
    * was, since waiting may help; else `BadResponse`, reachable only through a stopped pool, which needs an
    * operator; else `Query`, the input is over every profile's limit. No status or cause, since either would
    * describe one profile only.
    */
  private def noneCouldServe(failures: List[(String, LlmError)]): LlmError = {
    val faults = failures.map { case (_, e) => e.fault }.toSet
    LlmError(
      "no profile could serve the call: " +
      failures.map { case (name, e) => s""""$name" (${e.getMessage})""" }.mkString("; "),
      LlmError.Kind.Final,
      if (faults(LlmError.Fault.Unavailable)) LlmError.Fault.Unavailable
      else if (faults(LlmError.Fault.BadResponse)) LlmError.Fault.BadResponse
      else LlmError.Fault.Query,
    )
  }
}
