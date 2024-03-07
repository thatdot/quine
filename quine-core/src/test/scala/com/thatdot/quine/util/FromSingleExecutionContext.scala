package com.thatdot.quine.util

import scala.concurrent.ExecutionContext

/** Use the same EC for both of them. Intended for use with ScalaTest's SerialExecutionContext
  * @param executionContext
  */
class FromSingleExecutionContext(executionContext: ExecutionContext) extends ComputeAndBlockingExecutionContexts {

  val nodeDispatcherEC: ExecutionContext = executionContext

  val blockingDispatcherEC: ExecutionContext = executionContext
}
