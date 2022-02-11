package scala.compat

import scala.concurrent.ExecutionContext

object ExecutionContexts {

  val parasitic: ExecutionContext = ExecutionContext.parasitic
}
