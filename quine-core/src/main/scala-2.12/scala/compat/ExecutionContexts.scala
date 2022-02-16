import scala.concurrent.ExecutionContext

/* Declarating an object in `akka.dispatch` let's us work around Akka's `private[dispatch]`
 * visibility on `ExecutionContexts.parasitic`. They keep it private because they're only going
 * to maintain this until they drop 2.12 support, but that's anyways when we'll stop needing it too
 */
package akka.dispatch {
  object ExecContextForward {
    val parasitic: ExecutionContext = ExecutionContexts.parasitic
  }
}

package scala.compat {
  object ExecutionContexts {
    val parasitic: ExecutionContext = akka.dispatch.ExecContextForward.parasitic
  }
}
