package com.thatdot.quine.graph.messaging

import scala.concurrent.Future

import org.apache.pekko.actor.ActorRef
import org.apache.pekko.util.Timeout

import com.thatdot.quine.graph.BaseGraph
import com.thatdot.quine.model.{Milliseconds, QuineId}

trait QuineIdOps {

  protected def graph: BaseGraph
  def atTime: Option[Milliseconds]

  /** Support sending typed messages to some other node in the graph, which
    * lives in the same time period (moving present or else historical time)
    */
  implicit final class RichQuineId(quineId: QuineId) {

    /** Tell a message to some node in the same time and graph
      *
      * @param message the message to send
      * @param originalSender who originally sent the message (for debug only)
      */
    def !(message: QuineMessage)(implicit originalSender: ActorRef): Unit =
      graph.relayTell(QuineIdAtTime(quineId, atTime), message, originalSender)

    /** Ask a message to some node in the same time and graph
      *
      * @param unattributedMessage how to make the message from a return address
      * @param timeout how long to wait for a response before timing out
      * @param originalSender who originally sent the message (for debug only)
      * @return a future that is fulfilled by the response sent back
      */
    def ?[A](unattributedMessage: QuineRef => QuineMessage with AskableQuineMessage[A])(implicit
      timeout: Timeout,
      originalSender: ActorRef,
      resultHandler: ResultHandler[A]
    ): Future[A] =
      graph.relayAsk[A](QuineIdAtTime(quineId, atTime), unattributedMessage, originalSender)
  }
}
