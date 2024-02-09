package com.thatdot.quine.graph.messaging

import scala.concurrent.Future

import org.apache.pekko.actor.ActorRef
import org.apache.pekko.stream.Materializer
import org.apache.pekko.util.Timeout

import com.thatdot.quine.graph.BaseGraph

trait QuineRefOps {

  protected def graph: BaseGraph

  /** Support sending typed messages to some destination in the graph */
  implicit final class RichQuineRef(quineRef: QuineRef) {

    /** Tell a message to some location in the graph.
      *
      * @param message the message to send
      * @param originalSender who originally sent the message (for debug only)
      */
    def !(message: QuineMessage)(implicit originalSender: ActorRef): Unit =
      graph.relayTell(quineRef, message, originalSender)

    /** Ask a message to some location in the graph.
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
      graph.relayAsk[A](quineRef, unattributedMessage, originalSender)
  }

  /** Support replying to a message */
  implicit final class RichAttributableQuineMessage[A: ResultHandler](
    message: QuineMessage with AskableQuineMessage[A]
  ) {
    def ?!(response: A)(implicit resultHandler: ResultHandler[A], mat: Materializer): Unit = {
      val messageStaysInJvm = graph.isOnThisHost(message.replyTo)
      resultHandler.respond(
        message.replyTo,
        response,
        graph,
        messageStaysInJvm
      )
    }
  }
}
