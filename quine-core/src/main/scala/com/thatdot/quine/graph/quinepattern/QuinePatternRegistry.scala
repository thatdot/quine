package com.thatdot.quine.graph.quinepattern

import org.apache.pekko.actor.Actor

import com.thatdot.quine.persistor.PrimePersistor

/** This is a placeholder for future QuinePattern functionality
  */
class QuinePatternRegistry(persistor: PrimePersistor) extends Actor {
  override def receive: Receive = { case _ =>
    ()
  }
}
