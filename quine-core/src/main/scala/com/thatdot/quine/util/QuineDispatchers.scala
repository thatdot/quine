package com.thatdot.quine.util

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.dispatch.MessageDispatcher

import com.typesafe.scalalogging.LazyLogging

import com.thatdot.quine.util.QuineDispatchers._

/** Initializes and maintains the canonical reference to each of the dispatchers Quine uses.
  * Similar to pekko-typed's DispatcherSelector
  *
  * See quine-core's `reference.conf` for definitions and documentation of the dispatchers
  *
  * @param system the actorsystem for which the dispatchers will be retrieved
  */
class QuineDispatchers(system: ActorSystem) extends LazyLogging {
  val shardDispatcherEC: MessageDispatcher =
    system.dispatchers.lookup(shardDispatcherName)
  val nodeDispatcherEC: MessageDispatcher =
    system.dispatchers.lookup(nodeDispatcherName)
  val blockingDispatcherEC: MessageDispatcher =
    system.dispatchers.lookup(blockingDispatcherName)
}
object QuineDispatchers {
  val shardDispatcherName = "pekko.quine.graph-shard-dispatcher"
  val nodeDispatcherName = "pekko.quine.node-dispatcher"
  val blockingDispatcherName = "pekko.quine.persistor-blocking-dispatcher"
}
