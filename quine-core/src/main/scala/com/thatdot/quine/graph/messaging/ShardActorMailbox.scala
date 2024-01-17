package com.thatdot.quine.graph.messaging

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.dispatch.{PriorityGenerator, UnboundedStablePriorityMailbox}

import com.typesafe.config.Config

import com.thatdot.quine.graph.SleepOutcome

class ShardActorMailbox(settings: ActorSystem.Settings, config: Config)
    extends UnboundedStablePriorityMailbox(
      PriorityGenerator { // Lower priority is handled first
        case _: SleepOutcome => 0
        case BaseMessage.DeliveryRelay(_, _, true) => 1 // needsAck == true
        case _ => 2
      }
    )
