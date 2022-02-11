package com.thatdot.quine.graph.messaging

/** Sent by exactly once actors to themselves to schedule a timeout */
case object GiveUpWaiting
