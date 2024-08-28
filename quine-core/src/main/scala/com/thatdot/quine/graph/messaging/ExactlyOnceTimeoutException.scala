package com.thatdot.quine.graph.messaging

import org.apache.pekko.pattern.AskTimeoutException

class ExactlyOnceTimeoutException(val msg: String) extends AskTimeoutException(msg)
