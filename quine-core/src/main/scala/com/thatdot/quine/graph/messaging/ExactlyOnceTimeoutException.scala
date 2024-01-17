package com.thatdot.quine.graph.messaging

import org.apache.pekko.pattern.AskTimeoutException

class ExactlyOnceTimeoutException(msg: String) extends AskTimeoutException(msg)
