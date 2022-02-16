package com.thatdot.quine.graph.messaging

import akka.pattern.AskTimeoutException

class ExactlyOnceTimeoutException(msg: String) extends AskTimeoutException(msg)
