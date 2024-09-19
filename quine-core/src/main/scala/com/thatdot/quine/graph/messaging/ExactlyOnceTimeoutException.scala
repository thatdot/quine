package com.thatdot.quine.graph.messaging

import org.apache.pekko.pattern.AskTimeoutException

import com.thatdot.quine.util.QuineError

case class ExactlyOnceTimeoutException(msg: String) extends AskTimeoutException(msg) with QuineError
