package com.thatdot.quine.graph

class QuineRuntimeFutureException(val msg: String, val cause: Throwable) extends RuntimeException(msg, cause)
