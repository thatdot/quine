package com.thatdot.quine.graph

import com.thatdot.quine.util.QuineError

class QuineRuntimeFutureException(val msg: String, val cause: Throwable)
    extends RuntimeException(msg, cause)
    with QuineError
