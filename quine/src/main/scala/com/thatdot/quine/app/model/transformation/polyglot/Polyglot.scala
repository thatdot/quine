package com.thatdot.quine.app.model.transformation.polyglot

object Polyglot {

  /** Value compatible with the org.graalvm.polyglot.Context.asValue parameter.
    * This can be passed to a GraalVM hosted language as a parameter value.
    */
  type HostValue = AnyRef

}
