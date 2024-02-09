package com.thatdot.quine.util

// Rename so we don't get a shadowing warning
import java.util.{concurrent => juc}

/** Helper for unwrapping the CompletionExceptions that Java 8 CompletableFutures wraps every
  * Exception in, for checked exception reasons.
  * Until someone adds this to the library we're using to convert them to Scala Futures
  * https://github.com/scala/scala-java8-compat/issues/120
  */
object CompletionException {
  def unapply(err: Throwable): Option[Throwable] = Option.when(err.isInstanceOf[juc.CompletionException])(err.getCause)
}
