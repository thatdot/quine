package com.thatdot.quine.webapp.util

import scala.concurrent.{ExecutionContext, Future}

import com.raquo.laminar.api.L._
import org.scalajs.dom

/** Laminar-native polling stream.
  *
  * Replaces manual `setTimeout`/`clearTimeout` polling loops with a declarative
  * `EventStream` that automatically manages its lifecycle via Laminar ownership.
  */
object PollingStream {

  /** Create an event stream that periodically polls an async function.
    *
    * The first fetch fires immediately on stream start (t=0), then repeats every
    * `intervalMs`. We construct the tick stream explicitly — `fromValue(0)` for the
    * t=0 tick, and `periodic(intervalMs).drop(1)` for the subsequent ones — rather
    * than relying on `EventStream.periodic` firing synchronously on start, since
    * that is implementation detail rather than a documented contract.
    *
    * @param intervalMs polling interval in milliseconds
    * @param fetch async function to call on each tick
    * @return an EventStream that emits each successful result
    */
  def apply[A](intervalMs: Int)(fetch: => Future[A])(implicit ec: ExecutionContext): EventStream[A] =
    EventStream
      .merge(EventStream.fromValue(0), EventStream.periodic(intervalMs).drop(1))
      .flatMapSwitch(_ => EventStream.fromFuture(fetch))
}

/** Laminar-native local storage integration.
  *
  * Creates reactive `Var`s backed by `window.localStorage`, with automatic
  * persistence on value changes.
  */
object LocalStorage {

  /** Create a `Var` initialized from localStorage, falling back to a default.
    *
    * @param key localStorage key
    * @param default default value if key is absent
    * @return a Var whose initial value comes from localStorage
    */
  def persistent(key: String, default: String): Var[String] = {
    val stored = Option(dom.window.localStorage.getItem(key)).getOrElse(default)
    Var(stored)
  }

  /** Binder that persists a `Var`'s value to localStorage on every change.
    *
    * @param key localStorage key
    * @param v the Var to sync
    * @return a Binder to attach to an element
    */
  def syncToStorage(key: String, v: Var[String]): Binder[HtmlElement] =
    v.signal --> { value =>
      dom.window.localStorage.setItem(key, value)
    }
}
