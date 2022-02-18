package com.thatdot.quine.webapp

import scala.annotation.nowarn
import scala.scalajs.js
import scala.scalajs.js.annotation.{JSGlobal, JSName}
import scala.scalajs.js.|

@js.native
trait DateCreateOptions extends js.Object {
  @nowarn
  val locale: js.UndefOr[String] = js.native
  @nowarn
  val past: js.UndefOr[Boolean] = js.native
  @nowarn
  val future: js.UndefOr[Boolean] = js.native
  @nowarn
  val fromUTC: js.UndefOr[Boolean] = js.native
  @nowarn
  val setUTC: js.UndefOr[Boolean] = js.native
  @nowarn
  @JSName("clone")
  val cloneVal: js.UndefOr[Boolean] = js.native
  @nowarn
  val params: js.UndefOr[js.Object] = js.native
}

@js.native
trait SugarDate extends js.Object {

  /** @see https://github.com/andrewplummer/Sugar/blob/3ca57818332473b601434001ac1445552d7753ff/lib/date.js#L2910
    */
  @nowarn
  def create(): js.Date = js.native
  @nowarn
  def create(d: String | Byte | Short | Int | Float | Double | js.Date): js.Date = js.native
  @nowarn
  def create(options: DateCreateOptions): js.Date = js.native
  @nowarn
  def create(d: String | Byte | Short | Int | Float | Double | js.Date, options: DateCreateOptions): js.Date = js.native

  @nowarn
  def isValid(d: js.Date): Boolean = js.native
}

@js.native
@JSGlobal
object Sugar extends js.Object {

  /** @see https://github.com/andrewplummer/Sugar/blob/master/sugar.d.ts#L288
    */
  @nowarn
  val Date: SugarDate = js.native

}
