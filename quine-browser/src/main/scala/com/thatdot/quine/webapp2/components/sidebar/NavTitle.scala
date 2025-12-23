package com.thatdot.quine.webapp2.components.sidebar

import com.raquo.laminar.api.L._

object NavTitle {
  def apply(text: String): LI =
    li(cls := "nav-title", text)
}
