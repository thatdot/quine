package com.thatdot.quine.webapp2.components.sidebar

import com.raquo.laminar.api.L._
import com.raquo.waypoint.Router

object NavItem {
  def apply[Page](
    iconClass: String,
    label: String,
    isActive: Boolean = false,
    page: Page,
    router: Router[Page],
  ): LI =
    li(
      cls := "nav-item",
      a(
        cls := (if (isActive) "nav-link active" else "nav-link"),
        router.navigateTo(page),
        i(cls := s"nav-icon $iconClass"),
        label,
      ),
    )
}
