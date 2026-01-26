package com.thatdot.quine.webapp2.components.sidebar

import com.raquo.laminar.api.L._
import com.raquo.waypoint.Router

import com.thatdot.quine.webapp2.LaminarRoot.NavItemData

sealed trait SidebarState
object SidebarState {
  object ShowSidebar extends SidebarState
  object HideSidebar extends SidebarState
}

object CoreUISidebar {
  def apply[Page](
    productName: String,
    navItems: Seq[NavItemData[Page]],
    router: Router[Page],
    showSidebarSignal: Signal[Boolean],
  ): Div =
    div(
      cls := "sidebar sidebar-dark sidebar-fixed border-end",
      cls("show") <-- showSidebarSignal,
      cls("hide") <-- showSidebarSignal.map(!_),
      idAttr := "sidebar",
      div(
        cls := "sidebar-header border-bottom",
        div(
          cls := "sidebar-brand",
          span(cls := "sidebar-brand-full", productName),
        ),
      ),
      ul(
        cls := "sidebar-nav",
        NavTitle("Navigation"),
        children <-- router.currentPageSignal.map { currentPage =>
          navItems
            .filter(!_.hidden)
            .map { navItem =>
              NavItem(
                iconClass = navItem.icon,
                label = navItem.name,
                page = navItem.page,
                router = router,
                isActive = navItem.page == currentPage,
              )
            }
        },
      ),
    )
}
