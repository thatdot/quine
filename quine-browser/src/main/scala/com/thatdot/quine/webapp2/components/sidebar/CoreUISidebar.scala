package com.thatdot.quine.webapp2.components.sidebar

import com.raquo.laminar.api.L._
import com.raquo.waypoint.Router

import com.thatdot.quine.webapp2.LaminarRoot.NavItemData

sealed trait SidebarState
object SidebarState {
  object Expanded extends SidebarState
  object Narrow extends SidebarState
}

object CoreUISidebar {
  def apply[Page](
    productName: String,
    navItems: Seq[NavItemData[Page]],
    router: Router[Page],
    userAvatar: Option[HtmlElement],
    sidebarStateVar: Var[SidebarState],
  ): Div =
    div(
      cls := "sidebar sidebar-light sidebar-fixed border-end d-flex flex-column",
      cls("sidebar-narrow") <-- sidebarStateVar.signal.map(_ == SidebarState.Narrow),
      idAttr := "sidebar",
      div(
        cls := "sidebar-header border-bottom",
        div(
          cls := "sidebar-brand d-flex justify-content-between align-items-center",
          span(cls := "sidebar-brand-full", productName),
        ),
        button(
          cls := "sidebar-toggler",
          typ := "button",
          onClick.compose(_.sample(sidebarStateVar).map {
            case SidebarState.Expanded => SidebarState.Narrow
            case SidebarState.Narrow => SidebarState.Expanded
          }) --> sidebarStateVar,
        ),
      ),
      ul(
        cls := "sidebar-nav flex-grow-1",
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
      userAvatar
        .map { avatar =>
          div(
            cls := "sidebar-footer border-top",
            avatar,
          )
        }
        .getOrElse(emptyNode),
    )
}
