package com.thatdot.quine.webapp2

import com.raquo.laminar.api.L._
import com.raquo.waypoint.Router

import com.thatdot.quine.webapp2.components.header.Header
import com.thatdot.quine.webapp2.components.sidebar.SidebarState.{HideSidebar, ShowSidebar}
import com.thatdot.quine.webapp2.components.sidebar.{CoreUISidebar, SidebarState}

object LaminarRoot {
  case class NavItemData[Page](name: String, icon: String, page: Page, hidden: Boolean = false)
  case class LaminarRootProps[Page](
    productName: String,
    navItems: Seq[NavItemData[Page]],
    router: Router[Page],
    views: HtmlElement,
    userAvatar: Option[HtmlElement],
  )

  def apply[Page](props: LaminarRootProps[Page]): Div = {
    val sidebarWidth = "255px"
    val sidebarStateVar = Var[SidebarState](SidebarState.ShowSidebar)

    val mainContentWidthSignal = sidebarStateVar.signal.map {
      case ShowSidebar => s"calc(100% - ${sidebarWidth})"
      case HideSidebar => "100%"
    }
    val showSidebarSignal = sidebarStateVar.signal.map {
      case ShowSidebar => true
      case HideSidebar => false
    }
    val marginLeftSignal = sidebarStateVar.signal.map {
      case ShowSidebar => sidebarWidth
      case HideSidebar => "0"
    }

    div(
      CoreUISidebar(
        productName = props.productName,
        navItems = props.navItems,
        router = props.router,
        showSidebarSignal = showSidebarSignal,
      ),
      div(
        cls := "d-flex flex-column",
        transition := "margin-left .15s ease, width .15s ease",
        marginLeft <-- marginLeftSignal,
        width <-- mainContentWidthSignal,
        height := "100vh",
        Header(sidebarStateVar, props.userAvatar),
        props.views,
      ),
    )
  }
}
