package com.thatdot.quine.webapp

import scala.scalajs.js

import com.raquo.laminar.api.L._
import com.raquo.waypoint.Router
import org.scalajs.dom

import com.thatdot.quine.webapp.components.sidebar.SidebarState.{Expanded, Narrow}
import com.thatdot.quine.webapp.components.sidebar.{CoreUISidebar, SidebarState}

object LaminarRoot {
  // CoreUI's mobile breakpoint media query (lg breakpoint = 992px, so max-width is 991.98px)
  private val MobileBreakpointQuery = "(max-width: 991.98px)"

  case class NavItemData[Page](name: String, icon: String, page: Page, hidden: Boolean = false)
  case class LaminarRootProps[Page](
    productName: String,
    logo: Option[HtmlElement],
    navItems: Seq[NavItemData[Page]],
    router: Router[Page],
    views: HtmlElement,
    userAvatar: Option[HtmlElement],
  )

  def apply[Page](props: LaminarRootProps[Page]): Div = {
    // Media query for responsive sidebar
    val mediaQuery = dom.window.matchMedia(MobileBreakpointQuery)

    // Determine initial state based on media query
    val initialState =
      if (mediaQuery.matches) SidebarState.Narrow
      else SidebarState.Expanded

    val sidebarStateVar = Var[SidebarState](initialState)

    val mainContentWidthSignal = sidebarStateVar.signal.map {
      case Expanded => "calc(100% - var(--cui-sidebar-width, 256px))"
      case Narrow => "calc(100% - var(--cui-sidebar-narrow-width, 4rem))"
    }
    val marginLeftSignal = sidebarStateVar.signal.map {
      case Expanded => "var(--cui-sidebar-width, 256px)"
      case Narrow => "var(--cui-sidebar-narrow-width, 4rem)"
    }

    // Sync sidebar state with viewport width changes
    val handleMediaChange: js.Function0[Unit] = { () =>
      val newState = if (mediaQuery.matches) SidebarState.Narrow else SidebarState.Expanded
      sidebarStateVar.set(newState)
    }

    div(
      // Set up media query listener via onchange
      onMountCallback { _ =>
        mediaQuery.asInstanceOf[js.Dynamic].onchange = handleMediaChange
      },
      onUnmountCallback { _ =>
        mediaQuery.asInstanceOf[js.Dynamic].onchange = null
      },
      CoreUISidebar(
        productName = props.productName,
        logo = props.logo,
        navItems = props.navItems,
        router = props.router,
        userAvatar = props.userAvatar,
        sidebarStateVar = sidebarStateVar,
      ),
      div(
        cls := "d-flex flex-column p-3",
        transition := "margin-left .15s ease, width .15s ease",
        marginLeft <-- marginLeftSignal,
        width <-- mainContentWidthSignal,
        height := "100vh",
        props.views,
      ),
    )
  }
}
