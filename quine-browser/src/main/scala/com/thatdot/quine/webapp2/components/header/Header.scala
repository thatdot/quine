package com.thatdot.quine.webapp2.components.header

import com.raquo.laminar.api.L._
import com.raquo.laminar.nodes.ReactiveHtmlElement
import org.scalajs.dom.HTMLElement

import com.thatdot.quine.webapp2.components.sidebar.SidebarState

object Header {
  def apply(
    sidebarStateVar: Var[SidebarState],
    userAvatar: Option[HtmlElement],
  ): ReactiveHtmlElement[HTMLElement] =
    headerTag(
      cls := "header container-fluid border-bottom px-4 sticky-top",
      button(
        typ := "button",
        i(cls := "cil-menu"),
        onClick.compose(_.sample(sidebarStateVar).map {
          case SidebarState.ShowSidebar => SidebarState.HideSidebar
          case SidebarState.HideSidebar => SidebarState.ShowSidebar
        }) --> sidebarStateVar,
      ),
      userAvatar.getOrElse(emptyNode),
    )
}
