package com.thatdot.quine.webapp2

import com.thatdot.quine.webapp2.LaminarRoot.NavItemData
import com.thatdot.quine.webapp2.router.QuineOssPage
import com.thatdot.quine.webapp2.router.QuineOssPage._

class QuineOssNavItems(apiV1: Boolean) {
  private val docsNavItem =
    NavItemData[QuineOssPage](
      name = "Interactive Docs",
      icon = "cil-library",
      page = if (apiV1) DocsV1 else DocsV2,
    )

  private val navItems =
    List(
      NavItemData[QuineOssPage](name = "Exploration UI", icon = "cil-search", page = ExplorerUi),
      docsNavItem,
      NavItemData[QuineOssPage](name = "Metrics", icon = "cil-speedometer", page = Metrics),
    )
}

object QuineOssNavItems {
  def apply(apiV1: Boolean): List[NavItemData[QuineOssPage]] = new QuineOssNavItems(apiV1).navItems
}
