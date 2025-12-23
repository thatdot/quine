package com.thatdot.quine.webapp2.router

import com.raquo.waypoint._

trait RouterPage[Page] {
  def getPageTitle(page: Page): String
  def serializePage(page: Page): String
  def deserializePage(pageStr: String): Page
}

class AppRouter[Page](routes: List[Route[_ <: Page, _]])(implicit val productPage: RouterPage[Page])
    extends Router[Page](
      routes = routes,
      getPageTitle = page => productPage.getPageTitle(page),
      serializePage = page => productPage.serializePage(page),
      deserializePage = pageStr => productPage.deserializePage(pageStr),
    )
