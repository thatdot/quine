package com.thatdot.quine.webapp2.router

import io.circe.parser.decode
import io.circe.syntax._

object QuineOssRouter {
  import QuineOssPage._

  implicit val routerPage: RouterPage[QuineOssPage] = new RouterPage[QuineOssPage] {
    def getPageTitle(page: QuineOssPage): String = page.title
    def serializePage(page: QuineOssPage): String = page.asJson.noSpaces
    def deserializePage(pageStr: String): QuineOssPage = decode[QuineOssPage](pageStr).getOrElse(ExplorerUi)
  }

  def apply(apiV1: Boolean): AppRouter[QuineOssPage] =
    new AppRouter[QuineOssPage](new QuineOssRoutes(apiV1).routes)
}
