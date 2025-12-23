package com.thatdot.quine.webapp2.components

sealed trait RenderStrategy

object RenderStrategy {
  case object RenderAlwaysMountedPage extends RenderStrategy
  case object RenderRegularlyMountedPages extends RenderStrategy
}
