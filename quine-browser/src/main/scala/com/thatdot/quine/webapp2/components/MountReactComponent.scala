package com.thatdot.quine.webapp2.components

import com.raquo.laminar.api.L._
import slinky.core.facade.ReactElement

object MountReactComponent {
  def apply(component: ReactElement): HtmlElement = div(
    height := "100%",
    onMountCallback(ctx => slinky.web.ReactDOM.render(component, ctx.thisNode.ref): Unit),
    onUnmountCallback(ctx => slinky.web.ReactDOM.unmountComponentAtNode(ctx.ref)),
  )
}
