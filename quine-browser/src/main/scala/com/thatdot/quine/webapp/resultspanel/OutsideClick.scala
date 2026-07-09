package com.thatdot.quine.webapp.resultspanel

import scala.scalajs.js

import com.raquo.laminar.api.L._
import org.scalajs.dom
import org.scalajs.dom.document

/** Dismiss-on-outside-click plumbing for pop-overs (e.g. the export menu). While the
  * pop-over is open, a document-level `mousedown` outside the element these modifiers
  * are applied to invokes `onOutside`; the listener is installed only while open and
  * removed on close and on unmount.
  */
object OutsideClick {

  def dismiss(active: Signal[Boolean], onOutside: () => Unit): Modifier[HtmlElement] = {
    var self: Option[dom.Element] = None
    val onDown: js.Function1[dom.MouseEvent, Unit] = (e: dom.MouseEvent) =>
      if (!self.exists(_.contains(e.target.asInstanceOf[dom.Node]))) onOutside()

    Seq[Modifier[HtmlElement]](
      onMountCallback(ctx => self = Some(ctx.thisNode.ref)),
      active --> { open =>
        if (open) document.addEventListener("mousedown", onDown)
        else document.removeEventListener("mousedown", onDown)
      },
      onUnmountCallback { _ =>
        self = None
        document.removeEventListener("mousedown", onDown)
      },
    )
  }
}
