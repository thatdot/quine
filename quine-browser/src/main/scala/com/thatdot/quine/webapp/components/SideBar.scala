package com.thatdot.quine.webapp.components

import scala.scalajs.js

import org.scalajs.dom
import org.scalajs.dom.window
import slinky.core._
import slinky.core.annotations.react
import slinky.core.facade.ReactElement
import slinky.web.html._

import com.thatdot.quine.webapp.Styles

/** Structure of a page in [[PageWithSideBar]]
  *
  * @param icon Ionicon class name for an icon
  * @param name Title of the page (visible when the hamburger is clicked or on hover)
  * @param path URL path associated with this page
  * @param page context of the page
  * @param mountFunction An optional function to run when a Tab is 'mounted' (unhidden)
  */
final case class Tab(
  icon: String,
  name: String,
  path: String,
  page: facade.ReactElement,
  baseURI: String,
  mountFunction: Option[() => Unit] = None,
  hidden: Boolean = false,
)

/** Page which has a (fixed) side bar on the left. The side bar can be used to
  * navigate between different subpages.
  *
  * @note keeps all subpages lazily loaded (but the non-selected ones hidden). Doing this matters
  * for components like the `vis-network` or `SwaggerUI` one, where not _all_ state is captured by
  * the react component or where there is a network operation associated with the first mount.
  *
  * @note intercepts window `popstate` events to try to simulate navigation between tabs (so using
  * this embedded in another component might not work well)
  *
  * TODO: small screen support
  */
@react class PageWithSideBar extends Component {

  case class Props(children: Tab*)

  /** @param isOpen is the menu open?
    * @param selected index of page being displayed
    * @param visited indices of pages which have been displayed (and are now loaded but hidden)
    */
  case class State(
    isOpen: Boolean,
    selected: Int,
    visited: Vector[Boolean],
  ) {
    def switchToTab(newTab: Int): State = copy(
      isOpen = false,
      selected = newTab,
      visited = visited.updated(newTab, true),
    )
  }

  def initialState: com.thatdot.quine.webapp.components.PageWithSideBar.State = {
    val initialTab = props.children.view.zipWithIndex
      .find(t => t._1.path == window.location.pathname)
      .fold(0)(_._2)
    props.children.toList.lift(initialTab).foreach(_.mountFunction.foreach(_()))
    val visited = Vector.tabulate(props.children.length)(_ == initialTab)
    State(isOpen = false, initialTab, visited)
  }

  override def componentDidMount(): Unit = {
    super.componentDidMount()
    window.onpopstate = (event: dom.PopStateEvent) => {
      try {
        val pageIndexState = event.state.asInstanceOf[PageIndexState]
        setState(_.switchToTab(pageIndexState.pageIdx))
      } catch {
        case _: Throwable => // `event.state` must have come from an external `pushState`
      }
    }
  }

  def render(): ReactElement = {
    // Side-bar: a list of two-column table rows where the first column is an icon and the second column is a page title
    val sideBarItems: List[ReactElement] = props.children.toList.zipWithIndex.map { case (tab: Tab, idx: Int) =>
      val cls = if (state.selected == idx) Styles.selectedSideBarItem else Styles.sideBarItem
      tr(
        onClick := (_ =>
          setState { (s: State) =>
            val pageIndexState = new PageIndexState {
              val pageIdx = idx
            }
            window.history
              .pushState(pageIndexState, "", props.children(idx).baseURI.stripSuffix("/") + props.children(idx).path)

            tab.mountFunction.foreach(_())
            s.switchToTab(idx)
          },
        ),
        className := cls,
        key := s"item-$idx",
        hidden := tab.hidden,
      )(
        td(style := js.Dynamic.literal(textAlign = "center", padding = ".5em 0"), key := "tab-icon")(
          i(
            className := tab.icon,
            style := js.Dynamic.literal(fontSize = "2em"),
            title := tab.name,
          )(),
        ),
        td(key := "tab-name")(
          tab.name,
        ),
      )
    }
    window.dispatchEvent(new dom.Event("resize"))

    // a two-column row for the "hamburger menu" icon
    val drawerIcon: ReactElement = tr(
      onClick := (_ => setState(s => s.copy(isOpen = !s.isOpen))),
      className := Styles.sideBarItem,
      style := js.Dynamic.literal(padding = "1em .5em"),
      key := "hamburger",
    )(
      td(style := js.Dynamic.literal(textAlign = "center", padding = "1em 0"), key := "tab-icon")(
        i(
          className := "ion-android-menu",
          style := js.Dynamic.literal(fontSize = "2em"),
        ),
      ),
      td(key := "tab-name")(),
    )

    val overlayToggleClass = if (state.isOpen) Styles.openOverlay else Styles.closedOverlay
    val overlayDiv: ReactElement = div(
      className := s"${Styles.overlay} $overlayToggleClass",
      key := "overlay",
      onClick := (_ => setState(_.copy(isOpen = false))),
    )()

    val tabStripWidth = "3.5em"
    val tabPages: List[ReactElement] = props.children.view.zipWithIndex
      .zip(state.visited)
      .collect {
        case ((tab: Tab, idx: Int), visited: Boolean) if visited || state.selected == idx =>
          div(
            style := js.Dynamic.literal(
              zIndex = 0,
              display = if (state.selected == idx) "block" else "none",
              paddingLeft = tabStripWidth,
              height = "100%",
            ),
            key := s"page-$idx",
          )(
            tab.page,
          )
      }
      .toList

    div(
      className := Styles.sideBar,
      style := js.Dynamic.literal(width = if (state.isOpen) "14em" else tabStripWidth),
      key := "sidebar",
    )(
      table(
        thead(tr(th(style := js.Dynamic.literal(width = tabStripWidth, minWidth = tabStripWidth)), th())),
        tbody((drawerIcon :: sideBarItems): _*),
      ),
    ) :: overlayDiv :: tabPages
  }
}

/** State stored in the browser's `history` to track which tab we are on
  *
  * Every time we navigate to a tab, `pushState` is called  with the `state` parameter set to the
  * [[PageIndexState]] of the tab to which we are navigating. That means that we can set an event
  * handler for `onpopstate` to intercept all back/forward navigation where the state is a
  * [[PageIndexState]] and then switch to that tab.
  */
private trait PageIndexState extends js.Object {
  val pageIdx: Int
}
