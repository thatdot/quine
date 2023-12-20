package com.thatdot.quine.webapp.queryui

import slinky.core._
import slinky.core.annotations.react
import slinky.web.html._

import com.thatdot.quine.routes.SampleQuery
import com.thatdot.quine.webapp.{Styles, ThatDotLogoImg}

/** Blue bar at the top of the page which contains the logo, query input,
  * navigation buttons, and counters
  */
@react object TopBar {

  case class Props(
    query: String,
    runningTextQuery: Boolean,
    queryBarColor: Option[String],
    sampleQueries: Seq[SampleQuery],
    foundNodesCount: Option[Int],
    foundEdgesCount: Option[Int],
    updateQuery: String => Unit,
    submitButton: (Boolean) => Unit,
    cancelButton: () => Unit,
    navButtons: HistoryNavigationButtons.Props,
    downloadSvg: () => Unit
  )

  val component: FunctionalComponent[TopBar.Props] = FunctionalComponent[Props] { props =>
    div(className := Styles.navBar)(
      img(
        src := ThatDotLogoImg.toString,
        className := Styles.navBarLogo,
        onClick := (e => if (e.shiftKey) props.downloadSvg())
      ),
      QueryTextareaInput(
        props.runningTextQuery,
        props.updateQuery,
        props.query,
        props.submitButton,
        props.cancelButton,
        props.sampleQueries
      ),
      HistoryNavigationButtons(
        props.navButtons.undoMany,
        props.navButtons.undo,
        props.navButtons.isAnimating,
        props.navButtons.animate,
        props.navButtons.redo,
        props.navButtons.redoMany,
        props.navButtons.makeCheckpoint,
        props.navButtons.checkpointContextMenu,
        props.navButtons.downloadHistory,
        props.navButtons.uploadHistory,
        props.navButtons.atTime,
        props.navButtons.setTime,
        props.navButtons.toggleLayout,
        props.navButtons.recenterViewport
      ),
      Counters.nodeEdgeCounters(props.foundNodesCount -> props.foundEdgesCount)
    )
  }
}
