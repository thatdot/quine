package com.thatdot.quine.webapp.queryui

import scala.scalajs.js

import slinky.core._
import slinky.core.annotations.react
import slinky.core.facade.{Fragment, ReactRef}
import slinky.web.html._

import com.thatdot.quine.routes.SampleQuery
import com.thatdot.quine.webapp.hooks.LocalStorageHook.useLocalStorage
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
    downloadSvg: () => Unit,
    areSampleQueriesVisible: Boolean,
    setAreSampleQueriesVisible: Boolean => Unit,
    textareaRef: ReactRef[textarea.tag.RefType]
  )

  val component: FunctionalComponent[TopBar.Props] = FunctionalComponent[Props] { props =>
    val (textareaWidth, setTextareaWidth) = useLocalStorage("textareaWidth", "")
    val (textareaHeight, setTextareaHeight) = useLocalStorage("textareaHeight", "")

    div(style := js.Dynamic.literal(position = "relative", zIndex = 20))(
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
          props.sampleQueries,
          props.areSampleQueriesVisible,
          props.setAreSampleQueriesVisible,
          props.textareaRef,
          textareaWidth,
          textareaHeight,
          setTextareaWidth,
          setTextareaHeight
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
      ),
      SampleQueryWindow(
        props.updateQuery,
        props.textareaRef,
        props.sampleQueries,
        props.query,
        props.areSampleQueriesVisible,
        props.setAreSampleQueriesVisible,
        textareaWidth,
        textareaHeight
      )
    )
  }
}
