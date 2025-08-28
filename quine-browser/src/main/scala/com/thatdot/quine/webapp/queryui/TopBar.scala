package com.thatdot.quine.webapp.queryui

import scala.scalajs.js.Dynamic.{literal => jsObj}

import slinky.core._
import slinky.core.annotations.react
import slinky.web.html._

import com.thatdot.quine.routes.SampleQuery
import com.thatdot.quine.webapp.{QuineLogo, Styles}

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
    submitButton: Boolean => Unit,
    cancelButton: () => Unit,
    navButtons: HistoryNavigationButtons.Props,
    downloadSvg: () => Unit,
  )

  val component: FunctionalComponent[TopBar.Props] = FunctionalComponent[Props] { props =>
    val queryInputStyle = if (props.runningTextQuery) {
      jsObj(animation = "activequery 1.5s ease infinite")
    } else {
      jsObj(backgroundColor = props.queryBarColor.getOrElse[String]("white"))
    }

    val buttonTitle: String =
      if (props.runningTextQuery) "Cancel query"
      else "Hold \"Shift\" to return results as a table"

    div(className := Styles.navBar)(
      div(style := jsObj(flex = "0 1 auto", minWidth = "128px"))(
        img(
          src := QuineLogo.toString,
          className := Styles.navBarLogo,
          onClick := (e => if (e.shiftKey) props.downloadSvg()),
        ),
      ),
      div(className := Styles.queryInput)(
        input(
          `type` := "text",
          list := "starting-queries",
          placeholder := "Query returning nodes",
          className := Styles.queryInputInput,
          style := queryInputStyle,
          value := props.query,
          onChange := (e => props.updateQuery(e.target.value)),
          onKeyUp := (e => if (e.key == "Enter") props.submitButton(e.shiftKey)),
          disabled := props.runningTextQuery,
        ),
        datalist(id := "starting-queries")(
          props.sampleQueries.map(q => option(value := q.query)(q.name)): _*,
        ),
        button(
          className := s"${Styles.grayClickable} ${Styles.queryInputButton}",
          onClick := (e => if (props.runningTextQuery) props.cancelButton() else props.submitButton(e.shiftKey)),
          title := buttonTitle,
        )(if (props.runningTextQuery) "Cancel" else "Query"),
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
        props.navButtons.recenterViewport,
      ),
      Counters.nodeEdgeCounters(props.foundNodesCount -> props.foundEdgesCount),
    )
  }
}
