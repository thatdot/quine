package com.thatdot.quine.webapp.queryui

import scala.scalajs.js.Date
import scala.scalajs.js.Dynamic.{literal => jsObj}
import scala.util.Try
import scala.util.matching.Regex

import org.scalajs.dom
import org.scalajs.dom.{console, html, window}
import slinky.core._
import slinky.core.annotations.react
import slinky.core.facade.{React, ReactElement, ReactRef}
import slinky.web.SyntheticMouseEvent
import slinky.web.html._

import com.thatdot.quine.webapp.{Styles, Sugar}

/** Bar of buttons for adjusting history */
@react
class HistoryNavigationButtons extends Component {

  /** @param undoMany action to take when clicking on the rewind button
    * @param undo action to take when clicking on the skip backward button
    * @param isAnimating are we animating? (decides whether the button is play/pause)
    * @param toggleAnimation action to take when clicking on the play/pause button
    * @param redo action to take when clicking on the fast-forward button
    * @param redoMany action to take when clicking on the skip forward button
    * @param makeCheckpoint action to take when clicking on the checkpoint button
    * @param checkpointContextMenu action to take when right-clicking on the checkpoint button
    * @param downloadHistory action to take when clicking on the download button, depending on shift
    * @param uploadHistory action to take when clicking on the upload button
    * @param atTime current time
    * @param setTime action to take when trying to set the time
    * @param toggleLayout action to take when toggling network layout
    * @param recenterViewport tell the UI to return to the center of the canvas
    */
  case class Props(
    undoMany: Option[() => Unit],
    undo: Option[() => Unit],
    isAnimating: Boolean,
    animate: () => Unit,
    redo: Option[() => Unit],
    redoMany: Option[() => Unit],
    makeCheckpoint: () => Unit,
    checkpointContextMenu: SyntheticMouseEvent[dom.HTMLElement] => Unit,
    downloadHistory: Boolean => Unit,
    uploadHistory: SyntheticEvent[html.Input, dom.Event] => Unit,
    atTime: Option[Long],
    setTime: Option[Option[Long] => Unit],
    toggleLayout: () => Unit,
    recenterViewport: () => Unit
  )

  // These buttons flash with different icons to indicate the successful action
  case class State(
    madeCheckpointConfirmation: Boolean,
    downloadConfirmation: Boolean
  )

  def initialState: com.thatdot.quine.webapp.queryui.HistoryNavigationButtons.State =
    State(madeCheckpointConfirmation = false, downloadConfirmation = false)

  // Human readable time
  def currentTime: String = props.atTime match {
    case None => "now"
    case Some(millis) => new Date(millis.toDouble).toISOString()
  }

  object ShorthandRelativeTime {
    val SecondsShorthand: Regex = """([\-+])\s*(\d+\.?\d*)\s*s(?:ec|ecs|econd|econds)?\s*$""".r
    val MinutesShorthand: Regex = """([\-+])\s*(\d+\.?\d*)\s*m(?:in|ins|inute|inutes)?\s*$""".r
    val HoursShorthand: Regex = """([\-+])\s*(\d+\.?\d*)\s*h(?:r|rs|our|ours)?\s*$""".r
    val DaysShorthand: Regex = """([\-+])\s*(\d+\.?\d*)\s*d(?:ay|ays)?\s*$""".r
    def nowMillis: Double = new Date().getTime()
    def unapply(timestamp: String): Option[Long] = (timestamp match {
      case SecondsShorthand("-", seconds) => Some(nowMillis - seconds.toDouble * 1000)
      case SecondsShorthand("+", seconds) => Some(nowMillis + seconds.toDouble * 1000)
      case MinutesShorthand("-", minutes) => Some(nowMillis - minutes.toDouble * 1000 * 60)
      case MinutesShorthand("+", minutes) => Some(nowMillis + minutes.toDouble * 1000 * 60)
      case HoursShorthand("-", hours) => Some(nowMillis - hours.toDouble * 1000 * 60 * 60)
      case HoursShorthand("+", hours) => Some(nowMillis + hours.toDouble * 1000 * 60 * 60)
      case DaysShorthand("-", days) => Some(nowMillis - days.toDouble * 1000 * 60 * 60 * 24)
      case DaysShorthand("+", days) => Some(nowMillis + days.toDouble * 1000 * 60 * 60 * 24)
      case _ => None
    }).map(_.toLong)
  }
  object UnixLikeTime {
    def unapply(millis: String): Option[Long] =
      Try(millis.toLong).toOption.map(millisLong => Sugar.Date.create(millisLong.toDouble).getTime().toLong)
  }
  object SugaredDate {
    def unapply(datestr: String): Option[Long] = {
      val sugarDate = Sugar.Date.create(datestr)
      if (Sugar.Date.isValid(sugarDate)) Some(sugarDate.getTime().toLong)
      else None
    }
  }

  // Hidden input that drives the upload button
  val uploadInputRef: ReactRef[html.Input] = React.createRef[html.Input]

  def render(): ReactElement =
    div(style := jsObj(flexGrow = "1", display = "flex"))(
      HistoryNavButton(
        "ion-ios-rewind",
        "Undo until previous checkpoint",
        onClick = props.undoMany.map(func => (_ => func()))
      ),
      HistoryNavButton(
        "ion-ios-skipbackward",
        "Undo previous change",
        onClick = props.undo.map(func => (_ => func()))
      ),
      HistoryNavButton(
        if (props.isAnimating) "ion-ios-pause" else "ion-ios-play",
        if (props.isAnimating) "Stop animating graph" else "Animate graph",
        onClick = Some(_ => props.animate())
      ),
      HistoryNavButton(
        "ion-ios-skipforward",
        "Redo or apply next change",
        onClick = props.redo.map(func => (_ => func()))
      ),
      HistoryNavButton(
        "ion-ios-fastforward",
        "Redo until next checkpoint",
        onClick = props.redoMany.map(func => (_ => func()))
      ),
      HistoryNavButton(
        s"ion-ios-location${if (state.madeCheckpointConfirmation) "" else "-outline"}",
        "Create a checkpoint",
        onClick = Some { _ =>
          setState(
            _.copy(madeCheckpointConfirmation = true),
            () => {
              window.setTimeout(() => setState(_.copy(madeCheckpointConfirmation = false)), 2000)
              ()
            }
          )
          props.makeCheckpoint()
        },
        onContextMenu = Some(props.checkpointContextMenu)
      ),
      HistoryNavButton(
        if (state.downloadConfirmation) "ion-checkmark-round" else "ion-ios-cloud-download-outline",
        "Download history log. Shift click to download only the current state.",
        onClick = Some { e =>
          setState(
            _.copy(downloadConfirmation = true),
            () => {
              window.setTimeout(() => setState(_.copy(downloadConfirmation = false)), 2000)
              ()
            }
          )
          props.downloadHistory(e.shiftKey)
        }
      ),
      HistoryNavButton(
        "ion-ios-cloud-upload-outline",
        "Upload a history log.",
        onClick = Some(_ => uploadInputRef.current.click())
      ),
      input(
        `type` := "file",
        ref := uploadInputRef,
        name := "file",
        style := jsObj(display = "none"),
        onChange := (e => props.uploadHistory(e))
      ),
      HistoryNavButton(
        "ion-ios-time-outline",
        s"Querying for time: $currentTime",
        onClick = props.setTime.map { (func: Option[Long] => Unit) => _ =>
          {
            val enteredDate = window.prompt(
              s"""Enter the moment in time the UI should track and use for all queries. The moment entered must be one of:
               |
               |  \u2022 "now"
               |  \u2022 A number (milliseconds elapsed since Unix epoch)
               |  \u2022 A relative time (eg, "six seconds ago" or "-15s")
               |  \u2022 An absolute time (eg, "6:47 PM December 21, 2043" or "2021-05-29T10:02:00.004Z")
               |
               |The moment currently being tracked is: $currentTime.
               |
               |WARNING: this will reset the query history and clear all currently rendered nodes from the browser window (the actual data is unaffected)
               |""".stripMargin,
              currentTime
            )
            enteredDate match {
              case null => // this indicates that the user clicked "cancel", so do nothing
              case "now" =>
                console.log("Query time set to the present moment")
                func(None)
              case UnixLikeTime(ms) =>
                console.log("Historical query time set to UNIX timestamp", enteredDate)
                func(Some(ms))
              case ShorthandRelativeTime(timestampMs) =>
                console.log(
                  "Historical query time set from offset timestamp",
                  enteredDate,
                  "to UNIX timestamp",
                  timestampMs
                )
                func(Some(timestampMs))
              case SugaredDate(ms) =>
                console.log("Historical query time set from date-like string", enteredDate)
                func(Some(ms))
              case _ => window.alert(s"Invalid time provided: $enteredDate")
            }
          }
        }
      ),
      HistoryNavButton(
        "ion-android-share-alt",
        "Toggle between a tree and graph layout of nodes",
        onClick = Some(_ => props.toggleLayout())
      ),
      HistoryNavButton(
        "ion-pinpoint",
        "Recenter the viewport to the initial location",
        onClick = Some(_ => props.recenterViewport())
      )
    )
}

/** Button on the history navigation bar */
@react
object HistoryNavButton {

  /** `ionicons`-driven button */
  case class Props(
    ionClass: String,
    tooltipTitle: String,
    onClick: Option[SyntheticMouseEvent[dom.HTMLElement] => Unit] = None,
    onContextMenu: Option[SyntheticMouseEvent[dom.HTMLElement] => Unit] = None
  )

  val component: FunctionalComponent[HistoryNavButton.Props] = FunctionalComponent[Props] {
    case Props(ionClass, tooltipTitle, onClickAct, onContextMenuAct) =>
      val classes = List(
        ionClass,
        Styles.navBarButton,
        if (onClickAct.isEmpty) Styles.disabled else Styles.clickable
      )
      i(
        className := classes.mkString(" "),
        title := tooltipTitle,
        onClick := (e => onClickAct.foreach(_.apply(e))),
        onContextMenu := (e => onContextMenuAct.foreach(_.apply(e)))
      )()
  }
}
