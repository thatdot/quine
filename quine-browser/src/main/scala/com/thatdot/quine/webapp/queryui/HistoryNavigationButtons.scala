package com.thatdot.quine.webapp.queryui

import scala.scalajs.js.Date
import scala.util.Try
import scala.util.matching.Regex

import com.raquo.laminar.api.L._
import org.scalajs.dom
import org.scalajs.dom.{console, window}

import com.thatdot.quine.webapp.Sugar
import com.thatdot.quine.webapp.components.ToolbarButton

/** Bar of buttons for adjusting history */
object HistoryNavigationButtons {

  // Time parsing helpers

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

  private def currentTime(atTimeOpt: Option[Long]): String = atTimeOpt match {
    case None => "now"
    case Some(millis) => new Date(millis.toDouble).toISOString()
  }

  def apply(
    canStepBackward: Signal[Boolean],
    canStepForward: Signal[Boolean],
    isAnimating: Signal[Boolean],
    undo: () => Unit,
    undoMany: () => Unit,
    undoAll: () => Unit,
    animate: () => Unit,
    redo: () => Unit,
    redoMany: () => Unit,
    redoAll: () => Unit,
    makeCheckpoint: () => Unit,
    checkpointMenuItems: () => Seq[ToolbarButton.MenuAction],
    downloadHistory: Boolean => Unit,
    downloadGraphJsonLd: () => Unit,
    uploadHistory: dom.FileList => Unit,
    atTime: Signal[Option[Long]],
    canSetTime: Signal[Boolean],
    setTime: Option[Long] => Unit,
    toggleLayout: () => Unit,
    recenterViewport: () => Unit,
  ): HtmlElement = {
    var uploadInputEl: Option[dom.html.Input] = None

    div(
      flexGrow := "1",
      display := "flex",
      alignItems := "center",
      // Back button: left-click = previous, right-click = {Previous, Previous Checkpoint, Beginning}
      ToolbarButton(
        "ion-ios-skipbackward",
        "Undo previous change (right-click for more options)",
        enabled = canStepBackward,
        onClickAction = _ => undo(),
        menuActions = () =>
          Seq(
            ToolbarButton.MenuAction("Previous", "Undo previous change", undo),
            ToolbarButton.MenuAction("Previous Checkpoint", "Undo until previous checkpoint", undoMany),
            ToolbarButton.MenuAction("Beginning", "Undo all changes", undoAll),
          ),
      ),
      // Play/Pause
      ToolbarButton.dynamic(
        ionClass = isAnimating.map(a => if (a) "ion-ios-pause" else "ion-ios-play"),
        tooltipTitle = isAnimating.map(a => if (a) "Stop animating graph" else "Animate graph"),
        onClickAction = _ => animate(),
      ),
      // Forward button: left-click = next, right-click = {Next, Next Checkpoint, End}
      ToolbarButton(
        "ion-ios-skipforward",
        "Redo or apply next change (right-click for more options)",
        enabled = canStepForward,
        onClickAction = _ => redo(),
        menuActions = () =>
          Seq(
            ToolbarButton.MenuAction("Next", "Redo or apply next change", redo),
            ToolbarButton.MenuAction("Next Checkpoint", "Redo until next checkpoint", redoMany),
            ToolbarButton.MenuAction("End", "Redo all changes", redoAll),
          ),
      ),
      // Checkpoint button: left-click = create, right-click = navigate to checkpoint
      ToolbarButton(
        "ion-ios-location-outline",
        "Create a checkpoint (right-click to navigate checkpoints)",
        onClickAction = _ => makeCheckpoint(),
        menuActions = checkpointMenuItems,
      ),
      // Data button: left-click = download history, right-click = {History Log, Snapshot, Graph, Upload}
      ToolbarButton(
        "ion-ios-cloud-download-outline",
        "Download history log (right-click for more options)",
        onClickAction = _ => downloadHistory(false),
        menuActions = () =>
          Seq(
            ToolbarButton.MenuAction("History Log", "Download the full history log", () => downloadHistory(false)),
            ToolbarButton
              .MenuAction("History Snapshot", "Download the current history snapshot", () => downloadHistory(true)),
            ToolbarButton
              .MenuAction("Current Graph", "Download the current graph as JSON-LD", () => downloadGraphJsonLd()),
            ToolbarButton
              .MenuAction("Upload History", "Upload a history log file", () => uploadInputEl.foreach(_.click())),
          ),
      ),
      // Hidden file input for upload
      input(
        typ := "file",
        nameAttr := "file",
        display := "none",
        onMountCallback(ctx => uploadInputEl = Some(ctx.thisNode.ref)),
        onChange --> { e =>
          val files = e.target.asInstanceOf[dom.html.Input].files
          uploadHistory(files)
        },
      ),
      // Time button
      child <-- atTime.combineWith(canSetTime).map { case (atTimeOpt, canSet) =>
        val timeStr = currentTime(atTimeOpt)
        ToolbarButton.simple(
          "ion-ios-time-outline",
          s"Querying for time: $timeStr",
          enabled = Val(canSet),
          onClickAction = { _ =>
            if (canSet) {
              val enteredDate = window.prompt(
                s"""Enter the moment in time the UI should track and use for all queries. The moment entered must be one of:
                   |
                   |  \u2022 "now"
                   |  \u2022 A number (milliseconds elapsed since Unix epoch)
                   |  \u2022 A relative time (eg, "six seconds ago" or "-15s")
                   |  \u2022 An absolute time (eg, "6:47 PM December 21, 2043" or "2021-05-29T10:02:00.004Z")
                   |
                   |The moment currently being tracked is: $timeStr.
                   |
                   |WARNING: this will reset the query history and clear all currently rendered nodes from the browser window (the actual data is unaffected)
                   |""".stripMargin,
                timeStr,
              )
              enteredDate match {
                case null => // user clicked cancel
                case "now" =>
                  console.log("Query time set to the present moment")
                  setTime(None)
                case UnixLikeTime(ms) =>
                  console.log("Historical query time set to UNIX timestamp", enteredDate)
                  setTime(Some(ms))
                case ShorthandRelativeTime(timestampMs) =>
                  console.log(
                    "Historical query time set from offset timestamp",
                    enteredDate,
                    "to UNIX timestamp",
                    timestampMs,
                  )
                  setTime(Some(timestampMs))
                case SugaredDate(ms) =>
                  console.log("Historical query time set from date-like string", enteredDate)
                  setTime(Some(ms))
                case _ => window.alert(s"Invalid time provided: $enteredDate")
              }
            }
          },
        )
      },
      // Layout toggle
      ToolbarButton.simple(
        "ion-android-share-alt",
        "Toggle between a tree and graph layout of nodes",
        onClickAction = _ => toggleLayout(),
      ),
      // Recenter viewport
      ToolbarButton.simple(
        "ion-pinpoint",
        "Recenter the viewport to the initial location",
        onClickAction = _ => recenterViewport(),
      ),
    )
  }
}
