package com.thatdot.quine.webapp.queryui

import scala.scalajs.js
import scala.scalajs.js.Date
import scala.util.Try
import scala.util.matching.Regex

import com.raquo.laminar.api.L._
import org.scalajs.dom
import org.scalajs.dom.{console, window}

import com.thatdot.quine.webapp.{Styles, Sugar}

/** Button on the history navigation bar */
object HistoryNavButton {

  /** Standard button with a reactively-toggled enabled state */
  def apply(
    ionClass: String,
    tooltipTitle: String,
    enabled: Signal[Boolean] = Val(true),
    onClickAction: dom.MouseEvent => Unit = _ => (),
    onContextMenuAction: Option[dom.MouseEvent => Unit] = None,
  ): HtmlElement =
    htmlTag("i")(
      cls <-- enabled.map { e =>
        s"$ionClass ${Styles.navBarButton} ${if (e) Styles.clickable else Styles.disabled}"
      },
      title := tooltipTitle,
      onClick.compose(_.withCurrentValueOf(enabled).collect { case (e, true) => e }) --> onClickAction,
      onContextMenu --> (e => onContextMenuAction.foreach(_(e))),
    )

  /** Button with a dynamically changing icon (for play/pause) */
  def dynamic(
    ionClass: Signal[String],
    tooltipTitle: Signal[String],
    onClickAction: dom.MouseEvent => Unit,
  ): HtmlElement =
    htmlTag("i")(
      cls <-- ionClass.map(icon => s"$icon ${Styles.navBarButton} ${Styles.clickable}"),
      title <-- tooltipTitle,
      onClick --> onClickAction,
    )
}

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
    undoMany: () => Unit,
    undo: () => Unit,
    animate: () => Unit,
    redo: () => Unit,
    redoMany: () => Unit,
    makeCheckpoint: () => Unit,
    checkpointContextMenu: dom.MouseEvent => Unit,
    downloadHistory: Boolean => Unit,
    downloadGraphJsonLd: () => Unit,
    uploadHistory: dom.FileList => Unit,
    atTime: Signal[Option[Long]],
    canSetTime: Signal[Boolean],
    setTime: Option[Long] => Unit,
    toggleLayout: () => Unit,
    recenterViewport: () => Unit,
  ): HtmlElement = {
    val confirmCheckpointVar = Var(false)
    val confirmDownloadVar = Var(false)
    val downloadMenuOpenVar = Var(false)

    var downloadWrapperEl: Option[dom.html.Element] = None
    var uploadInputEl: Option[dom.html.Input] = None

    val handleDocumentClick: js.Function1[dom.MouseEvent, Unit] = { (event: dom.MouseEvent) =>
      if (downloadMenuOpenVar.now()) {
        val target = event.target.asInstanceOf[dom.Node]
        val clickedInside = downloadWrapperEl.exists(_.contains(target))
        if (!clickedInside) downloadMenuOpenVar.set(false)
      }
    }

    def downloadMenuItem(label: String, action: () => Unit): HtmlElement =
      li(
        onClick --> { _ =>
          confirmDownloadVar.set(true)
          downloadMenuOpenVar.set(false)
          window.setTimeout(() => confirmDownloadVar.set(false), 2000)
          action()
        },
        label,
      )

    div(
      flexGrow := "1",
      display := "flex",
      onMountCallback(_ => dom.document.addEventListener("click", handleDocumentClick)),
      onUnmountCallback(_ => dom.document.removeEventListener("click", handleDocumentClick)),
      HistoryNavButton(
        "ion-ios-rewind",
        "Undo until previous checkpoint",
        enabled = canStepBackward,
        onClickAction = _ => undoMany(),
      ),
      HistoryNavButton(
        "ion-ios-skipbackward",
        "Undo previous change",
        enabled = canStepBackward,
        onClickAction = _ => undo(),
      ),
      HistoryNavButton.dynamic(
        ionClass = isAnimating.map(a => if (a) "ion-ios-pause" else "ion-ios-play"),
        tooltipTitle = isAnimating.map(a => if (a) "Stop animating graph" else "Animate graph"),
        onClickAction = _ => animate(),
      ),
      HistoryNavButton(
        "ion-ios-skipforward",
        "Redo or apply next change",
        enabled = canStepForward,
        onClickAction = _ => redo(),
      ),
      HistoryNavButton(
        "ion-ios-fastforward",
        "Redo until next checkpoint",
        enabled = canStepForward,
        onClickAction = _ => redoMany(),
      ),
      // Checkpoint button
      htmlTag("i")(
        cls <-- confirmCheckpointVar.signal.map { confirmed =>
          s"ion-ios-location${if (confirmed) "" else "-outline"} ${Styles.navBarButton} ${Styles.clickable}"
        },
        title := "Create a checkpoint",
        onClick --> { _ =>
          confirmCheckpointVar.set(true)
          window.setTimeout(() => confirmCheckpointVar.set(false), 2000)
          makeCheckpoint()
        },
        onContextMenu --> (e => checkpointContextMenu(e)),
      ),
      // Download wrapper
      div(
        position := "relative",
        display := "flex",
        alignItems := "center",
        onMountCallback(ctx => downloadWrapperEl = Some(ctx.thisNode.ref)),
        htmlTag("i")(
          cls <-- confirmDownloadVar.signal.map { confirmed =>
            val icon = if (confirmed) "ion-checkmark-round" else "ion-ios-cloud-download-outline"
            s"$icon ${Styles.navBarButton} ${Styles.clickable}"
          },
          title := "Download options",
          onClick --> { _ => downloadMenuOpenVar.update(!_) },
        ),
        ul(
          cls <-- downloadMenuOpenVar.signal.map { open =>
            s"download-dropdown-menu${if (open) " open" else ""}"
          },
          downloadMenuItem("History Log", () => downloadHistory(false)),
          downloadMenuItem("History Snapshot", () => downloadHistory(true)),
          downloadMenuItem("Current Graph", () => downloadGraphJsonLd()),
        ),
      ),
      // Upload button
      HistoryNavButton(
        "ion-ios-cloud-upload-outline",
        "Upload a history log.",
        onClickAction = _ => uploadInputEl.foreach(_.click()),
      ),
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
        HistoryNavButton(
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
      HistoryNavButton(
        "ion-android-share-alt",
        "Toggle between a tree and graph layout of nodes",
        onClickAction = _ => toggleLayout(),
      ),
      HistoryNavButton(
        "ion-pinpoint",
        "Recenter the viewport to the initial location",
        onClickAction = _ => recenterViewport(),
      ),
    )
  }
}
