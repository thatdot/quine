package com.thatdot.quine.webapp.queryui

import com.raquo.laminar.api.L._
import org.scalajs.dom

import com.thatdot.quine.webapp.Styles
import com.thatdot.quine.webapp.components.ToolbarButton

/** Bar of buttons for adjusting history */
object HistoryNavigationButtons {

  private def currentTime(atTimeOpt: Option[Long]): String = atTimeOpt.fold("now")(AtTimeInput.formatIso)

  /** The clock button. While a time is pinned the glyph fills and turns amber, the hue the app
    * already uses for a paused stream, not the blue that means hover or pressed elsewhere in
    * the bar; the words ("as of 3 hours ago") are on the canvas, in [[AtTimeTag]].
    */
  private def queryTime(
    atTime: Signal[Option[Long]],
    canSetTime: Signal[Boolean],
    openAtTimeModal: () => Unit,
  ): HtmlElement =
    htmlTag("i")(
      cls <-- atTime
        .combineWith(canSetTime)
        .map { case (t, canSet) =>
          val icon = if (t.isDefined) "ion-ios-time" else "ion-ios-time-outline"
          val state = if (canSet) Styles.clickable else Styles.disabled
          val pin = if (t.isDefined) s" ${AtTimeModalStyles.toolbarPinned}" else ""
          s"$icon ${Styles.navBarButton} $state$pin"
        }
        .distinct,
      title <-- atTime.map(t => s"Querying for time: ${currentTime(t)}").distinct,
      onClick.compose(_.sample(canSetTime)) --> Observer[Boolean](canSet => if (canSet) openAtTimeModal()),
    )

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
    checkpointMenuItemsAvailable: Signal[Boolean],
    downloadHistory: Boolean => Unit,
    downloadGraphJsonLd: () => Unit,
    uploadHistory: dom.FileList => Unit,
    atTime: Signal[Option[Long]],
    canSetTime: Signal[Boolean],
    openAtTimeModal: () => Unit,
    toggleLayout: () => Unit,
    recenterViewport: () => Unit,
  ): HtmlElement = {
    var uploadInputEl: Option[dom.html.Input] = None

    div(
      flexGrow := "0",
      flexShrink := "0",
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
        hasExtraOptions = checkpointMenuItemsAvailable,
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
      // Time button: opens the query-time dialog (AtTimeModal), mounted by the host at viewport
      // level alongside the other explorer modals.
      queryTime(atTime, canSetTime, openAtTimeModal),
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
      // Reset canvas (clear canvas / reset persisted state) lives in the junk drawer's
      // Maintenance section (design doc §4), wired directly by the host.
    )
  }
}
