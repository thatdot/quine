package com.thatdot.quine.webapp

import scala.scalajs.js
import scala.scalajs.js.annotation.JSImport

@JSImport("resources/logo.svg", JSImport.Default)
@js.native
object QuineLogo extends js.Object

/** Classes defined in `IndexCSS`.
  *
  * TODO: use `scalacss` to write this CSS inline
  */
object Styles {
  val grayClickable = "gray-clickable"
  val clickable = "clickable"
  val disabled = "disabled"
  val rightIcon = "right-icon"
  val navBarButton = "nav-bar-button"
  val messageBar = "message-bar"
  val messageBarResizeHandle = "message-bar-resize-handle"
  val messageBarButton = "message-bar-button"
  val navBar = "nav-bar"
  val navBarLogo = "nav-bar-logo"

  // Query input bar
  val queryInput = "query-input"
  val queryInputInput = "query-input-input"
  val queryInputButton = "query-input-button"

  // Query result sentiment
  val queryResultError = "query-result-error"
  val queryResultSuccess = "query-result-success"
  val queryResultEmpty = "query-result-empty"

  val cypherResultsTable = "cypher-results"

  // Context menu
  val contextMenu = "context-menu"

  // Loader related
  val loader = "loader"
  val loaderSpinner = "loader-spinner"
  val loaderCounter = "loader-counter"
  val loaderCancellable = "loader-cancellable"

  // Overlay
  val overlay = "overlay"
  val openOverlay = "open-overlay"
  val closedOverlay = "closed-overlay"

  // Sidebar
  val sideBar = "side-bar"
  val sideBarItem = "side-bar-item"
  val selectedSideBarItem = "side-bar-item selected"

}
