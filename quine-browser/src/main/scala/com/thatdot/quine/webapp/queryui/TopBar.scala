package com.thatdot.quine.webapp.queryui

import com.raquo.laminar.api.L._

import com.thatdot.quine.routes.SampleQuery
import com.thatdot.quine.webapp.Styles

object TopBar {

  def apply(
    query: Signal[String],
    updateQuery: String => Unit,
    runningTextQuery: Signal[Boolean],
    queryBarColor: Signal[Option[String]],
    sampleQueries: Signal[Seq[SampleQuery]],
    submitButton: UiQueryType => Unit,
    cancelButton: () => Unit,
    navButtons: HtmlElement,
    useV2Api: Boolean,
    qpEnabled: Boolean,
    serverUrl: Option[String],
    trailing: Option[HtmlElement] = None,
    permissions: Option[Set[String]] = None,
    bookmarkDialog: HtmlElement,
    onBookmark: () => Unit,
    onOpenTapModal: () => Unit,
  ): HtmlElement = {
    val canRead = permissions match {
      case Some(perms) => Set("GraphRead").subsetOf(perms)
      case None => true
    }

    div(
      cls := Styles.navBar,
      MonacoQueryInput(
        query = query,
        updateQuery = updateQuery,
        runningTextQuery = runningTextQuery,
        queryBarColor = queryBarColor,
        sampleQueries = sampleQueries,
        submitButton = submitButton,
        cancelButton = cancelButton,
        canRead = canRead,
        useV2Api = useV2Api,
        qpEnabled = qpEnabled,
        serverUrl = serverUrl,
        bookmarkDialog = bookmarkDialog,
        onBookmark = onBookmark,
        onOpenTapModal = onOpenTapModal,
      ),
      navButtons,
      // Node/edge counters no longer live here (design doc §5 / Lane D): they now render as the
      // ephemeral ResultCountIndicator that fades in over the canvas on query completion, rather
      // than sitting permanently in the bar. See QueryUi's ResultCountIndicator mount.
      // Trailing widget (graph selector) pinned to the far right, preceded by a
      // vertical divider. Fixed width so it never resizes the query input.
      trailing
        .map { t =>
          div(
            cls := Styles.navBarTrailing,
            span(cls := Styles.navBarDivider),
            t,
          )
        }
        .getOrElse(emptyNode),
    )
  }

}
