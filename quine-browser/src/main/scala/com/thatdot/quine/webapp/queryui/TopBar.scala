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
    foundNodesCount: Signal[Option[Int]],
    foundEdgesCount: Signal[Option[Int]],
    submitButton: UiQueryType => Unit,
    cancelButton: () => Unit,
    navButtons: HtmlElement,
    useV2Api: Boolean,
    qpEnabled: Boolean,
    serverUrl: Option[String],
    trailing: Option[HtmlElement] = None,
    permissions: Option[Set[String]] = None,
    bookmark: BookmarkUi,
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
        bookmark = bookmark,
      ),
      navButtons,
      // Reserve the counters' horizontal footprint so the slot keeps a stable width whether or
      // not the counters are shown. submitQuery resets foundNodesCount/foundEdgesCount to None at
      // the start of every submit (they repopulate when results arrive), so without a reserved
      // slot the freed space is absorbed by the flex-grow query input — which visibly widens and
      // snaps back on each submit. min-width matches the rendered counters' footprint (two icons
      // + counts).
      div(
        flexGrow := "0",
        minWidth := "116px",
        display := "flex",
        alignItems := "center",
        justifyContent := "flex-end",
        child <-- foundNodesCount.combineWith(foundEdgesCount).map {
          case (None, None) => emptyNode
          case (n, e) => Counters.nodeEdgeCounters(n, e)
        },
      ),
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
