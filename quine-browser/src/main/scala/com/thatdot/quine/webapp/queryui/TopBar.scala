package com.thatdot.quine.webapp.queryui

import com.raquo.laminar.api.L._

import com.thatdot.quine.routes.SampleQuery
import com.thatdot.quine.webapp.Styles

/** Blue bar at the top of the page which contains the logo, query input,
  * navigation buttons, and counters
  */
object TopBar {

  def apply(
    query: Signal[String],
    updateQuery: String => Unit,
    runningTextQuery: Signal[Boolean],
    queryBarColor: Signal[Option[String]],
    sampleQueries: Signal[Seq[SampleQuery]],
    foundNodesCount: Signal[Option[Int]],
    foundEdgesCount: Signal[Option[Int]],
    submitButton: Boolean => Unit,
    cancelButton: () => Unit,
    navButtons: HtmlElement,
    permissions: Option[Set[String]] = None,
  ): HtmlElement = {
    val canRead = permissions match {
      case Some(perms) => Set("GraphRead").subsetOf(perms)
      case None => true
    }

    div(
      cls := Styles.navBar,
      QueryInput(
        query = query,
        updateQuery = updateQuery,
        runningTextQuery = runningTextQuery,
        queryBarColor = queryBarColor,
        sampleQueries = sampleQueries,
        submitButton = submitButton,
        cancelButton = cancelButton,
        canRead = canRead,
      ),
      navButtons,
      child <-- foundNodesCount.combineWith(foundEdgesCount).map {
        case (None, None) => emptyNode
        case (n, e) => Counters.nodeEdgeCounters(n, e)
      },
    )
  }

}
