package com.thatdot.quine.webapp.queryui

import com.raquo.laminar.api.L._
import com.raquo.laminar.codecs.Codec

import com.thatdot.quine.routes.SampleQuery
import com.thatdot.quine.webapp.Styles

/** Blue bar at the top of the page which contains the logo, query input,
  * navigation buttons, and counters
  */
object TopBar {

  private val listAttr: HtmlAttr[String] = htmlAttr("list", Codec.stringAsIs)

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
      div(
        cls := Styles.queryInput,
        input(
          typ := "text",
          listAttr := "starting-queries",
          placeholder := (if (canRead) "Query returning nodes" else "Not Authorized to READ from graph"),
          cls := Styles.queryInputInput,
          cls <-- queryBarColor.map(_.getOrElse("")),
          styleAttr <-- runningTextQuery.map { running =>
            if (running) "animation: activequery 1.5s ease infinite" else ""
          },
          controlled(
            value <-- query,
            onInput.mapToValue --> (v => updateQuery(v)),
          ),
          onKeyUp --> (e => if (e.key == "Enter") submitButton(e.shiftKey)),
          disabled <-- runningTextQuery.map(_ || !canRead),
        ),
        htmlTag("datalist")(
          idAttr := "starting-queries",
          children <-- sampleQueries.map(_.map(q => option(value := q.query, q.name))),
        ),
        child <-- runningTextQuery.map { running =>
          button(
            cls := s"${Styles.grayClickable} ${Styles.queryInputButton}",
            onClick --> { e =>
              if (running) cancelButton()
              else submitButton(e.shiftKey)
            },
            title := (if (running) "Cancel query" else "Hold \"Shift\" to return results as a table"),
            disabled := !canRead,
            if (running) "Cancel" else "Query",
          )
        },
      ),
      navButtons,
      child <-- foundNodesCount.combineWith(foundEdgesCount).map { case (n, e) =>
        Counters.nodeEdgeCounters(n, e)
      },
    )
  }
}
