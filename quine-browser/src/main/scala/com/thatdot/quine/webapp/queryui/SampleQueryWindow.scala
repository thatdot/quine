package com.thatdot.quine.webapp.queryui

import org.scalajs.dom.{HTMLTextAreaElement, window}
import slinky.core.FunctionalComponent
import slinky.core.annotations.react
import slinky.core.facade.ReactRef
import slinky.web.SyntheticMouseEvent
import slinky.web.html._

import com.thatdot.quine.routes.SampleQuery
import com.thatdot.quine.webapp.Styles

@react object SampleQueryWindow {
  case class Props(
    updateQuery: String => Unit,
    textareaRef: ReactRef[HTMLTextAreaElement],
    sampleQueries: Seq[SampleQuery],
    query: String,
    areSampleQueriesVisible: Boolean,
    setAreSampleQueriesVisible: Boolean => Unit,
    textareaWidth: String,
    textareaHeight: String
  )

  val component: FunctionalComponent[SampleQueryWindow.Props] = FunctionalComponent[Props] { props =>
    val sampleQueriesCardClasses =
      s"position-fixed rounded border border-primary-subtle p-2 overflow-scroll ${Styles.sampleQueries}${if (props.areSampleQueriesVisible)
        s" ${Styles.focused}"
      else ""}"

    val handleSampleQueryClick: SampleQuery => SyntheticMouseEvent[button.tag.RefType] => Unit = { query => _ =>
      props.updateQuery(query.query)
      props.setAreSampleQueriesVisible(false)
    }

    div(
      className := sampleQueriesCardClasses
    )(
      div(className := "d-flex justify-content-between")(
        div(className := "mb-2 fs-3")("Sample Queries"),
        button(
          className := "btn-close",
          `type` := "button",
          onClick := { _ =>
            props.setAreSampleQueriesVisible(false)
          }
        )
      ),
      div(className := "list-group")(
        props.sampleQueries.map(query =>
          button(
            key := query.name,
            `type` := "button",
            className := "list-group-item text-start",
            onMouseDown := handleSampleQueryClick(query)
          )(
            div(strong(query.name)),
            div(className := "font-monospace text-body-secondary")(query.query)
          )
        ): _*
      )
    )
  }
}
