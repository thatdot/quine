package com.thatdot.quine.webapp.queryui

import org.scalajs.dom.window
import slinky.core._
import slinky.core.annotations.react
import slinky.core.facade.Hooks._
import slinky.web.html._
import slinky.web.{SyntheticFocusEvent, SyntheticKeyboardEvent, SyntheticMouseEvent}

import com.thatdot.quine.routes.SampleQuery
import com.thatdot.quine.webapp.Styles
import com.thatdot.quine.webapp.hooks.LocalStorageHook.useLocalStorage

@react object QueryTextareaInput {
  case class Props(
    runningTextQuery: Boolean,
    updateQuery: String => Unit,
    query: String,
    submitButton: (Boolean) => Unit,
    cancelButton: () => Unit,
    sampleQueries: Seq[SampleQuery]
  )

  val component: FunctionalComponent[QueryTextareaInput.Props] = FunctionalComponent[Props] { props =>
    val textareaRef = useRef[textarea.tag.RefType](null)
    val (areSampleQueriesVisible, setAreSampleQueriesVisible) = useState(false)
    val (textareaWidth, setTextareaWidth) = useLocalStorage("textareaWidth", "")
    val (textareaHeight, setTextareaHeight) = useLocalStorage("textareaHeight", "")

    val textareaOnFocus: SyntheticFocusEvent[textarea.tag.RefType] => Unit = { _ =>
      textareaRef.current.style.height = textareaHeight
      textareaRef.current.style.width = textareaWidth
      setAreSampleQueriesVisible(true)
    }

    val textareaOnBlur: SyntheticFocusEvent[textarea.tag.RefType] => Unit = { _ =>
      val height = textareaRef.current.style.height
      val width = textareaRef.current.style.width

      setTextareaHeight(
        if (height != "" && height.dropRight(2).toInt > 40) height else ""
      )
      setTextareaWidth(if (width != "" && width.dropRight(2).toInt < window.innerWidth * 0.95) width else "")

      textareaRef.current.style = ""
    }

    val handleTextareaOnKeyDown: SyntheticKeyboardEvent[textarea.tag.RefType] => Unit = { e =>
      if (e.key == "Enter" && !e.shiftKey) {
        e.preventDefault()
        props.submitButton(e.ctrlKey)
        textareaRef.current.blur()
        setAreSampleQueriesVisible(false)
      }
    }

    val handleButtonOnClick: SyntheticMouseEvent[button.tag.RefType] => Unit = { e =>
      if (props.runningTextQuery) props.cancelButton()
      else {
        props.submitButton(e.ctrlKey)
        setAreSampleQueriesVisible(false)
      }
    }

    val sampleQueriesCardClasses =
      s"position-fixed rounded border border-primary-subtle p-2 overflow-scroll ${Styles.sampleQueries}${if (areSampleQueriesVisible)
        s" ${Styles.focused}"
      else ""}"

    val handleSampleQueryClick: SampleQuery => SyntheticMouseEvent[button.tag.RefType] => Unit = { query => _ =>
      props.updateQuery(query.query)
      textareaRef.current.focus()
    }

    div(className := Styles.queryInput)(
      textarea(
        ref := textareaRef,
        placeholder := "Query returning nodes",
        className := Styles.queryTextareaInput,
        value := props.query,
        onChange := { e => props.updateQuery(e.target.value) },
        onKeyDown := handleTextareaOnKeyDown,
        onBlur := textareaOnBlur,
        onFocus := textareaOnFocus,
        disabled := props.runningTextQuery,
        rows := "1"
      ),
      button(
        className := s"${Styles.grayClickable} ${Styles.queryInputButton}",
        onClick := handleButtonOnClick
      )(if (props.runningTextQuery) "Cancel" else "Query"),
      div(
        className := sampleQueriesCardClasses
      )(
        div(className := "d-flex justify-content-between")(
          div(className := "mb-2 fs-3")("Sample Queries"),
          button(
            className := "btn-close",
            `type` := "button",
            onClick := { _ =>
              setAreSampleQueriesVisible(false)
            }
          )
        ),
        div(className := "list-group")(
          props.sampleQueries.map(query =>
            button(
              key := query.name,
              `type` := "button",
              className := s"list-group-item text-start${if (query.query == props.query) " active" else ""}",
              onClick := handleSampleQueryClick(query)
            )(
              div(strong(query.name)),
              div(className := "font-monospace text-body-secondary")(query.query)
            )
          ): _*
        )
      )
    )
  }
}
