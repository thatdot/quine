package com.thatdot.quine.webapp.queryui

import scala.scalajs.js

import org.scalajs.dom
import org.scalajs.dom.window
import slinky.core._
import slinky.core.annotations.react
import slinky.core.facade.Hooks._
import slinky.core.facade.ReactRef
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
    sampleQueries: Seq[SampleQuery],
    areSampleQueriesVisible: Boolean,
    setAreSampleQueriesVisible: Boolean => Unit,
    textareaRef: ReactRef[textarea.tag.RefType],
    textareaWidth: String,
    textareaHeight: String,
    setTextareaWidth: String => Unit,
    setTextareaHeight: String => Unit
  )

  val component: FunctionalComponent[QueryTextareaInput.Props] = FunctionalComponent[Props] { props =>
    val saveAndResetTextAreaSize = { () =>
      val computedStyle = window.getComputedStyle(props.textareaRef.current)

      val height = computedStyle.height
      val width = computedStyle.width

      props.setTextareaHeight(
        if (height != "" && height.dropRight(2).toFloat > 40) height else ""
      )
      props.setTextareaWidth(if (width != "" && width.dropRight(2).toFloat < window.innerWidth * 0.95) width else "")

      props.textareaRef.current.style = ""
    }

    useEffect(
      { () =>
        val handleClickOutside: js.Function1[dom.MouseEvent, Unit] = { event =>
          event.target match {
            case node: dom.Node =>
              if (!props.textareaRef.current.contains(node)) {
                val styleAttribute = props.textareaRef.current.getAttribute("style")
                val _ = if (styleAttribute != "") saveAndResetTextAreaSize()
              }
            case _ =>
            // do nothing, not a dom node
          }
        }
        window.addEventListener("click", handleClickOutside)

        () => window.removeEventListener("click", handleClickOutside)
      },
      Seq()
    )

    val textareaOnFocus: SyntheticFocusEvent[textarea.tag.RefType] => Unit = { _ =>
      props.textareaRef.current.style.height = props.textareaHeight
      props.textareaRef.current.style.width = props.textareaWidth
      props.setAreSampleQueriesVisible(props.query == "")
    }

    val handleTextareaOnKeyDown: SyntheticKeyboardEvent[textarea.tag.RefType] => Unit = { e =>
      if (e.key == "Enter" && !e.shiftKey) {
        e.preventDefault()
        props.submitButton(e.ctrlKey)
        props.textareaRef.current.blur()
        props.setAreSampleQueriesVisible(false)
      }
    }

    val handleTextareaOnBlur: SyntheticFocusEvent[textarea.tag.RefType] => Unit = { _ =>
      saveAndResetTextAreaSize()
      props.setAreSampleQueriesVisible(false)
    }

    val handleButtonOnClick: SyntheticMouseEvent[button.tag.RefType] => Unit = { e =>
      if (props.runningTextQuery) props.cancelButton()
      else {
        props.submitButton(e.ctrlKey)
      }
    }

    val handleTextareaChange: SyntheticEvent[textarea.tag.RefType, dom.Event] => Unit = { e =>
      val newQuery = e.target.value
      props.updateQuery(newQuery)
      props.setAreSampleQueriesVisible(newQuery == "")
    }

    div(className := Styles.queryInput)(
      textarea(
        ref := props.textareaRef,
        placeholder := "Query returning nodes",
        className := Styles.queryTextareaInput,
        value := props.query,
        onChange := handleTextareaChange,
        onKeyDown := handleTextareaOnKeyDown,
        onFocus := textareaOnFocus,
        onBlur := handleTextareaOnBlur,
        disabled := props.runningTextQuery,
        rows := "1"
      ),
      button(
        className := s"${Styles.grayClickable} ${Styles.queryInputButton}",
        onMouseDown := handleButtonOnClick
      )(if (props.runningTextQuery) "Cancel" else "Query")
    )
  }
}
