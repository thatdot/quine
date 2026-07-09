package com.thatdot.quine.webapp.queryui

import scala.scalajs.js

import com.raquo.laminar.api.L._
import org.scalajs.dom

import com.thatdot.quine.routes.SampleQuery
import com.thatdot.quine.webapp.Styles

final case class SampleQueryDraft(
  existingIndex: Option[Int],
  name: String,
  query: String,
)

object SampleQueryDraft {
  def forQuery(query: String, existing: Option[(SampleQuery, Int)]): SampleQueryDraft =
    existing.fold(SampleQueryDraft(None, "", query)) { case (sq, idx) =>
      SampleQueryDraft(Some(idx), sq.name, sq.query)
    }
}

/** The two Laminar elements that make up the sample-query bookmark feature: the query-bar
  * toggle button and its edit dialog. Always built and rendered as a pair.
  */
final case class BookmarkUi(button: HtmlElement, dialog: HtmlElement)

object SampleQueryBookmark {

  def queryBarToggle(isBookmarked: Signal[Boolean], onToggleClick: () => Unit): HtmlElement =
    htmlTag("i")(
      cls := s"ion-bookmark ${Styles.bookmarkToggle}",
      cls <-- isBookmarked.map(if (_) Styles.bookmarkToggleActive else ""),
      title <-- isBookmarked.map(b => if (b) "Edit sample query bookmark" else "Bookmark as sample query"),
      onClick --> (_ => onToggleClick()),
    )

  def editDialog(
    draftVar: Var[Option[SampleQueryDraft]],
    onSave: (Option[Int], SampleQuery) => Unit,
    onDelete: Int => Unit,
  ): HtmlElement = {
    def close(): Unit = draftVar.set(None)

    var dialogEl: Option[dom.html.Element] = None
    var justOpened: Boolean = false

    val handleMouseDown: js.Function1[dom.MouseEvent, Unit] = { (event: dom.MouseEvent) =>
      if (justOpened) {
        justOpened = false
      } else if (draftVar.now().isDefined) {
        val target = event.target.asInstanceOf[dom.Node]
        val clickedInside = dialogEl.exists(_.contains(target))
        if (!clickedInside) close()
      }
    }

    draftVar.signal.updates.foreach { v =>
      if (v.isDefined) justOpened = true
    }(unsafeWindowOwner)

    def renderDialog(initial: SampleQueryDraft): HtmlElement = {
      val nameVar = Var(initial.name)
      val isEdit = initial.existingIndex.isDefined

      def save(): Unit = {
        val sq = SampleQuery(name = nameVar.now().trim, query = initial.query)
        onSave(initial.existingIndex, sq)
        close()
      }

      div(
        cls := Styles.bookmarkDialog,
        onClick --> (_.stopPropagation()),
        div(cls := Styles.bookmarkDialogTitle, if (isEdit) "Edit sample query" else "Save as new sample query"),
        div(cls := Styles.bookmarkGlobalWarning, "This is a global setting. Changes apply to all users."),
        label(cls := Styles.bookmarkFieldLabel, "Name"),
        input(
          typ := "text",
          cls := Styles.bookmarkFieldInput,
          placeholder := "e.g. Recent orphan nodes",
          controlled(value <-- nameVar, onInput.mapToValue --> nameVar),
          onMountFocus,
        ),
        label(cls := Styles.bookmarkFieldLabel, "Query"),
        div(cls := Styles.bookmarkQueryPreview, initial.query),
        div(
          cls := Styles.bookmarkDialogFooter,
          initial.existingIndex.fold(emptyNode: Node) { idx =>
            button(
              cls := Styles.bookmarkRemoveButton,
              "Remove",
              onClick --> { _ =>
                onDelete(idx)
                close()
              },
            )
          },
          button(cls := Styles.bookmarkCancelButton, "Cancel", onClick --> (_ => close())),
          button(
            cls := Styles.bookmarkSaveButton,
            disabled <-- nameVar.signal.map(_.trim.isEmpty),
            if (isEdit) "Save changes" else "Save Query",
            onClick --> (_ => save()),
          ),
        ),
      )
    }

    div(
      onMountCallback(ctx => dialogEl = Some(ctx.thisNode.ref)),
      onMountCallback(_ => dom.document.addEventListener("mousedown", handleMouseDown)),
      onUnmountCallback(_ => dom.document.removeEventListener("mousedown", handleMouseDown)),
      child <-- draftVar.signal.map {
        case Some(draft) => renderDialog(draft)
        case None => emptyNode
      },
    )
  }
}
