package com.thatdot.quine.webapp.queryui

import com.raquo.laminar.api.L._

import com.thatdot.quine.webapp.components.streams.EmbeddedEditorConfig
import com.thatdot.quine.webapp.components.{Toast, ToastMessage, ToastVariant}
import com.thatdot.quine.webapp.dataservice.{
  DataService,
  GraphFeedService,
  SaveFailed,
  SaveResult,
  SaveSucceeded,
  WiretapService,
}
import com.thatdot.quine.webapp.resultspanel.tapmodal.TapModalStyles
import com.thatdot.quine.webapp.util.Pot
import com.thatdot.quine.webapp.v2api.V2ApiTypes.V2GraphFeed

/** A graph-feed editor on its own, as a dialog over the explorer: what the query menu's
  * "New graph feed" opens, so creating a feed from where the user is doesn't drag in the
  * whole Exploration UI Settings modal and its other catalogs. The settings modal keeps its own copy of the editor for browsing and editing
  * the saved list.
  *
  * Reuses the tap modal's shell classes (overlay, dialog, header, body) and the settings
  * modal's dismissal protocol for an open editor: the header × and the editor's own Cancel
  * close it, while Escape and an outside click are ignored so a stray one (including an
  * Escape aimed at Monaco's autocomplete widget, which propagates to `document`) doesn't
  * silently discard the edit.
  *
  * Saves go through the same whole-list `SaveGraphFeeds` as the settings modal, against the
  * latest fetched list, so the same guards apply: nothing is saved until that list has
  * loaded, and a name already in it is refused. On success the dialog closes and the host
  * hears about the feed through `onSaved`; on failure it stays open with the error shown
  * inside, so the edit isn't lost.
  *
  * A saved feed is off in every browser until its pill in the graph menu is switched on.
  * The "Start drawing on my graph" switch next to the save button, off by default, turns
  * the feed on in this browser once the save lands, so it starts drawing without a trip to
  * the pill. The switch can't act from the save callback: the wiretap runtime prunes any
  * enabled feed whose name is missing from its copy of the server list, and the refetch
  * that carries the new name hasn't landed yet at that point. The name is parked instead
  * and the enable dispatched from the feed-list signal once it includes the feed.
  */
object GraphFeedModal {

  private val DialogClass = "graph-feed-dialog"

  /** @param requests each event opens the dialog on that editor mode, replacing an editor
    *                 already open
    * @param dataService the graph-feed feed (existing names, the list a save replaces) and
    *                    the standing queries the editor's point picker offers
    * @param editorConfig threading for the embedded Cypher editor
    * @param onSaved called once the server has accepted the saved feed, after the dialog
    *                closes, with whether the feed is being switched on in this browser
    */
  def apply(
    requests: EventStream[GraphFeedEditorMode],
    dataService: DataService,
    editorConfig: EmbeddedEditorConfig,
    onSaved: (V2GraphFeed, Boolean) => Unit,
  ): HtmlElement = {
    val modeVar: Var[Option[GraphFeedEditorMode]] = Var(None)
    val savingVar: Var[Boolean] = Var(false)
    val toastVar: Var[Option[ToastMessage]] = Var(None)
    // The "Start drawing on my graph" switch, off on every open.
    val drawAfterSaveVar: Var[Boolean] = Var(false)
    // Feeds saved with that switch on, to be enabled once the refetched list carries them.
    val pendingEnablesVar: Var[Set[String]] = Var(Set.empty)

    // The feed list as last delivered, read at build and save time rather than sampled: the
    // list is refetched underneath an open editor (and would rebuild it if the editor were
    // keyed on the signal), and a one-shot sample of a signal nobody else subscribes to
    // reads back its initial Pending. `None` whenever the signal holds no value, which is
    // both before the first fetch completes and again after a namespace switch resets it to
    // Pending: a whole-list save built from a never-loaded list would delete every other
    // definition, and one built from the previous graph's list would write that graph's
    // feeds into the new one.
    var latestGraphFeeds: Option[Vector[V2GraphFeed]] = None

    def close(): Unit = {
      modeVar.set(None)
      savingVar.set(false)
      toastVar.set(None)
    }

    def notifyError(message: String): Unit =
      toastVar.set(Some(ToastMessage(message, ToastVariant.Error)))

    def save(feed: V2GraphFeed, priorName: Option[String]): Unit =
      latestGraphFeeds match {
        case None => notifyError("Graph feeds are still loading, try again in a moment")
        case Some(existing) if existing.exists(d => d.name == feed.name && !priorName.contains(d.name)) =>
          notifyError(s"""A feed named "${feed.name}" already exists""")
        case Some(existing) =>
          val updated = priorName
            .flatMap { name =>
              existing.indexWhere(_.name == name) match {
                case -1 => None
                case idx => Some(existing.updated(idx, feed))
              }
            }
            .getOrElse(existing :+ feed)
          savingVar.set(true)
          dataService.graphFeedDispatch.onNext(
            GraphFeedService.SaveGraphFeeds(
              updated,
              replyTo = Observer[SaveResult] {
                case SaveSucceeded =>
                  val drawNow = drawAfterSaveVar.now()
                  if (drawNow) pendingEnablesVar.update(_ + feed.name)
                  close()
                  onSaved(feed, drawNow)
                case SaveFailed(message) =>
                  savingVar.set(false)
                  notifyError(s"Save failed: $message")
              },
            ),
          )
      }

    def editor(mode: GraphFeedEditorMode): HtmlElement = {
      val priorName: Option[String] = mode match {
        case GraphFeedEditorMode.Editing(name) => Some(name)
        case GraphFeedEditorMode.Creating => None
      }
      val existing = latestGraphFeeds.getOrElse(Vector.empty)
      div(
        div(
          cls := "d-flex align-items-center mb-3 gap-2 small text-body-secondary",
          span("Graph:"),
          b(child.text <-- dataService.currentNamespaceSignal.map(_.namespaceId)),
        ),
        GraphFeedEditor(
          mode = mode,
          initialValue = priorName.flatMap(name => existing.find(_.name == name)),
          standingQueries = dataService.standingQueriesSignal.map(_.toOption.getOrElse(Seq.empty)),
          existingNames = existing.iterator.map(_.name).toSet,
          editorConfig = editorConfig,
          onSave = feed => save(feed, priorName),
          onDelete = None,
          onCancel = () => close(),
          saving = savingVar.signal,
          leadingAction = Some(drawAfterSaveSwitch()),
        ),
      )
    }

    def drawAfterSaveSwitch(): HtmlElement = {
      val idStr = "graph-feed-draw-after-save"
      div(
        cls := "form-check form-switch small mb-0",
        // The row's buttons are taller than the switch, so centre it on them rather than
        // leaving its label a few pixels above Cancel and Create.
        alignSelf := "center",
        input(
          cls := "form-check-input",
          typ := "checkbox",
          role := "switch",
          idAttr := idStr,
          checked <-- drawAfterSaveVar.signal,
          onChange.mapToChecked --> drawAfterSaveVar.writer,
        ),
        label(
          cls := "form-check-label",
          forId := idStr,
          title := "Switch the feed on in this browser as soon as it is saved, so its results start " +
          "drawing right away. Others keep it off until they switch it on themselves",
          "Start drawing on my graph",
        ),
      )
    }

    div(
      cls := TapModalStyles.overlay,
      display <-- modeVar.signal.map(mode => if (mode.isDefined) "flex" else "none"),
      dataService.graphFeedsSignal --> { pot => latestGraphFeeds = pot.toOption },
      // A parked enable belongs to the graph it was saved in: drop it on a switch, so a
      // same-named feed in another graph is never switched on by it. Fires with the
      // initial namespace on mount, when the set is empty anyway.
      dataService.currentNamespaceSignal.distinct --> (_ => pendingEnablesVar.set(Set.empty)),
      // Enable parked feeds once the list is Ready with them. This runs from an observer,
      // i.e. in the transaction after the list propagated, so the runtime's own view of
      // the list already includes the name and won't prune the opt-in. Driven by the
      // list's *updates*, not a combine with the parked set: at park time the signal still
      // holds the pre-save list (the save-triggered refetch is in flight, and its restart
      // emits no Pending), and a combine would fire on the park itself against that stale
      // list. The first Ready change after a park is therefore the refetch (a poll landing
      // with the unchanged old list is deduplicated upstream), so that list is
      // authoritative: a parked name it lacks was removed underneath the save and is
      // dropped rather than left waiting to switch on whatever next takes the name.
      dataService.graphFeedsSignal.updates.withCurrentValueOf(pendingEnablesVar.signal) --> {
        case (Pot.Ready(feeds), pending) if pending.nonEmpty =>
          pendingEnablesVar.set(Set.empty)
          feeds
            .filter(feed => pending(feed.name))
            .foreach(feed => dataService.wiretapDispatch.onNext(WiretapService.EnableGraphFeed(feed)))
        case _ => ()
      },
      requests --> { mode =>
        savingVar.set(false)
        toastVar.set(None)
        drawAfterSaveVar.set(false)
        modeVar.set(Some(mode))
      },
      div(
        cls := s"${TapModalStyles.dialog} $DialogClass",
        onClick.stopPropagation --> (_ => ()),
        div(
          cls := TapModalStyles.header,
          span(
            cls := TapModalStyles.title,
            child.text <-- modeVar.signal.map {
              case Some(_: GraphFeedEditorMode.Editing) => "Edit graph feed"
              case _ => "New graph feed"
            },
          ),
          button(
            tpe := "button",
            cls := TapModalStyles.closeButton,
            title := "Close",
            onClick --> (_ => close()),
            "×",
          ),
        ),
        div(
          cls := TapModalStyles.body,
          // Built per open and torn down on close, so each open starts from that request's
          // mode rather than the last attempt's edits.
          child <-- modeVar.signal.map {
            case Some(mode) => editor(mode)
            case None => emptyNode
          },
        ),
        Toast(toastVar),
      ),
    )
  }
}
