package com.thatdot.quine.webapp.queryui

import scala.concurrent.Future
import scala.scalajs.js

import com.raquo.laminar.api.L._
import io.circe.Json
import io.circe.Printer.{noSpaces, spaces2}
import org.scalajs.dom
import org.scalajs.dom.document
import org.scalajs.macrotaskexecutor.MacrotaskExecutor.Implicits._

import com.thatdot.quine.routes.CypherQueryResult
import com.thatdot.quine.webapp.Styles
import com.thatdot.quine.webapp.queryui.JsonCypherUtils.{jsonToCypher, stringToCypherIdent}

/** Structured data extracted from a `RETURN n` query result (the node's `id`,
  * `labels`, and `properties` fields).
  */
case class NodePopupData(
  idJson: Json,
  labels: Vector[String],
  props: Seq[(String, Json)],
)

/** Content of the popup: either structured (editable) or a fallback element. */
sealed trait NodePopupContent
object NodePopupContent {
  case class Structured(data: NodePopupData) extends NodePopupContent
  case class Fallback(element: HtmlElement) extends NodePopupContent
}

case class NodePopupState(
  x: Double,
  y: Double,
  content: NodePopupContent,
  iconCode: String = "",
  iconColor: String = "",
)

object NodePropertiesPopup {

  /** The "kind" (text, number, boolean, or JSON) of a single node property,
    * which affects how the property value is displayed and edited. For example,
    *   - Text values are displayed in green text.
    *   - Number and JSON input boxes turn red when their content is invalid.
    */
  sealed abstract private class PropKind(val name: String)
  private object PropKind {
    case object Txt extends PropKind("text")
    case object Num extends PropKind("number")
    case object Bool extends PropKind("boolean")
    case object Jsn extends PropKind("JSON")

    /** All property types, in type-selector order. */
    val all: List[PropKind] = List(Txt, Num, Bool, Jsn)

    def of(json: Json): PropKind =
      if (json.isBoolean) Bool
      else if (json.isNumber) Num
      else if (json.isString) Txt
      else Jsn

    def byName(s: String): PropKind = all.find(_.name == s).getOrElse(Txt)
  }

  /** Status of a property/label row, for the git-style diff coloring. */
  sealed private trait Status
  private object Status {
    case object Unchanged extends Status
    case object Added extends Status
    case object Altered extends Status
    case object Deleted extends Status
  }

  /** Live tally of changes shown in the footer. */
  private case class Counts(added: Int, altered: Int, deleted: Int) {
    def total: Int = added + altered + deleted
  }

  /** Generates strictly increasing integers (as strings). Its only purpose is to
    * hand out a fresh integer on each call, so edit rows can be given a stable,
    * unique identity.
    */
  private val nextId: () => String = {
    var rowIdCounter: Long = 0
    () => { rowIdCounter += 1; rowIdCounter.toString }
  }

  /** Text shown in an editor for a JSON value: strings unquoted, scalars bare, and
    * lists/objects pretty-printed as multiline JSON.
    */
  private def bufferOf(json: Json): String =
    json.asString.getOrElse(if (json.isNull) "" else spaces2.print(json))

  /** Turn an editor buffer back into a JSON value, given the chosen type. */
  private def coerce(buf: String, t: PropKind): Json = t match {
    case PropKind.Bool => Json.fromBoolean(buf == "true")
    case PropKind.Num =>
      io.circe.parser.parse(buf.trim).toOption.filter(_.isNumber).getOrElse(Json.fromInt(0))
    case PropKind.Txt => Json.fromString(buf)
    case PropKind.Jsn => io.circe.parser.parse(buf).getOrElse(Json.obj())
  }

  /** Whether a buffer parses as a JSON number (the grammar the endpoint round-trips). */
  private def isValidNumber(buf: String): Boolean =
    io.circe.parser.parse(buf.trim).toOption.exists(_.isNumber)

  /** Whether a buffer parses as any JSON value. */
  private def isValidJson(buf: String): Boolean =
    io.circe.parser.parse(buf).isRight

  /** Determines whether `json` should be displayed in "multiline mode" */
  private def shouldDisplayAsMultiline(json: Json): Boolean =
    json.asString match {
      case Some(str) => str.length > 52 || str.contains("\n")
      case None => (json.isObject || json.isArray) && noSpaces.print(json).length > 52
    }

  /* ------------------------------------------------------------------------
     Clipboard
     ------------------------------------------------------------------------ */
  private[queryui] def copyToClipboard(text: String, copied: Var[Boolean]): Unit = {
    val _ = dom.window.navigator.clipboard.writeText(text)
    copied.set(true)
    val _ = dom.window.setTimeout(() => copied.set(false), 1200)
  }

  private def icon(name: String): HtmlElement = htmlTag("i")(cls := name)

  /* ------------------------------------------------------------------------
     Cypher serialization (unchanged behavior)
     ------------------------------------------------------------------------ */

  private def whereClause(idJson: Json): String =
    if (idJson.isNumber) s"id(n) = ${noSpaces.print(idJson)}"
    else s"strId(n) = ${noSpaces.print(idJson)}"

  /** Shown when a popup query result isn't shaped like node properties. The popup only
    * displays a node's properties, so any other shape is reported rather than rendered.
    */
  private def unexpectedShape: NodePopupContent =
    NodePopupContent.Fallback(div("No node properties to display."))

  /** Parses a `CypherQueryResult` into structured popup content, or a fallback notice when it isn't node-shaped. */
  def parseContent(result: Either[Seq[Json], CypherQueryResult]): NodePopupContent =
    result match {
      case Right(CypherQueryResult(Seq("n"), resultRows)) =>
        resultRows.headOption
          .flatMap { row =>
            for {
              nodeObj <- row.headOption.flatMap(_.asObject)
              // Keep idJson's native JSON type so whereClause can pick id(n) vs
              // strId(n) — currently always a string, but may become a number.
              idJson <- nodeObj("id")
              propsObj <- nodeObj("properties").flatMap(_.asObject)
            } yield {
              val labels = nodeObj("labels").flatMap(_.asArray).getOrElse(Vector.empty).flatMap(_.asString)
              val props = propsObj.toMap.toSeq.sortBy(_._1)
              NodePopupContent.Structured(NodePopupData(idJson, labels, props))
            }
          }
          .getOrElse(unexpectedShape)

      case Right(_) => unexpectedShape

      case Left(_) => unexpectedShape
    }

  /* ========================================================================
     View-mode renderers
     ======================================================================== */

  /** The node icon displayed in the top left of the popup. */
  private[queryui] def nodeIcon(iconCode: String, iconColor: String): HtmlElement =
    if (iconCode.nonEmpty)
      span(cls := Styles.nodePropertiesNodeIcon, styleAttr := s"color: $iconColor", iconCode)
    else
      span(cls := Styles.nodePropertiesNodeDot)

  /** Masked node id — head only until hover; full id on hover; click copies. */
  private[queryui] def nodeIdValue(idStr: String): HtmlElement = {
    val hover = Var(false)
    val copied = Var(false)
    val masked = idStr.length > 10
    val head = if (masked) idStr.take(8) + "…" else idStr
    val open = hover.signal.combineWith(copied.signal).map { case (h, c) => h || c }
    span(
      cls := Styles.nodePropertiesId,
      cls("open") <-- open,
      cls("copied") <-- copied.signal,
      role := "button",
      tabIndex := 0,
      title <-- copied.signal.map(c => if (c) "Copied full id" else "Click to copy full id"),
      onClick --> { _ => copyToClipboard(idStr, copied) },
      onMouseEnter --> { _ => hover.set(true) },
      onMouseLeave --> { _ => hover.set(false) },
      // Collapsed, in-flow id: alone it sizes the box, so the popup never resizes.
      span(cls := "np-id-text", head),
      // Full id overlay: absolutely positioned (out of flow) so it can overflow
      // the popup's right edge without contributing to layout width.
      span(
        cls := "np-id-full",
        span(idStr),
      ),
    )
  }

  /** Inline, type-colored, copy-on-click value (short primitives). Mirrors the
    * masked node id: the collapsed chip (`.np-val-text`) stays in flow and sizes
    * the cell, while the full value (`.np-val-full`) is a viewport-fixed overlay
    * shown on hover/after-copy. Pinning it with `position: fixed` (coordinates
    * set in JS, below) lets it spill past the popup's right edge without being
    * clipped by the scrolling content area. Because those pinned coordinates are
    * frozen at hover/click time, a `dismiss` tick (emitted when the popup scrolls
    * or is dragged) resets the Vars so the overlay hides rather than stranding.
    */
  private def inlineValue(json: Json, dismiss: EventStream[Unit]): HtmlElement = {
    val t = PropKind.of(json)
    val copyText = json.asString.getOrElse(noSpaces.print(json))
    val emptyString = t == PropKind.Txt && json.asString.contains("")
    val shown = if (t == PropKind.Txt) json.asString.getOrElse("") else noSpaces.print(json)
    val hover = Var(false)
    val copied = Var(false)
    val open = hover.signal.combineWith(copied.signal).map { case (h, c) => h || c }

    // Pin the overlay's top-left onto the chip's border box. With identical
    // padding and font, the overlay's text then lands exactly over the chip's.
    var chipRef: Option[dom.html.Element] = None
    var fullRef: Option[dom.html.Element] = None
    def pinOverlay(): Unit = for {
      chip <- chipRef
      full <- fullRef
    } {
      val rect = chip.getBoundingClientRect()
      full.style.left = s"${rect.left}px"
      full.style.top = s"${rect.top}px"
    }

    def valText: HtmlElement =
      if (emptyString) span(cls := "np-empty-string", "empty string") else span(shown)

    span(
      cls := s"${Styles.nodePropertiesVal} ${Styles.nodePropertiesVal}--${t.name}",
      cls("open") <-- open,
      cls("copied") <-- copied.signal,
      role := "button",
      tabIndex := 0,
      title := s"Copy ${t.name} value",
      onMountCallback(ctx => chipRef = Some(ctx.thisNode.ref)),
      onMouseEnter --> { _ => pinOverlay(); hover.set(true) },
      onMouseLeave --> { _ => hover.set(false) },
      onClick --> { _ => pinOverlay(); copyToClipboard(copyText, copied) },
      dismiss --> { _ => if (hover.now() || copied.now()) { hover.set(false); copied.set(false) } },
      // Collapsed, in-flow value: alone it sizes the cell, so the popup never resizes.
      span(cls := "np-val-text", valText),
      // Full value overlay: fixed (out of flow), so it can overflow the popup.
      span(
        cls := "np-val-full",
        onMountCallback(ctx => fullRef = Some(ctx.thisNode.ref)),
        valText,
      ),
    )
  }

  /** Viewer for a "multiline" property value — a long string or complex
    * array/object (per [[shouldDisplayAsMultiline]]) that won't fit the inline chip ([[inlineValue]]).
    * Rendered in a wrapped box that clamps to a few lines, copies on click, and
    * expands via "Show all".
    */
  private def multilineValue(json: Json): HtmlElement = {
    val raw = json.asString.getOrElse(spaces2.print(json))
    val copied = Var(false)
    val expanded = Var(false)
    val lineCount = raw.count(_ == '\n') + 1
    val clampable = raw.length > 220 || lineCount > 6
    div(
      cls := "node-properties-multiline-wrap",
      div(
        cls := Styles.nodePropertiesMultiline,
        cls("clamped") <-- expanded.signal.map(e => clampable && !e),
        cls("copied") <-- copied.signal,
        role := "button",
        tabIndex := 0,
        title := "Click to copy " ++ (if (json.isString) "text" else "JSON") ++ " value",
        onClick --> { _ => copyToClipboard(raw, copied) },
        raw,
      ),
      if (clampable || json.isString)
        div(
          cls := Styles.nodePropertiesMultilineFoot,
          if (clampable)
            button(
              tpe := "button",
              cls := Styles.nodePropertiesShowAllBtn,
              child.text <-- expanded.signal.map(e => if (e) "Show less" else "Show all"),
              onClick.stopPropagation --> { _ => expanded.update(!_) },
            )
          else span(),
          if (json.isString)
            span(
              cls := Styles.nodePropertiesCharcount,
              child.text <-- copied.signal.map(c => if (c) "copied" else s"${raw.length} chars"),
            )
          else emptyNode,
        )
      else emptyNode,
    )
  }

  /** Copyable property key. */
  private def propKey(key: String): HtmlElement = {
    val copied = Var(false)
    span(
      cls := Styles.nodePropertiesKey,
      cls("copied") <-- copied.signal,
      role := "button",
      tabIndex := 0,
      title := "Copy key",
      onClick --> { _ => copyToClipboard(key, copied) },
      span(cls := "np-key-text", key),
    )
  }

  private def viewProps(data: NodePopupData, dismiss: EventStream[Unit]): HtmlElement =
    if (data.props.isEmpty) p(cls := Styles.nodePropertiesEmpty, "no properties")
    else
      div(
        cls := Styles.nodePropertiesList,
        data.props.map { case (k, v) =>
          if (shouldDisplayAsMultiline(v))
            div(
              cls := s"${Styles.nodePropertiesRow} ${Styles.nodePropertiesRow}--multiline",
              propKey(k),
              multilineValue(v),
            )
          else
            div(
              cls := Styles.nodePropertiesRow,
              propKey(k),
              div(cls := Styles.nodePropertiesValCell, inlineValue(v, dismiss)),
            )
        },
      )

  private def viewLabels(data: NodePopupData): HtmlElement =
    div(
      cls := Styles.nodePropertiesLabels,
      if (data.labels.isEmpty) span(cls := Styles.nodePropertiesEmpty, "no labels")
      else data.labels.map(l => span(cls := Styles.nodePropertiesLabel, s":$l")),
    )

  /* ========================================================================
     Edit model — per-row reactive state with git-style diff status
     ======================================================================== */

  final private class PropRow(
    val id: String,
    val origKey: Option[String],
    val origJson: Option[Json],
    val added: Boolean,
    val keyV: Var[String],
    val kindV: Var[PropKind],
    val bufV: Var[String],
    val deletedV: Var[Boolean],
  ) {
    def origKind: Option[PropKind] = origJson.map(PropKind.of)
    def currentJson: Json = coerce(bufV.now(), kindV.now())

    private def altered(k: String, t: PropKind, b: String): Boolean =
      !origKey.contains(k) || !origKind.contains(t) || coerce(b, t) != origJson.getOrElse(Json.Null)

    private def statusFrom(k: String, t: PropKind, b: String, del: Boolean): Status =
      if (del && !added) Status.Deleted
      else if (added) if (k.trim.nonEmpty) Status.Added else Status.Unchanged
      else if (altered(k, t, b)) Status.Altered
      else Status.Unchanged

    def status: Status = statusFrom(keyV.now(), kindV.now(), bufV.now(), deletedV.now())

    val statusSignal: Signal[Status] =
      keyV.signal.combineWith(kindV.signal, bufV.signal, deletedV.signal).map { case (k, t, b, del) =>
        statusFrom(k, t, b, del)
      }

    /** A row is invalid only when live (not deleted, key present) and its typed buffer won't parse. */
    val validSignal: Signal[Boolean] =
      keyV.signal.combineWith(kindV.signal, bufV.signal, deletedV.signal).map { case (k, t, b, del) =>
        del || k.trim.isEmpty || (t match {
          case PropKind.Num => isValidNumber(b)
          case PropKind.Jsn => isValidJson(b)
          case _ => true
        })
      }
  }

  final private class LabelRow(
    val id: String,
    val name: String,
    val added: Boolean,
    val deletedV: Var[Boolean],
  ) {
    def status: Status =
      if (deletedV.now() && !added) Status.Deleted
      else if (added && !deletedV.now()) Status.Added
      else Status.Unchanged

    val statusSignal: Signal[Status] = deletedV.signal.map { d =>
      if (d && !added) Status.Deleted
      else if (added && !d) Status.Added
      else Status.Unchanged
    }
  }

  private def newPropRow(key: String, json: Json): PropRow =
    new PropRow(
      id = s"$key:${nextId()}",
      origKey = Some(key),
      origJson = Some(json),
      added = false,
      keyV = Var(key),
      kindV = Var(PropKind.of(json)),
      bufV = Var(bufferOf(json)),
      deletedV = Var(false),
    )

  private def blankPropRow(): PropRow =
    new PropRow(s"new:${nextId()}", None, None, added = true, Var(""), Var(PropKind.Txt), Var(""), Var(false))

  final private class EditModel(data: NodePopupData) {
    val propsVar: Var[List[PropRow]] = Var(data.props.map { case (k, v) => newPropRow(k, v) }.toList)
    val labelsVar: Var[List[LabelRow]] =
      Var(data.labels.map(l => new LabelRow(s"L:$l", l, added = false, Var(false))).toList)
    val newLabel: Var[String] = Var("")

    /* property mutators */
    def setKind(r: PropRow, t: PropKind): Unit = r.kindV.set(t)
    def setBuf(r: PropRow, v: String): Unit = r.bufV.set(v)
    def removeProp(r: PropRow): Unit =
      if (r.added) propsVar.update(_.filterNot(_.id == r.id)) else r.deletedV.set(true)
    def restoreProp(r: PropRow): Unit = r.deletedV.set(false)

    /** Reset an edited row back to its original key/type/value. */
    def revertProp(r: PropRow): Unit = {
      r.origKey.foreach(r.keyV.set)
      r.origKind.foreach(r.kindV.set)
      r.bufV.set(r.origJson.map(bufferOf).getOrElse(""))
      r.deletedV.set(false)
    }
    def addProp(): Unit = propsVar.update(_ :+ blankPropRow())

    /* label mutators */
    def removeLabel(l: LabelRow): Unit =
      if (l.added) labelsVar.update(_.filterNot(_.id == l.id)) else l.deletedV.set(true)
    def restoreLabel(l: LabelRow): Unit = l.deletedV.set(false)
    def addLabel(): Unit = {
      val v = newLabel.now().trim
      // block re-adding any label the node already has, including one currently marked for
      // deletion — otherwise restoring that one would leave a duplicate
      if (v.nonEmpty && !labelsVar.now().exists(_.name == v))
        labelsVar.update(_ :+ new LabelRow(s"L:$v:${nextId()}", v, added = true, Var(false)))
      newLabel.set("")
    }

    private def statuses: Signal[List[Status]] =
      propsVar.signal.combineWith(labelsVar.signal).flatMapSwitch { case (ps, ls) =>
        val sigs = ps.map(_.statusSignal) ++ ls.map(_.statusSignal)
        if (sigs.isEmpty) Signal.fromValue(List.empty[Status]) else Signal.combineSeq(sigs).map(_.toList)
      }

    private def countsOf(sts: List[Status]): Counts =
      Counts(sts.count(_ == Status.Added), sts.count(_ == Status.Altered), sts.count(_ == Status.Deleted))

    val counts: Signal[Counts] = statuses.map(countsOf)

    /** Synchronous snapshot of the change counts (for the confirm dialog). */
    def countsNow: Counts = countsOf(propsVar.now().map(_.status) ++ labelsVar.now().map(_.status))

    /** False when any property row has invalid input (e.g. a non-number in a number field). */
    val allValid: Signal[Boolean] =
      propsVar.signal.flatMapSwitch { ps =>
        if (ps.isEmpty) Signal.fromValue(true)
        else Signal.combineSeq(ps.map(_.validSignal)).map(_.forall(identity))
      }

    /** The constructed query shown in the footer preview, recomputed whenever any prop row or
      * label changes. Falls back to [[baseQuery]] (the bare `MATCH ... RETURN n`) when there are
      * no changes to apply, so the preview always shows a runnable query.
      */
    val querySignal: Signal[String] =
      propsVar.signal.combineWith(labelsVar.signal).flatMapSwitch { case (ps, ls) =>
        val rowSigs: List[Signal[Unit]] =
          ps.map(r => r.keyV.signal.combineWith(r.kindV.signal, r.bufV.signal, r.deletedV.signal).map(_ => ())) ++
          ls.map(_.deletedV.signal.map(_ => ()))
        if (rowSigs.isEmpty) Signal.fromValue(buildQuery().getOrElse(baseQuery))
        else Signal.combineSeq(rowSigs).map(_ => buildQuery().getOrElse(baseQuery))
      }

    /* ----------------------------------------------------------------------
       Property-name collisions
       ----------------------------------------------------------------------
       A row is `live` when it contributes a property to the final node:
         live(r)  ⟺  ¬deleted(r) ∧ key(r) ≠ ""
       (deletions are applied first, so a deleted row simply isn't live.)

       A row is `moved` when it claims a name it didn't originally own:
         moved(r) ⟺  added(r) ∨ origKey(r) ≠ key(r)
       A live, non-moved row is the *incumbent* of its key; since original
       keys are distinct, each key has at most one incumbent.

       The alteration set is VALID iff all live rows have pairwise-distinct
       keys. A row is FLAGGED (red) iff it both collides and is at fault:
         collides(r) ⟺ ∃ r' ∈ live, r' ≠ r, key(r') = key(r)
         flag(r)     ⟺ collides(r) ∧ moved(r)
       The incumbent is spared; if a colliding group has no incumbent, all of
       its rows are moved and so all are flagged. Thus "some row is flagged"
       ⟺ "some collision exists" ⟺ invalid. */
    private val flaggedKeyRowIds: Signal[Set[String]] =
      propsVar.signal.flatMapSwitch { ps =>
        if (ps.isEmpty) Signal.fromValue(Set.empty[String])
        else
          Signal
            .combineSeq(ps.map { r =>
              r.keyV.signal.combineWith(r.deletedV.signal).map { case (k, del) =>
                val key = k.trim
                (r.id, key, !del && key.nonEmpty, r.added || !r.origKey.contains(key))
              }
            })
            .map { rows =>
              val liveKeyCounts =
                rows.collect { case (_, key, true, _) => key }.groupBy(identity).map { case (k, vs) => k -> vs.size }
              rows.collect {
                case (id, key, true, moved) if moved && liveKeyCounts.getOrElse(key, 0) > 1 => id
              }.toSet
            }
      }

    /** Whether a given row's key collides with another live row (and is at fault). */
    def keyCollidesSignal(r: PropRow): Signal[Boolean] = flaggedKeyRowIds.map(_.contains(r.id))

    /** True when no live rows share a key — a precondition for applying. */
    val noKeyCollisions: Signal[Boolean] = flaggedKeyRowIds.map(_.isEmpty)

    /** The base read query with no mutation clauses — the `MATCH ... RETURN n`
      * skeleton shared with [[buildQuery]], which a mutating query reduces to
      * once its SET/REMOVE clauses are dropped.
      */
    def baseQuery: String = s"MATCH (n) WHERE ${whereClause(data.idJson)} RETURN n"

    /** Build the Cypher mutation, or None when nothing changed. */
    def buildQuery(): Option[String] = {
      val props = propsVar.now()
      val removeKeys = props.flatMap { r =>
        if (r.deletedV.now()) r.origKey
        else if (!r.added && r.origKey.exists(_ != r.keyV.now().trim)) r.origKey // renamed: drop old key
        else None
      }.distinct
      val setProps = props.flatMap { r =>
        val k = r.keyV.now().trim
        if (r.deletedV.now() || k.isEmpty) None
        else {
          val changed = r.added || !r.origKey.contains(k) || !r.origKind.contains(r.kindV.now()) ||
            r.currentJson != r.origJson.getOrElse(Json.Null)
          if (changed) Some(s"n.${stringToCypherIdent(k)} = ${jsonToCypher(r.currentJson)}") else None
        }
      }
      val origLabels = data.labels.toSet
      val curLabels = labelsVar.now().filterNot(_.deletedV.now()).map(_.name).toSet
      val labelsToRemove = (origLabels -- curLabels).toSeq
      val labelsToAdd = (curLabels -- origLabels).toSeq

      val clauses = List(
        if (removeKeys.nonEmpty) Some(s"REMOVE ${removeKeys.map(k => s"n.${stringToCypherIdent(k)}").mkString(", ")}")
        else None,
        if (labelsToRemove.nonEmpty) Some(s"REMOVE n:${labelsToRemove.map(stringToCypherIdent).mkString(":")}")
        else None,
        if (labelsToAdd.nonEmpty) Some(s"SET n:${labelsToAdd.map(stringToCypherIdent).mkString(":")}")
        else None,
        if (setProps.nonEmpty) Some(s"SET ${setProps.mkString(", ")}") else None,
      ).flatten
      if (clauses.isEmpty) None
      else Some(s"MATCH (n) WHERE ${whereClause(data.idJson)} ${clauses.mkString(" ")} RETURN n")
    }
  }

  /* ------------------------------------------------------------------------
     Edit-mode renderers
     ------------------------------------------------------------------------ */

  private def statusModifier(s: Status): String = s match {
    case Status.Added => "added"
    case Status.Altered => "altered"
    case Status.Deleted => "deleted"
    case Status.Unchanged => "unchanged"
  }
  private def statusLabel(s: Status): String = s match {
    case Status.Added => "added"
    case Status.Altered => "edited"
    case Status.Deleted => "removed"
    case Status.Unchanged => ""
  }

  /** Auto-growing mono textarea for editing string values. */
  private def autoTextarea(
    buf: Var[String],
    invalid: Signal[Boolean] = Signal.fromValue(false),
    invalidTitle: String = "",
  ): HtmlElement = {
    def resize(el: dom.html.TextArea): Unit = {
      el.style.height = "auto"
      el.style.height = s"${math.min(el.scrollHeight.toDouble, 200.0)}px"
    }
    textArea(
      cls := Styles.nodePropertiesEditInput,
      cls("np-input-invalid") <-- invalid,
      title <-- invalid.map(v => if (v) invalidTitle else ""),
      rows := 1,
      controlled(value <-- buf.signal, onInput.mapToValue --> buf.writer),
      onInput --> { e => resize(e.target.asInstanceOf[dom.html.TextArea]) },
      onMountCallback { ctx =>
        val _ = dom.window.setTimeout(() => resize(ctx.thisNode.ref), 0)
      },
    )
  }

  private def valueEditor(model: EditModel, r: PropRow): HtmlElement =
    div(
      cls := Styles.nodePropertiesProwVal,
      select(
        cls := Styles.nodePropertiesTypeSelect,
        value <-- r.kindV.signal.map(_.name),
        onChange.mapToValue --> { v => model.setKind(r, PropKind.byName(v)) },
        PropKind.all.map(t => option(value := t.name, t.name)),
      ),
      child <-- r.kindV.signal.map {
        case PropKind.Bool =>
          select(
            cls := Styles.nodePropertiesBoolSelect,
            value <-- r.bufV.signal.map(b => if (b == "true") "true" else "false"),
            onChange.mapToValue --> { v => model.setBuf(r, v) },
            option(value := "true", "true"),
            option(value := "false", "false"),
          )
        case PropKind.Num =>
          input(
            cls := Styles.nodePropertiesTextInput,
            cls("np-input-invalid") <-- r.validSignal.map(!_),
            title <-- r.validSignal.map(v => if (v) "" else "Not a valid number"),
            tpe := "text",
            placeholder := "0",
            controlled(value <-- r.bufV.signal, onInput.mapToValue --> r.bufV.writer),
          )
        case PropKind.Jsn => autoTextarea(r.bufV, r.validSignal.map(!_), "Not valid JSON")
        case PropKind.Txt => autoTextarea(r.bufV)
      },
    )

  private def rowActionBtn(danger: Boolean, label: String, iconName: String, action: () => Unit): HtmlElement =
    button(
      tpe := "button",
      cls := (if (danger) s"${Styles.nodePropertiesIconBtn} ${Styles.nodePropertiesIconBtn}--danger"
              else Styles.nodePropertiesIconBtn),
      aria.label := s"$label property",
      title := label,
      icon(iconName),
      onClick.stopPropagation --> { _ => action() },
    )

  private def propRowEdit(model: EditModel, r: PropRow): HtmlElement =
    div(
      cls := Styles.nodePropertiesProw,
      cls <-- r.statusSignal.map(s => s"${Styles.nodePropertiesProw}--${statusModifier(s)}"),
      // key line
      div(
        cls := Styles.nodePropertiesProwKey,
        child <-- r.deletedV.signal.map { deleted =>
          if (deleted)
            span(cls := "np-deleted-key", if (r.origKey.getOrElse("").nonEmpty) r.origKey.get else "—")
          else
            input(
              cls := Styles.nodePropertiesTextInput,
              cls("np-input-invalid") <-- model.keyCollidesSignal(r),
              title <-- model.keyCollidesSignal(r).map(c => if (c) "Duplicate property name" else ""),
              tpe := "text",
              placeholder := "key",
              controlled(value <-- r.keyV.signal, onInput.mapToValue --> r.keyV.writer),
            )
        },
        span(
          cls := Styles.nodePropertiesStatusTag,
          cls <-- r.statusSignal.map(s => s"${Styles.nodePropertiesStatusTag}--${statusModifier(s)}"),
          child.text <-- r.statusSignal.map(statusLabel),
        ),
        child <-- r.statusSignal.map {
          case Status.Deleted => rowActionBtn(danger = false, "Restore", "ion-ios-undo", () => model.restoreProp(r))
          case Status.Altered => rowActionBtn(danger = false, "Revert", "ion-ios-undo", () => model.revertProp(r))
          case _ => rowActionBtn(danger = true, "Remove", "ion-ios-trash-outline", () => model.removeProp(r))
        },
      ),
      // value line
      child <-- r.deletedV.signal.map { deleted =>
        if (deleted)
          r.origJson match {
            case Some(j) if j.isString && j.asString.contains("") =>
              div(cls := "np-deleted-val", span(cls := "np-empty-string", "empty string"))
            case Some(j) if j.isString => div(cls := "np-deleted-val", j.asString.getOrElse[String](""))
            case Some(j) => div(cls := "np-deleted-val", noSpaces.print(j))
            case None => div(cls := "np-deleted-val")
          }
        else valueEditor(model, r)
      },
    )

  private def editProps(model: EditModel): HtmlElement =
    div(
      cls := Styles.nodePropertiesEditList,
      // split keeps each row's element stable across add/remove (so inputs keep focus);
      // the PropRow instance is stable per id and owns its own Vars.
      children <-- model.propsVar.signal.splitSeq(_.id)(rowSig => propRowEdit(model, rowSig.now())),
      button(
        tpe := "button",
        cls := Styles.nodePropertiesAddBtn,
        icon("ion-ios-plus-empty"),
        span("Add property"),
        onClick.stopPropagation --> { _ => model.addProp() },
      ),
    )

  private def editLabelChip(model: EditModel, l: LabelRow): HtmlElement =
    span(
      cls := Styles.nodePropertiesLabel,
      cls(s"${Styles.nodePropertiesLabel}--added") <-- l.deletedV.signal.map(d => l.added && !d),
      cls(s"${Styles.nodePropertiesLabel}--deleted") <-- l.deletedV.signal,
      s":${l.name}",
      child <-- l.deletedV.signal.map { deleted =>
        if (deleted)
          htmlTag("i")(
            cls := s"ion-ios-undo ${Styles.nodePropertiesLabelRemoveBtn}",
            title := "Restore",
            onClick.stopPropagation --> { _ => model.restoreLabel(l) },
          )
        else
          htmlTag("i")(
            cls := s"ion-ios-close-empty ${Styles.nodePropertiesLabelRemoveBtn}",
            title := "Remove",
            onClick.stopPropagation --> { _ => model.removeLabel(l) },
          )
      },
    )

  private def editLabels(model: EditModel): HtmlElement =
    div(
      cls := Styles.nodePropertiesLabels,
      children <-- model.labelsVar.signal.splitSeq(_.id)(labelSig => editLabelChip(model, labelSig.now())),
      // the entry flows as the trailing pill, so its spacing matches the gap
      // between the real label chips
      newLabelPill(model),
    )

  private def newLabelPill(model: EditModel): HtmlElement =
    label(
      cls := "np-new-label-pill",
      span(cls := "np-new-label-colon", ":"),
      // the input is an overlay sized by an invisible mirror span holding the
      // same text, so the pill hugs the glyphs pixel-tight as it grows
      span(
        cls := "np-new-label-sizer-wrap",
        span(
          cls := "np-new-label-sizer",
          child.text <-- model.newLabel.signal.map(v => if (v.isEmpty) "new label" else v),
        ),
        input(
          cls := Styles.nodePropertiesNewLabelInput,
          placeholder := "new label",
          controlled(value <-- model.newLabel.signal, onInput.mapToValue --> model.newLabel.writer),
          onKeyDown.filter(_.key == "Enter") --> { _ => model.addLabel() },
        ),
      ),
    )

  /* ------------------------------------------------------------------------
     Footer: diff summary + guarded apply
     ------------------------------------------------------------------------ */

  private def diffSummary(counts: Counts): HtmlElement =
    if (counts.total == 0)
      span(cls := s"${Styles.nodePropertiesDiffSummary} np-no-changes", "No changes yet")
    else {
      def item(n: Int, modifier: String, sign: String): HtmlElement =
        span(
          cls := s"${Styles.nodePropertiesDiffItem} ${Styles.nodePropertiesDiffItem}--$modifier",
          cls("zero") := (n == 0),
          span(cls := Styles.nodePropertiesDiffDot),
          s"$sign$n",
        )
      span(
        cls := Styles.nodePropertiesDiffSummary,
        item(counts.added, "added", "+"),
        item(counts.altered, "altered", "~"),
        item(counts.deleted, "deleted", "−"),
      )
    }

  private def diffSummaryText(counts: Counts): String = {
    def pluralize(count: Int, noun: String): String =
      s"$count $noun${if (count == 1) "" else "s"}"
    val parts = List(
      if (counts.deleted > 0) Some(pluralize(counts.deleted, "deletion")) else None,
      if (counts.altered > 0) Some(pluralize(counts.altered, "edit")) else None,
      if (counts.added > 0) Some(pluralize(counts.added, "addition")) else None,
    ).flatten
    val summary = if (parts.isEmpty) "0 changes" else parts.mkString(" · ")
    s"« $summary »"
  }

  /** Modes the popup toggles between. */
  sealed private trait Mode
  private object Mode {
    case object View extends Mode
    case object Edit extends Mode
  }

  private def footer(
    model: EditModel,
    submitQuery: String => Future[Unit],
    onRefreshProperties: () => Unit,
  ): HtmlElement = {
    val expanded = Var(false)
    val copied = Var(false)
    div(
      cls := Styles.nodePropertiesFooter,
      // Show Cypher query (left) · diff summary + Apply changes (grouped right)
      div(
        cls := Styles.nodePropertiesFooterRow,
        button(
          tpe := "button",
          cls := Styles.nodePropertiesShowAllBtn,
          child.text <-- expanded.signal.map(e => if (e) "▾ Cypher query" else "▸ Cypher query"),
          onClick.stopPropagation --> { _ => expanded.update(!_) },
        ),
        child <-- model.counts.map(diffSummary),
        button(
          tpe := "button",
          cls := s"${Styles.nodePropertiesBtn} ${Styles.nodePropertiesBtn}--primary",
          icon("ion-ios-checkmark"),
          span("Apply changes"),
          disabled <-- model.counts.combineWith(model.allValid, model.noKeyCollisions).map {
            case (c, valid, noCollisions) => c.total == 0 || !valid || !noCollisions
          },
          onClick.stopPropagation --> { _ =>
            val confirmed =
              dom.window.confirm(s"Apply ${diffSummaryText(model.countsNow)} to this node? This writes to the graph.")
            if (confirmed) {
              model.buildQuery().foreach { q =>
                submitQuery(q).foreach(_ => onRefreshProperties())
              }
            }
          },
        ),
      ),
      // read-only constructed query, revealed below the row; click to copy
      child <-- expanded.signal.map { e =>
        if (!e) emptyNode
        else
          div(
            cls := Styles.nodePropertiesCypher,
            div(
              cls := Styles.nodePropertiesCypherBox,
              cls("copied") <-- copied.signal,
              role := "button",
              tabIndex := 0,
              title := "Click to copy query",
              onClick.stopPropagation --> { _ =>
                copyToClipboard(model.buildQuery().getOrElse(model.baseQuery), copied)
              },
              child.text <-- model.querySignal,
            ),
          )
      },
    )
  }

  /* ========================================================================
     Assembly
     ======================================================================== */

  def apply(
    state: NodePopupState,
    closePopup: () => Unit,
    submitQuery: String => Future[Unit],
    onBack: (Double, Double) => Unit,
    onRefreshProperties: (Double, Double) => Unit,
    canWrite: Boolean,
  ): HtmlElement = {
    var popupEl: Option[dom.html.Div] = None
    // Fires when the popup scrolls or is dragged; inline value overlays subscribe
    // to it and hide themselves, since their `position: fixed` coords are pinned
    // at hover/click time and don't follow the moving popup.
    val dismissOverlays = new EventBus[Unit]
    // Anchor position for the popup. Starts at the click location, but is updated as
    // the user drags the popup by its header so that repositioning (eg. when toggling
    // edit mode) keeps the dragged location instead of snapping back to the click.
    var posX = state.x
    var posY = state.y

    def adjustPosition(): Unit = for (el <- popupEl) {
      val pad = 8.0
      val vw = dom.window.innerWidth
      val vh = dom.window.innerHeight
      val parentRect = Option(el.offsetParent).map(_.getBoundingClientRect())
      val parentLeft = parentRect.fold(0.0)(_.left)
      val parentTop = parentRect.fold(0.0)(_.top)
      val sidebarRight = Option(dom.document.querySelector(".sidebar"))
        .map(_.getBoundingClientRect().right)
        .filter(_ > 0)
        .getOrElse(pad)
      val minLeft = sidebarRight - parentLeft

      el.style.left = s"${posX}px"
      el.style.top = s"${posY}px"
      val rect = el.getBoundingClientRect()
      val rightOverflow = rect.right - vw + pad
      val bottomOverflow = rect.bottom - vh + pad
      if (rightOverflow > 0) el.style.left = s"${math.max(minLeft, posX - rightOverflow)}px"
      if (rect.left < sidebarRight) el.style.left = s"${minLeft}px"
      if (bottomOverflow > 0) el.style.top = s"${math.max(pad - parentTop, posY - bottomOverflow)}px"
    }

    // Drag the popup by its header, mirroring the graph context menu. Mousedowns that
    // land on header controls (buttons, label inputs) are left alone so they keep working.
    def startHeaderDrag(e: dom.MouseEvent): Unit = {
      val target = e.target.asInstanceOf[dom.Element]
      val onControl = target.closest("button, input, select, textarea") != null
      if (!onControl) for (el <- popupEl) {
        e.preventDefault()
        val startX = e.clientX
        val startY = e.clientY
        val startLeft = el.offsetLeft
        val startTop = el.offsetTop
        val onMove: js.Function1[dom.MouseEvent, Unit] = { (me: dom.MouseEvent) =>
          val pad = 8.0
          val vw = dom.window.innerWidth
          val vh = dom.window.innerHeight
          val parentRect = Option(el.offsetParent).map(_.getBoundingClientRect())
          val parentLeft = parentRect.fold(0.0)(_.left)
          val parentTop = parentRect.fold(0.0)(_.top)
          val sidebarRight = Option(dom.document.querySelector(".sidebar"))
            .map(_.getBoundingClientRect().right)
            .filter(_ > 0)
            .getOrElse(pad)
          val minLeft = sidebarRight - parentLeft
          val rect = el.getBoundingClientRect()

          val rawX = startLeft + me.clientX - startX
          val rawY = startTop + me.clientY - startY
          posX = math.max(minLeft, math.min(rawX, vw - rect.width - pad - parentLeft))
          posY = math.max(pad - parentTop, math.min(rawY, vh - rect.height - pad - parentTop))
          el.style.left = s"${posX}px"
          el.style.top = s"${posY}px"
          dismissOverlays.emit(())
        }
        lazy val onUp: js.Function1[dom.MouseEvent, Unit] = { (_: dom.MouseEvent) =>
          dom.document.removeEventListener("mousemove", onMove)
          dom.document.removeEventListener("mouseup", onUp)
        }
        dom.document.addEventListener("mousemove", onMove)
        dom.document.addEventListener("mouseup", onUp)
      }
    }

    val onDocumentClick: js.Function1[dom.MouseEvent, Unit] = (e: dom.MouseEvent) =>
      for (el <- popupEl)
        if (!el.contains(e.target.asInstanceOf[dom.Node]))
          closePopup()

    // Scroll events don't bubble, so listen in the capture phase to catch the
    // content area scrolling and dismiss any open value overlays.
    val onScroll: js.Function1[dom.Event, Unit] = (_: dom.Event) => dismissOverlays.emit(())

    val bodyModifiers: List[Modifier[HtmlElement]] = state.content match {
      case NodePopupContent.Structured(data) =>
        val modeVar = Var[Mode](Mode.View)
        val model = new EditModel(data)
        val idStr = data.idJson.asString.getOrElse(noSpaces.print(data.idJson))
        val editing = modeVar.signal.map(_ != Mode.View)

        List[Modifier[HtmlElement]](
          modeVar.signal.updates --> { _ =>
            val _ = dom.window.setTimeout(() => adjustPosition(), 0)
          },
          cls("editing") <-- editing,
          headerTag(
            cls := Styles.nodePropertiesPopupHeader,
            cursor := "grab",
            onMouseDown --> (e => startHeaderDrag(e)),
            div(
              cls := Styles.nodePropertiesHeaderMain,
              div(
                cls := Styles.nodePropertiesEyebrowRow,
                nodeIcon(state.iconCode, state.iconColor),
                span(
                  cls := Styles.nodePropertiesEyebrow,
                  child.text <-- editing.map(e => if (e) "Editing properties" else "Properties"),
                ),
                nodeIdValue(idStr),
              ),
              child <-- editing.map(e => if (e) editLabels(model) else viewLabels(data)),
            ),
            div(
              cls := Styles.nodePropertiesHeaderActions,
              child <-- modeVar.signal.map {
                case Mode.View =>
                  button(
                    tpe := "button",
                    cls := s"${Styles.nodePropertiesBtn} ${Styles.nodePropertiesBtn}--secondary",
                    disabled := !canWrite,
                    title := (if (canWrite) "" else "Not authorized to WRITE to graph"),
                    icon("cil-pencil"),
                    span("Edit"),
                    onClick.stopPropagation --> { _ => if (canWrite) modeVar.set(Mode.Edit) },
                  )
                case Mode.Edit =>
                  button(
                    tpe := "button",
                    cls := s"${Styles.nodePropertiesBtn} ${Styles.nodePropertiesBtn}--secondary",
                    icon("ion-ios-close-empty"),
                    span("Cancel"),
                    onClick.stopPropagation --> { _ => modeVar.set(Mode.View) },
                  )
              },
              button(
                tpe := "button",
                cls := Styles.nodePropertiesIconBtn,
                aria.label := "Back",
                title := "Back to menu",
                icon("ion-ios-arrow-back"),
                onClick.stopPropagation --> { _ => onBack(posX, posY) },
              ),
            ),
          ),
          div(
            cls := Styles.nodePropertiesPopupContent,
            div(
              cls := Styles.nodePropertiesBody,
              div(
                cls := Styles.nodePropertiesField,
                child <-- editing.map(e => if (e) editProps(model) else viewProps(data, dismissOverlays.events)),
              ),
            ),
          ),
          child <-- editing.map(e =>
            if (e) footer(model, submitQuery, () => onRefreshProperties(posX, posY)) else emptyNode,
          ),
        )

      case NodePopupContent.Fallback(el) =>
        List[Modifier[HtmlElement]](
          // Pad the fallback message so it isn't flush against the popup edge.
          // (Wrapping of the message text itself is handled where the fallback
          // element is built — see QueryUi's `pre(cls := "wrap", …)`.)
          div(cls := Styles.nodePropertiesPopupContent, padding := "0.8em", el),
        )
    }

    div(
      cls := Styles.nodePropertiesPopup,
      left := s"${state.x}px",
      top := s"${state.y}px",
      onMountCallback { ctx =>
        popupEl = Some(ctx.thisNode.ref)
        document.addEventListener("click", onDocumentClick)
        ctx.thisNode.ref.addEventListener("scroll", onScroll, useCapture = true)
        val _ = dom.window.setTimeout(() => adjustPosition(), 0)
      },
      onUnmountCallback { node =>
        document.removeEventListener("click", onDocumentClick)
        node.ref.removeEventListener("scroll", onScroll, useCapture = true)
        popupEl = None
      },
      bodyModifiers,
    )
  }
}
