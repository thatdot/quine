package com.thatdot.quine.webapp.fields

import com.raquo.laminar.api.L._

/** Layer 3 of the field-learning stack: a generic, selectable rendering of a [[FieldSchema]].
  *
  * Knows nothing about Novelty, observations, or tags. The caller supplies a set of
  * [[FieldPicker.Role]]s (named selection slots — "observation", "tag", "column", "key", …) and
  * reads the user's choices from the `selections` Var. A `multi` role may be assigned to many
  * fields; a single (non-multi) role is exclusive — assigning it to a field clears it elsewhere.
  */
object FieldPicker {

  /** A named selection slot the caller wants filled. `multi` allows a role on many fields;
    * `allows` restricts which fields the role may be assigned to (its control is disabled on the
    * rest).
    *
    * @param description tooltip on the role's control, saying what assigning a field to it means
    * @param disallowedHint tooltip on the control where `allows` rejects the field, saying why
    */
  final case class Role(
    id: String,
    label: String,
    multi: Boolean,
    allows: FieldPath => Boolean = _ => true,
    description: String = "",
    disallowedHint: String = "",
  )

  /** A single assignment of a field path to a role. */
  final case class Selection(path: FieldPath, roleId: String)

  /** Swap the `index`-th selection of role `roleId` with its neighbour `delta` places away (counting
    * only that role's selections), leaving every other selection where it is. Unchanged when the
    * target falls outside the role's selections. A multi role's selections keep the order fields
    * were picked in, so this is how a caller lets the user rearrange them.
    */
  def moveWithinRole(sels: Vector[Selection], roleId: String, index: Int, delta: Int): Vector[Selection] = {
    val inRole = sels.filter(_.roleId == roleId)
    val target = index + delta
    if (!inRole.indices.contains(index) || !inRole.indices.contains(target)) sels
    else {
      val reordered = inRole.updated(index, inRole(target)).updated(target, inRole(index)).iterator
      sels.map(sel => if (sel.roleId == roleId) reordered.next() else sel)
    }
  }

  private def kindBadges(stat: FieldStat): Seq[HtmlElement] =
    stat.kinds.toVector.sortBy(JsonKind.label).map { k =>
      span(cls := "badge bg-secondary-subtle text-body-secondary me-1", JsonKind.label(k))
    }

  private def exampleText(stat: FieldStat): Option[String] =
    stat.examples.headOption.map { j =>
      val s = j.asString.getOrElse(j.noSpaces)
      if (s.length > 40) s.take(39) + "…" else s
    }

  /** @param schema     the learned field schema (reactive — rows appear as samples arrive)
    * @param roles      the selection slots to offer per field
    * @param selections the user's current assignments, owned by the caller
    */
  def apply(
    schema: Signal[FieldSchema],
    roles: Vector[Role],
    selections: Var[Vector[Selection]],
  ): HtmlElement = {

    def toggle(path: FieldPath, role: Role): Unit =
      selections.update { sels =>
        val sel = Selection(path, role.id)
        if (sels.contains(sel)) sels.filterNot(_ == sel)
        else if (role.multi) sels :+ sel
        else sels.filterNot(_.roleId == role.id) :+ sel // single role: exclusive
      }

    def roleControl(path: FieldPath, role: Role): HtmlElement = {
      val isChecked = selections.signal.map(_.contains(Selection(path, role.id)))
      val tooltip = if (role.allows(path)) role.description else role.disallowedHint
      label(
        cls := "form-check form-check-inline mb-0 me-3",
        if (tooltip.nonEmpty) title := tooltip else emptyMod,
        input(
          cls := "form-check-input",
          typ := (if (role.multi) "checkbox" else "radio"),
          disabled := !role.allows(path),
          checked <-- isChecked,
          onClick --> { _ => toggle(path, role) },
        ),
        span(cls := "form-check-label small", role.label),
      )
    }

    def row(path: FieldPath, stat: FieldStat, sampleCount: Int): HtmlElement = {
      val presencePct = if (sampleCount == 0) 0 else math.round(stat.presentIn.toDouble / sampleCount * 100).toInt
      div(
        cls := "d-flex align-items-center justify-content-between border-bottom py-1",
        div(
          cls := "me-2",
          code(cls := "small", path.render),
          span(cls := "ms-2", kindBadges(stat)),
          if (stat.isMixed) span(cls := "badge bg-warning-subtle text-warning-emphasis me-1", "mixed") else emptyNode,
          span(
            cls := "small text-body-secondary ms-1",
            if (stat.declared && stat.presentIn == 0) "added by hand" else s"seen $presencePct%",
          ),
          exampleText(stat).map(ex => span(cls := "small text-body-secondary ms-2", s"e.g. $ex")).getOrElse(emptyNode),
        ),
        div(cls := "d-flex flex-shrink-0", roles.map(r => roleControl(path, r))),
      )
    }

    div(
      cls := "field-picker",
      children <-- schema.map { s =>
        val leaves = s.leaves
        if (leaves.isEmpty) List(div(cls := "text-body-secondary small fst-italic", "No fields discovered yet."))
        else leaves.map { case (path, stat) => row(path, stat, s.sampleCount) }.toList
      },
    )
  }
}
