package com.thatdot.quine.language.server

import org.eclipse.lsp4j.CompletionItemKind

/** The kind of a completion item, carrying the LSP [[CompletionItemKind]] it maps to.
  *
  * An ADT (like the sibling [[QueryKind]]) so an illegal kind cannot be constructed and each case
  * carries its `CompletionItemKind` directly. [[CompletionKind.NameKind]] is the subset a
  * [[NameCompletionSource]] can yield — a name is a function or a procedure — so
  * [[CompletionName.kind]] is constrained to those two; [[CompletionKind.Variable]] is reached only
  * by the in-scope-variable path.
  */
sealed trait CompletionKind {

  /** The LSP completion-item kind this maps to, used to set the editor's item icon. */
  def itemKind: CompletionItemKind
}

object CompletionKind {

  /** The kinds a [[NameCompletionSource]] yields: a name is either a function or a procedure. */
  sealed trait NameKind extends CompletionKind

  /** A Cypher function (built-in, Quine-specific, or user-defined). */
  case object Function extends NameKind {
    val itemKind: CompletionItemKind = CompletionItemKind.Function
  }

  /** A Cypher procedure. LSP 3.17 has no dedicated procedure kind, so it maps to `Method`, the
    * conventional choice for stored procedures.
    */
  case object Procedure extends NameKind {
    val itemKind: CompletionItemKind = CompletionItemKind.Method
  }

  /** An in-scope variable from the query buffer; never produced by a [[NameCompletionSource]]. */
  case object Variable extends CompletionKind {
    val itemKind: CompletionItemKind = CompletionItemKind.Variable
  }
}
