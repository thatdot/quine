package com.thatdot.quine.language.server

import com.thatdot.quine.language.functions.{BuiltinFunctionNames, FunctionDocRegistry}
import com.thatdot.quine.language.procedures.ProcedureDocRegistry

/** One function- or procedure-name completion candidate offered by a [[NameCompletionSource]].
  *
  * @param label the canonical name shown in the popup, e.g. `reify.time` or `idFrom`
  * @param kind whether the name is a function or a procedure
  * @param detail the signature line(s) shown beside the label, or `None` for a name without one
  * @param documentation the Markdown description shown in the popup, or `None`
  */
final case class CompletionName(
  label: String,
  kind: CompletionKind.NameKind,
  detail: Option[String],
  documentation: Option[String],
)

/** Supplies the function- and procedure-name candidates the editor offers as completions.
  *
  * The abstraction exists because of the module dependency direction: the authoritative runtime
  * registries (`Func`/`Proc`) live in `quine-core`, which depends on `quine-language`, so this
  * module cannot read them directly. The completion logic here depends on this trait instead, and
  * the implementation backed by the live registries is injected from the `quine` app — the only
  * place that sees both modules.
  *
  * [[NameCompletionSource.Static]] reads the in-module documentation registries, keeping
  * `quine-language` self-contained for tests and standalone use. A running Quine app instead
  * injects a source backed by the live runtime registries, so user-defined functions and
  * procedures are offered too and the list grows with the language.
  */
trait NameCompletionSource {

  /** The current name candidates. Read on each completion request, so a source backed by mutable
    * runtime registries reflects names registered after the source was constructed.
    */
  def names(): Seq[CompletionName]
}

object NameCompletionSource {

  /** The static, in-module source: Quine's documented functions and procedures plus the standard
    * openCypher built-in names. Documented entries come first so they win the de-duplication in
    * [[ContextAwareLanguageService.nameCompletions]] over a same-named bare built-in.
    */
  object Static extends NameCompletionSource {
    def names(): Seq[CompletionName] =
      FunctionDocRegistry.all.map(doc =>
        CompletionName(doc.name, CompletionKind.Function, Some(doc.signature), Some(doc.description)),
      ) ++
      ProcedureDocRegistry.all.map(doc =>
        CompletionName(doc.name, CompletionKind.Procedure, Some(doc.signature), Some(doc.description)),
      ) ++
      BuiltinFunctionNames.all.map(name => CompletionName(name, CompletionKind.Function, None, None))
  }
}
