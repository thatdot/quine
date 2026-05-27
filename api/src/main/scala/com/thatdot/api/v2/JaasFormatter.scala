package com.thatdot.api.v2

/** Shared helper for formatting Kafka JAAS login-module configuration strings.
  *
  * Lives here (rather than duplicated in `api.v2.SaslJaasConfig` and
  * `outputs2.SaslJaasConfig`) so that any escaping or structural change happens in exactly
  * one place. Pure-string interface — no `Secret` knowledge — so callers can render redacted
  * vs. preserved secrets however they like before handing the value here.
  */
object JaasFormatter {

  /** Escape a value for embedding in a JAAS `key="value"` option.
    *
    * Kafka's JAAS parser treats `"` as the value delimiter and `\` as an escape character,
    * so unescaped occurrences in user-supplied values (passwords, paths, etc.) produce
    * malformed JAAS that Kafka either rejects or misparses.
    *
    * Backslash MUST be escaped first to avoid double-escaping the substitution we add for `"`.
    */
  def escape(value: String): String =
    value.replace("\\", "\\\\").replace("\"", "\\\"")

  /** Render a single `key="value"` option with the value escaped. */
  def option(key: String, value: String): String =
    s"""$key="${escape(value)}""""

  /** Build a full JAAS login-module string of the form
    * `<loginModuleClass> required key1="v1" key2="v2" ...;`
    *
    * Optional fields should be filtered to `Some` and unwrapped before being passed in.
    */
  def loginModule(loginModuleClass: String, options: Seq[(String, String)]): String = {
    val rendered = options.map { case (k, v) => option(k, v) }.mkString(" ")
    s"$loginModuleClass required $rendered;"
  }
}
