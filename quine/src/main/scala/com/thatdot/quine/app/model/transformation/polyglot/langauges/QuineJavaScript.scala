package com.thatdot.quine.app.model.transformation.polyglot.langauges

import java.io.ByteArrayInputStream

import scala.collection.SeqView

import cats.syntax.all._
import org.graalvm.polyglot
import org.graalvm.polyglot._
import org.graalvm.polyglot.io.IOAccess

import com.thatdot.quine.app.model.transformation.polyglot.{Polyglot, Transformation}
import com.thatdot.quine.exceptions.JavaScriptException
import com.thatdot.quine.util.BaseError

object JavascriptRuntime {

  /** Helper function for setting a series of repetitive "js.foo" options on the language runtime
    * @param builder
    * @param value The value you wish to set. Will be set to all flags passed
    * @param flags The flag names you wish to set (sans "js." prefix)
    * @return
    */
  private def setAll(builder: Engine#Builder, value: String, flags: Seq[String]): Engine#Builder =
    flags.foldLeft(builder)((b, flag) => b.option("js." + flag, value))

  implicit private class EngingBuilderOps(private val builder: Engine#Builder) extends AnyVal {
    def enableAll(flags: String*): Engine#Builder = setAll(builder, "true", flags)
    def disableAll(flags: String*): Engine#Builder = setAll(builder, "false", flags)
  }

  private val engine = Engine
    .newBuilder()
    .in(new ByteArrayInputStream(Array.emptyByteArray))
    .allowExperimentalOptions(true)
    .option("engine.WarnInterpreterOnly", "false")
    // Enable strict mode, set Array.prototype as the prototype of arrays passed-in from Java, and disable eval()
    // I can't think of anything unsafe they could do with `eval` (or `Graal`), but I removed them anyways.
    .enableAll("strict", "foreign-object-prototype", "disable-eval")
    // remove load, loadWithNewGlobal, print, console, and Graal globals
    // load / loadWithGlobal just return "PolyglotException: Error: Operation is not allowed for: foo.js" ( because we set allowIO to false ), but go ahead and remove them anyways.
    .disableAll("load", "print", "console", "graal-builtin")
    .build

  // Make the global context (and the objects it contains) immutable to prevent setting / changing global vars.
  // NB - do we want to recurse all the way down making everything immutable?
  private val freezeGlobals = polyglot.Source.create(
    "js",
    """
       Object.freeze(globalThis);
       Object.getOwnPropertyNames(globalThis).forEach(k => Object.freeze(globalThis[k]));
    """,
  )

  private def mkContext: Context = {
    val context = Context
      .newBuilder("js")
      .engine(engine)
      .allowAllAccess(false)
      .allowCreateProcess(false)
      .allowCreateThread(false)
      .allowEnvironmentAccess(EnvironmentAccess.NONE)
      .allowExperimentalOptions(false)
      .allowHostAccess(HostAccess.NONE)
      .allowHostClassLoading(false)
      .allowIO(IOAccess.NONE)
      .allowNativeAccess(false)
      .allowPolyglotAccess(PolyglotAccess.NONE)
      .build()
    context.eval(freezeGlobals)
    context
  }

  private val currentJsContext: ThreadLocal[Context] =
    ThreadLocal.withInitial(() => mkContext)

  def eval(source: polyglot.Source): polyglot.Value = currentJsContext.get.eval(source)
  def catchPolyglotException(a: => polyglot.Value): Either[String, polyglot.Value] = // Syntax errors are caught here
    Either.catchOnly[PolyglotException](a).leftMap(_.getMessage)

  def asSeqView(value: polyglot.Value): SeqView[polyglot.Value] =
    (0L until value.getArraySize).view.map(value.getArrayElement)

  def asList(value: polyglot.Value): Either[String, List[polyglot.Value]] = Either.cond(
    value.hasArrayElements,
    asSeqView(value).toList,
    s"'$value' should be an array",
  )

}

object JavaScriptTransformation {

  import JavascriptRuntime.{catchPolyglotException, eval}

  /** Validate the supplied JavaScript text and return a ready‑to‑run instance.
    *
    * @param jsText             the user‑supplied source (function literal or `function (…) { … }`)
    * @param outputCardinality  whether the JS returns one element or an array of elements
    * @param recordFormat       whether each element is Bare or Tagged
    */
  def makeInstance(
    jsText: String,
  ): Either[BaseError, JavaScriptTransformation] = {

    // Wrap in parentheses so both fat‑arrow and classic functions parse the same way
    val source = polyglot.Source.create("js", s"($jsText)")
    catchPolyglotException(eval(source)).left.map(JavaScriptException.apply).flatMap { compiled =>
      Either.cond(
        compiled.canExecute,
        new JavaScriptTransformation(compiled),
        JavaScriptException(s"'$jsText' must be a JavaScript function"),
      )
    }
  }
}

final class JavaScriptTransformation(
  transformationFunction: polyglot.Value,
) extends Transformation {
  def apply(input: Polyglot.HostValue): Either[BaseError, polyglot.Value] =
    try Right(transformationFunction.execute(input))
    catch {
      case ex: PolyglotException => Left(JavaScriptException(ex.getMessage))
      case ex: IllegalArgumentException => Left(JavaScriptException(ex.getMessage))
    }

}
