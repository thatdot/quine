package com.thatdot.quine.docs

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths, StandardOpenOption}

import scala.reflect.runtime.{universe => ru}

import io.circe.{Json, Printer}
import pureconfig.{ConfigFieldMapping, KebabCase, PascalCase}

/** Shared machinery for generating `capabilities.json`, the capability reference
  * consumed by the documentation site. Carries identifiers only; the docs supply
  * the prose. Reflection reads Scala signatures out of compiled classfiles, so
  * this must run on the ordinary compile classpath (as `runMain` does), never
  * against an obfuscated product jar.
  *
  * Only capabilities the OpenAPI spec does not already describe belong here.
  * Ingest sources, destinations, and output formats are rendered by the docs
  * site straight from the spec, so duplicating them here would be a second copy
  * to keep in step.
  */
object Capabilities {

  private val PersistenceAgentTypeClass: String = "com.thatdot.quine.app.config.PersistenceAgentType"

  private lazy val mirror = ru.runtimeMirror(getClass.getClassLoader)

  /** pureconfig's own mapping: `ClickHouse` becomes `click-house`, not `clickhouse`. */
  private val toConfigKey = ConfigFieldMapping(PascalCase, KebabCase)

  /** Direct subclass names of a sealed type. An unsealed type reports none,
    * which would silently shrink the docs rather than fail.
    */
  def sealedSubclassNames(fqcn: String): Set[String] = {
    val cls = mirror.staticClass(fqcn)
    require(cls.isSealed, s"$fqcn is not sealed, so its members cannot be enumerated")
    val names = cls.knownDirectSubclasses.map(_.name.toString)
    require(names.nonEmpty, s"$fqcn reported no subclasses")
    names
  }

  /** Persistors this product ships, as `{ id, className }` where `id` is the
    * `store` config block's `type` value. Support varies by product while the
    * type does not, so callers state it and every persistor must be accounted for.
    */
  def persistors(shipped: Set[String], notShipped: Set[String]): Json = {
    val known = sealedSubclassNames(PersistenceAgentTypeClass)
    val overlap = shipped.intersect(notShipped)
    require(
      overlap.isEmpty,
      s"Persistors listed as both shipped and not shipped: ${overlap.toVector.sorted.mkString(", ")}",
    )
    val declared = shipped ++ notShipped
    require(
      declared == known,
      "The persistor support declaration is out of step with PersistenceAgentType. " +
      s"Undeclared: ${(known -- declared).toVector.sorted.mkString(", ")}. " +
      s"Declared but not a persistor: ${(declared -- known).toVector.sorted.mkString(", ")}",
    )
    Json.arr(
      shipped.toVector.sorted.map { className =>
        Json.obj(
          "id" -> Json.fromString(toConfigKey(className)),
          "className" -> Json.fromString(className),
        )
      }: _*,
    )
  }

  /** Capability families common to every product. */
  def common(shipped: Set[String], notShipped: Set[String]): Vector[(String, Json)] =
    Vector("persistors" -> persistors(shipped, notShipped))

  /** The single output path a generator takes on its command line. Exits the
    * process on anything else, as the other generators do.
    */
  def outputPath(generatorName: String, args: Array[String]): Path = args match {
    case Array(output) => Paths.get(output)
    case _ =>
      println(generatorName + " expected one path argument but got: " + args.mkString("[", ", ", "]"))
      sys.exit(1)
  }

  def write(outputPath: Path, fields: Vector[(String, Json)]): Unit = {
    val json = Printer.spaces2.print(Json.obj(fields: _*))
    Files.createDirectories(outputPath.getParent)
    Files.write(
      outputPath,
      json.getBytes(StandardCharsets.UTF_8),
      StandardOpenOption.TRUNCATE_EXISTING,
      StandardOpenOption.CREATE,
    )
    println(s"Generated capability reference at: $outputPath")
  }
}
