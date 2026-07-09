package com.thatdot.quine.language.procedures

import cats.data.NonEmptyList

import com.thatdot.quine.language.types.Type
import com.thatdot.quine.language.types.Type.PrimitiveType

/** Return signature of a procedure callable from Cypher.
  *
  * @param name the procedure's canonical (mixed-case) name, exactly as the runtime declares it
  * @param outputs the procedure's output columns in declaration order: column name -> the
  *                quine-language type of the column's values
  */
final case class ProcedureSignature(name: String, outputs: Vector[(String, Type)]) {

  /** Whether the procedure's rows consist entirely of node values.
    *
    * True exactly when there is at least one output column and every column is node-typed; a
    * procedure with no output columns yields no values at all, so it has no node rows.
    */
  def returnsOnlyNodes: Boolean =
    outputs.nonEmpty && outputs.forall { case (_, ty) => ty == PrimitiveType.NodeType }

  /** The declared type of the named output column, if the procedure declares it. Column names
    * are case-sensitive, as YIELD result fields are.
    */
  def outputType(column: String): Option[Type] =
    outputs.collectFirst { case (`column`, ty) => ty }
}

/** Registry of the return signatures of the procedures Quine ships, keyed by procedure name.
  *
  * == Where the truth lives ==
  *
  * The authoritative signatures are declared next to the procedure implementations in the
  * runtime modules, which depend on this module (quine-language is upstream of both), so the
  * shapes are mirrored here:
  *
  *   - `resolveCalls.builtInProcedures` in quine-cypher
  *     (`com.thatdot.quine.compiler.cypher.Procedures`), each procedure's
  *     `UserDefinedProcedureSignature.outputs`, plus the procedures the application registers
  *     at startup (`standing.wiretap`, `parseProtobuf`, `toProtobuf`)
  *   - `QuinePatternProcedureRegistry` in quine-core
  *     (`com.thatdot.quine.graph.cypher.quinepattern.procedures`), whose procedures are a
  *     subset of the above by name
  *
  * Runtime output types with no quine-language counterpart (maps, bytes, date/time types,
  * `Anything`) are recorded as [[Type.Any]]; node and edge columns are recorded exactly,
  * which is what query classification depends on. `ProcedureRegistrySyncTest` in quine-cypher
  * asserts entry-by-entry that this registry matches `resolveCalls.builtInProcedures`, so a
  * procedure added, removed, or re-typed there fails that test until it is mirrored here.
  *
  * == Name resolution ==
  *
  * The runtime registries store and resolve procedures by lowercased name
  * (`resolveCalls.rewriteCall` in quine-cypher, `QuinePatternProcedureRegistry.get` in
  * quine-core), so [[lookup]] lowercases the requested name the same way.
  */
object ProcedureRegistry {

  /** Looks up a procedure's signature by name, case-insensitively. */
  def lookup(name: String): Option[ProcedureSignature] =
    byLowerCaseName.get(name.toLowerCase)

  /** The declared type of one output column of a named procedure, if both the procedure and
    * the column (case-sensitive) are known.
    */
  def outputType(procedureName: String, column: String): Option[Type] =
    lookup(procedureName).flatMap(_.outputType(column))

  private val node: Type = PrimitiveType.NodeType
  private val edge: Type = PrimitiveType.EdgeType
  private val integer: Type = PrimitiveType.Integer
  private val real: Type = PrimitiveType.Real
  private val string: Type = PrimitiveType.String
  private val listOfString: Type = Type.TypeConstructor(Symbol("List"), NonEmptyList.of(string))
  private val listOfAny: Type = Type.TypeConstructor(Symbol("List"), NonEmptyList.of(Type.Any))

  /** Every shipped procedure's return signature, mirroring the runtime declarations cited in
    * the object documentation.
    */
  val all: Vector[ProcedureSignature] = Vector(
    // Stubs for compatibility with external systems (StubbedUserDefinedProcedure)
    ProcedureSignature(
      "db.indexes",
      Vector(
        "description" -> Type.Any,
        "indexName" -> Type.Any,
        "tokenNames" -> Type.Any,
        "properties" -> Type.Any,
        "state" -> Type.Any,
        "type" -> Type.Any,
        "progress" -> Type.Any,
        "provider" -> Type.Any,
        "id" -> Type.Any,
        "failureMessage" -> Type.Any,
      ),
    ),
    ProcedureSignature("db.relationshipTypes", Vector("relationshipType" -> Type.Any)),
    ProcedureSignature("db.propertyKeys", Vector("propertyKey" -> Type.Any)),
    ProcedureSignature("dbms.labels", Vector("label" -> Type.Any)),
    // Introspection
    ProcedureSignature(
      "help.builtins",
      Vector("name" -> string, "signature" -> string, "description" -> string),
    ),
    ProcedureSignature(
      "help.functions",
      Vector("name" -> string, "signature" -> string, "description" -> string),
    ),
    ProcedureSignature(
      "help.procedures",
      Vector("name" -> string, "signature" -> string, "description" -> string, "mode" -> string),
    ),
    ProcedureSignature(
      "debug.node",
      Vector(
        "atTime" -> Type.Any,
        "properties" -> Type.Any,
        "edges" -> listOfAny,
        "latestUpdateMillisAfterSnapshot" -> integer,
        "subscribers" -> string,
        "subscriptions" -> string,
        "multipleValuesStandingQueryStates" -> listOfAny,
        "journal" -> listOfAny,
        "graphNodeHashCode" -> integer,
      ),
    ),
    ProcedureSignature(
      "subscribers",
      Vector("queryId" -> integer, "queryDepth" -> integer, "receiverId" -> string, "lastResult" -> Type.Any),
    ),
    ProcedureSignature(
      "subscriptions",
      Vector("queryId" -> integer, "queryDepth" -> integer, "receiverId" -> string, "lastResult" -> Type.Any),
    ),
    // Nested-query execution
    ProcedureSignature("do.when", Vector("value" -> Type.Any)),
    ProcedureSignature("cypher.doIt", Vector("value" -> Type.Any)),
    ProcedureSignature("cypher.do.case", Vector("value" -> Type.Any)),
    ProcedureSignature("cypher.runTimeboxed", Vector("value" -> Type.Any)),
    // Graph reads
    ProcedureSignature("recentNodes", Vector("node" -> node)),
    ProcedureSignature("recentNodeIds", Vector("nodeId" -> Type.Any)),
    ProcedureSignature("getFilteredEdges", Vector("edge" -> edge)),
    ProcedureSignature("random.walk", Vector("walk" -> listOfString)),
    // Graph writes
    ProcedureSignature("create.relationship", Vector("rel" -> edge)),
    ProcedureSignature("create.setProperty", Vector.empty),
    ProcedureSignature("create.setLabels", Vector.empty),
    ProcedureSignature("reify.time", Vector("node" -> node)),
    ProcedureSignature("incrementCounter", Vector("count" -> integer)),
    ProcedureSignature("int.add", Vector("result" -> integer)),
    ProcedureSignature("float.add", Vector("result" -> real)),
    ProcedureSignature("set.insert", Vector("result" -> listOfAny)),
    ProcedureSignature("set.union", Vector("result" -> listOfAny)),
    ProcedureSignature("purgeNode", Vector.empty),
    // Utilities
    ProcedureSignature("loadJsonLines", Vector("value" -> Type.Any)),
    ProcedureSignature("log", Vector("log" -> string)),
    ProcedureSignature("util.sleep", Vector.empty),
    ProcedureSignature("debug.sleep", Vector.empty),
    // Registered at application startup rather than in resolveCalls.builtInProcedures
    ProcedureSignature("standing.wiretap", Vector("data" -> Type.Any, "meta" -> Type.Any)),
    ProcedureSignature("parseProtobuf", Vector("value" -> Type.Any)),
    ProcedureSignature("toProtobuf", Vector("protoBytes" -> Type.Any)),
  )

  private val byLowerCaseName: Map[String, ProcedureSignature] =
    all.map(signature => signature.name.toLowerCase -> signature).toMap
}
