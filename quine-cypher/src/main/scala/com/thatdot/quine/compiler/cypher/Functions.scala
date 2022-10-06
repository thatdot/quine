package com.thatdot.quine.compiler.cypher

import java.nio.charset.StandardCharsets
import java.time.format.DateTimeFormatter
import java.time.temporal.TemporalField
import java.time.{
  Duration => JavaDuration,
  LocalDateTime => JavaLocalDateTime,
  ZoneId,
  ZonedDateTime => JavaZonedDateTime
}
import java.util.{Locale, TimeZone}

import scala.collection.concurrent
import scala.util.{Failure, Random, Try}

import com.google.common.hash.Hashing
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.codec.DecoderException
import org.apache.commons.codec.net.PercentCodec
import org.opencypher.v9_0.expressions._
import org.opencypher.v9_0.expressions.functions.{Category, Function, FunctionWithName}
import org.opencypher.v9_0.frontend.phases._
import org.opencypher.v9_0.util.Foldable.TreeAny
import org.opencypher.v9_0.util.Rewritable.IteratorEq
import org.opencypher.v9_0.util.StepSequencer.Condition
import org.opencypher.v9_0.util.{InputPosition, Rewritable, Rewriter, bottomUp, symbols}

import com.thatdot.quine.graph.cypher.UserDefinedProcedure.extractQuineId
import com.thatdot.quine.graph.cypher._
import com.thatdot.quine.graph.{hashOfCypherValues, idFrom}
import com.thatdot.quine.model.{NamespacedIdProvider, QuineId, QuineIdProvider}
import com.thatdot.quine.util.HexConversions

/** Class that wraps a Quine UDF into something that openCypher accepts as a function
  *
  * @param quineUdf underlying UDF
  */
final class OpenCypherUdf(quineUdf: UserDefinedFunction) extends Function with TypeSignatures {

  def name = quineUdf.name

  override def signatures: Seq[TypeSignature] = quineUdf.signatures.map {
    case UserDefinedFunctionSignature(arguments, outputType, description) =>
      FunctionTypeSignature(
        function = new FunctionWithName {
          override def name: String = quineUdf.name
        },
        names = arguments.map(_._1).toVector,
        argumentTypes = arguments.map(arg => OpenCypherUdf.typeToOpenCypherType(arg._2)).toVector,
        outputType = OpenCypherUdf.typeToOpenCypherType(outputType),
        description = description,
        category = quineUdf.category
      )
  }
}
object OpenCypherUdf {

  /** Convert a Quine type into the closest fitting openCypher type */
  def typeToOpenCypherType(cType: Type): symbols.CypherType =
    cType match {
      case Type.Number => symbols.CTNumber
      case Type.Integer => symbols.CTInteger
      case Type.Floating => symbols.CTFloat
      case Type.Bool => symbols.CTBoolean
      case Type.Str => symbols.CTString
      case Type.List(of) => symbols.CTList(typeToOpenCypherType(of))
      case Type.Map => symbols.CTMap
      case Type.Node => symbols.CTNode
      case Type.Relationship => symbols.CTRelationship
      case Type.Path => symbols.CTPath
      case Type.Duration => symbols.CTDuration
      case Type.DateTime => symbols.CTDateTime
      case Type.LocalDateTime => symbols.CTLocalDateTime
      case _ => symbols.CTAny
    }
}

/** Like [[FunctionInvocation]] but where the function is an extensible type
  * that specifies the function 'body' and which can be typechecked
  */
final class QuineFunctionInvocation(
  udf: UserDefinedFunction,
  override val namespace: Namespace,
  override val functionName: FunctionName,
  override val args: IndexedSeq[Expression],
  override val position: InputPosition
) extends FunctionInvocation(namespace, functionName, false, args)(position)
    with Rewritable {
  override val distinct = false
  override val function = new OpenCypherUdf(udf)

  /* This _must_ be overriden or else `QuineFunctionInvocation` risks being
   * re-written back to `FunctionInvocation`. This is all thanks to the fact
   * that this class is extending a `case class` and `ASTNode.dup` looks up the
   * constructor to use from `Rewritable.copyConstructor`, which in turn defers
   * to `Product`...
   *
   * See QU-433
   */
  override def dup(children: Seq[AnyRef]): this.type =
    if (children.iterator eqElements this.treeChildren) {
      this
    } else {
      require(children.length == 4, "Wrong number of AST children")
      new QuineFunctionInvocation(
        udf,
        children(0).asInstanceOf[Namespace],
        children(1).asInstanceOf[FunctionName],
        children(3).asInstanceOf[IndexedSeq[Expression @unchecked]],
        position
      ).asInstanceOf[this.type]
    }
}

/** Re-write unresolved functions into variants that are resolved via
  * reflection
  */
case object resolveFunctions extends StatementRewriter {

  val additionalFeatures: List[UserDefinedFunction] = List(
    CypherStrId,
    CypherQuineId,
    CypherBytes,
    CypherStringBytes,
    CypherHash,
    CypherIdFrom,
    CypherLocIdFrom,
    CypherGetHostFunction,
    CypherToJson,
    CypherParseJson,
    CypherUtf8Decode,
    CypherUtf8Encode,
    CypherMapFromPairs,
    CypherMapSortedProperties,
    CypherMapMerge,
    CypherMapRemoveKey,
    CypherMapDropNullValues,
    CypherTextSplit,
    CypherTextRegexFirstMatch,
    CypherTextUrlEncode,
    CypherTextUrlDecode,
    CypherDateTime,
    CypherLocalDateTime,
    CypherDuration,
    CypherDurationBetween,
    CypherFormatTemporal,
    CypherCollMax,
    CypherCollMin,
    CypherMetaType
  ) ++ CypherGenFroms.all

  /** This map is only meant to maintain backward compatibility for a short time. */
  val deprecatedNames: Map[String, UserDefinedFunction] = Map.empty

  private val functions: concurrent.Map[String, UserDefinedFunction] = Func.userDefinedFunctions
  additionalFeatures.foreach(registerUserDefinedFunction)
  functions ++= deprecatedNames.map { case (rename, f) => rename.toLowerCase -> f }

  val rewriteFunc: PartialFunction[AnyRef, AnyRef] = {
    case fi @ FunctionInvocation(ns, name, false, args) if fi.needsToBeResolved =>
      functions.get(fi.name.toLowerCase) match {
        case None => fi
        case Some(func) => new QuineFunctionInvocation(func, ns, name, args, fi.position)
      }
  }

  override def instance(ctx: BaseContext): Rewriter = bottomUp(Rewriter.lift(rewriteFunc))

  // TODO: add to this
  override def postConditions: Set[Condition] = Set.empty
}

/** Sample UDF: given a string Quine ID, turn that into [[Expr.Bytes]],
  * which we can then use to enter the graph via [[Query#ArgumentEntry]].
  */
object CypherQuineId extends UserDefinedFunction {
  val name = "quineId"
  val isPure = true
  val signatures: Vector[UserDefinedFunctionSignature] = Vector(
    UserDefinedFunctionSignature(
      arguments = Vector("input" -> Type.Str),
      output = Type.Bytes,
      description = "Returns the Quine ID corresponding to the string"
    )
  )
  val category = Category.SCALAR

  def call(args: Vector[Value])(implicit idProvider: QuineIdProvider): Value =
    args match {
      case Vector(Expr.Str(str)) =>
        idProvider
          .qidFromPrettyString(str)
          .toOption
          .fold[Value](Expr.Null)((qid: QuineId) => Expr.Bytes(qid))
      case other => throw wrongSignature(other)
    }
}

/** Given a node, extract the string representation of its ID. */
object CypherStrId extends UserDefinedFunction {
  val name = "strId"
  val isPure = true
  val signatures: Vector[UserDefinedFunctionSignature] = Vector(
    UserDefinedFunctionSignature(
      arguments = Vector("input" -> Type.Node),
      output = Type.Str,
      description = "Returns a string representation of the node's ID"
    )
  )
  val category = Category.SCALAR

  def call(args: Vector[Value])(implicit idProvider: QuineIdProvider): Value =
    args match {
      case Vector(Expr.Node(qid, _, _)) => Expr.Str(idProvider.qidToPrettyString(qid))
      case other => throw wrongSignature(other)
    }
}

/** Given a string of hexadecimal characters, extract a value of type bytes.
  *
  * If the string contains invalid characters, returns `null`.
  */
object CypherBytes extends UserDefinedFunction {
  val name = "bytes"
  val isPure = true
  val signatures: Vector[UserDefinedFunctionSignature] = Vector(
    UserDefinedFunctionSignature(
      arguments = Vector("input" -> Type.Str),
      output = Type.Bytes,
      description = "Returns bytes represented by a hexadecimal string"
    )
  )
  val category = Category.STRING

  def call(args: Vector[Value])(implicit idp: QuineIdProvider): Value =
    args match {
      case Vector(Expr.Str(hexStr)) =>
        val noSpaceHexStr = hexStr.filter(!_.isWhitespace)
        try Expr.Bytes(HexConversions.parseHexBinary(noSpaceHexStr))
        catch {
          case _: IllegalArgumentException => Expr.Null
        }
      case other => throw wrongSignature(other)
    }
}

object CypherStringBytes extends UserDefinedFunction {
  val name = "convert.stringToBytes"
  val isPure = true
  val signatures: Vector[UserDefinedFunctionSignature] = Vector(
    UserDefinedFunctionSignature(
      arguments = Vector("input" -> Type.Str, "encoding" -> Type.Str),
      output = Type.Bytes,
      description = "Encodes a string into bytes according to the specified encoding"
    )
  )
  val category = Category.STRING

  def call(args: Vector[Value])(implicit idp: QuineIdProvider): Value =
    args match {
      case Vector(Expr.Str(input), Expr.Str(encoding)) =>
        encoding.toLowerCase match {
          case "utf-8" => Expr.Bytes(input.getBytes(StandardCharsets.UTF_8))
          case "utf-16" => Expr.Bytes(input.getBytes(StandardCharsets.UTF_16))
          case "iso-8859-1" => Expr.Bytes(input.getBytes(StandardCharsets.ISO_8859_1))
          case _ => Expr.Null
        }
      case other => throw wrongSignature(other)
    }
}

object CypherHash extends UserDefinedFunction {
  val name = "hash"
  val isPure = true
  // `hash` should be variadic, but we compromise with up to 15 arguments
  val signatures: Vector[UserDefinedFunctionSignature] = Vector.tabulate(16) { (i: Int) =>
    UserDefinedFunctionSignature(
      arguments = Vector.tabulate(i) { j =>
        s"input$j" -> Type.Anything
      },
      output = Type.Integer,
      description = "Hashes the input arguments"
    )
  }
  val category = Category.SCALAR

  override def call(args: Vector[Value])(implicit idp: QuineIdProvider): Value = {
    val hasher = Hashing.murmur3_128().newHasher()
    for (arg <- args)
      hasher.putBytes(arg.hash.asBytes)
    Expr.Integer(hasher.hash.asLong)
  }
}

object CypherIdFrom extends UserDefinedFunction {
  val name = "idFrom"
  val isPure = true
  // `idFrom` should be variadic, but we compromise with up to 16 arguments
  val signatures: Vector[UserDefinedFunctionSignature] = Vector.tabulate(16) { (i: Int) =>
    UserDefinedFunctionSignature(
      arguments = Vector.tabulate(i) { j =>
        s"input$j" -> Type.Anything
      },
      output = Type.Anything,
      description = "Hashes the input arguments into a valid ID"
    )
  }
  val category = Category.SCALAR

  def call(args: Vector[Value])(implicit idProvider: QuineIdProvider): Value = {
    val hashedQid: QuineId = idFrom(args: _*)
    Expr.fromQuineValue(idProvider.qidToValue(hashedQid))
  }
}

// trait for functions that require a partition-aware IdProvider to function
// TODO only register these when an appropriate idProvider is configured (or when the cluster size is 1?)
trait PartitionSensitiveFunction extends UserDefinedFunction {
  final def call(arguments: Vector[Value])(implicit idProvider: QuineIdProvider): Value = idProvider match {
    case namespacedProvider: NamespacedIdProvider => callNamespaced(arguments)(namespacedProvider)
    case notNamespacedProvider @ _ =>
      throw CypherException.ConstraintViolation(
        s"Unable to use a non-namespaced ID provider ($notNamespacedProvider) with a namespace-dependent function $name",
        None
      )
  }
  def callNamespaced(arguments: Vector[Value])(implicit idProvider: NamespacedIdProvider): Value
}

object CypherLocIdFrom extends UserDefinedFunction with PartitionSensitiveFunction {
  val name = "locIdFrom"
  val isPure = true
  // as with [[CypherIdFrom]], we emulate a variadic argument, this time in the second position
  val signatures: Vector[UserDefinedFunctionSignature] = Vector.tabulate(15) { (i: Int) =>
    UserDefinedFunctionSignature(
      arguments = Vector.tabulate(i) {
        case 0 => s"partition" -> Type.Str
        case j => s"input${j - 1}" -> Type.Anything
      },
      output = Type.Integer,
      description = "Generates a localized ID (based on a hash of the input elements, if provided). " +
        "All IDs generated with the same `partition` will correspond to nodes on the same host."
    )
  }
  val category = Category.SCALAR

  def callNamespaced(arguments: Vector[Value])(implicit idProvider: NamespacedIdProvider): Value =
    Expr.fromQuineValue(
      idProvider.qidToValue(
        idProvider.customIdToQid(
          arguments.toList match {
            case Expr.Str(partition) :: idFromArgs =>
              (idFromArgs match {
                case Nil =>
                  idProvider
                    .newCustomIdInNamespace(partition)
                case hashMeValues =>
                  idProvider
                    .hashedCustomIdInNamespace(partition, hashOfCypherValues(hashMeValues))
              }).recoverWith { case err =>
                Failure(
                  CypherException.ConstraintViolation(
                    s"Unable to create localized (partitioned) ID; underlying error was ${err.getMessage}"
                  )
                )
              }.get
            case _ => // Nil or the first parameter is anything other than a string
              throw wrongSignature(arguments)
          }
        )
      )
    )
}

/** Get the host a node should be assigned to, according to the idProvider. If the ID provider doesn't specify, you'll
  * need the clusterConfig, and therefore the procedure variant [[GetHost]]
  */
object CypherGetHostFunction extends UserDefinedFunction {
  val name = "getHost"
  val isPure = false // because it reads cluster node configuration
  val signatures: Vector[UserDefinedFunctionSignature] = Vector(
    UserDefinedFunctionSignature(
      arguments = Vector("node" -> Type.Node),
      output = Type.Integer,
      description = "Compute which host a node should be assigned to (null if unknown without contacting the graph)"
    ),
    UserDefinedFunctionSignature(
      arguments = Vector("nodeIdStr" -> Type.Str),
      output = Type.Integer,
      description =
        "Compute which host a node ID (string representation) should be assigned to (null if unknown without contacting the graph)"
    ),
    UserDefinedFunctionSignature(
      arguments = Vector("nodeIdBytes" -> Type.Bytes),
      output = Type.Integer,
      description =
        "Compute which host a node ID (bytes representation) should be assigned to (null if unknown without contacting the graph)"
    )
  )
  val category = Category.SCALAR

  def call(arguments: Vector[Value])(implicit idProvider: QuineIdProvider): Value = {
    val id: QuineId = arguments match {
      case Vector(oneArg) => extractQuineId(oneArg)(idProvider).getOrElse(throw wrongSignature(arguments))
      case _ => throw wrongSignature(arguments)
    }

    idProvider.nodeLocation(id).hostIdx.fold[Value](Expr.Null)(hostIdx => Expr.Integer(hostIdx.toLong))
  }
}

// TODO consider serializing multiple parameters as arrays as wel
object CypherToJson extends UserDefinedFunction {
  val name = "toJson"
  val isPure = true
  val signatures: Vector[UserDefinedFunctionSignature] = Vector(
    UserDefinedFunctionSignature(
      arguments = Vector("x" -> Type.Anything),
      output = Type.Str,
      description = "Returns x encoded as a JSON string"
    )
  )
  val category = Category.STRING

  def call(args: Vector[Value])(implicit idProvider: QuineIdProvider): Value = args match {
    case Vector(x) => Expr.Str(ujson.write(Value.toJson(x)))
    case other => throw wrongSignature(other)
  }
}

object CypherParseJson extends UserDefinedFunction {
  val name = "parseJson"
  val isPure = true
  val signatures: Vector[UserDefinedFunctionSignature] = Vector(
    UserDefinedFunctionSignature(
      arguments = Vector("jsonStr" -> Type.Str),
      output = Type.Anything,
      description = "Parses jsonStr to a Cypher value"
    )
  )
  val category = Category.STRING

  def call(args: Vector[Value])(implicit idProvider: QuineIdProvider): Value = args match {
    case Vector(Expr.Str(jsonStr)) => Value.fromJson(ujson.read(jsonStr))
    case other => throw wrongSignature(other)
  }
}

object CypherUtf8Decode extends UserDefinedFunction {
  val name = "text.utf8Decode"
  val isPure = true
  val signatures: Vector[UserDefinedFunctionSignature] = Vector(
    UserDefinedFunctionSignature(
      arguments = Vector("bytes" -> Type.Bytes),
      output = Type.Str,
      description = "Returns the bytes decoded as a UTF-8 String"
    )
  )
  val category = Category.STRING

  // NB this will "fix" incorrectly-serialized UTF-8 by replacing invalid portions of input with the UTF-8 replacement string "\uFFFD"
  // This is typical for such decoders
  def call(args: Vector[Value])(implicit idProvider: QuineIdProvider): Value = args match {
    case Vector(Expr.Bytes(bytes, _)) =>
      Expr.Str(new String(bytes, StandardCharsets.UTF_8))
    case other => throw wrongSignature(other)
  }
}

object CypherUtf8Encode extends UserDefinedFunction {
  val name = "text.utf8Encode"
  val isPure = true
  val signatures: Vector[UserDefinedFunctionSignature] = Vector(
    UserDefinedFunctionSignature(
      arguments = Vector("string" -> Type.Str),
      output = Type.Bytes,
      description = "Returns the string encoded as UTF-8 bytes"
    )
  )
  val category = Category.STRING

  def call(args: Vector[Value])(implicit idProvider: QuineIdProvider): Value = args match {
    case Vector(Expr.Str(str)) => Expr.Bytes(str.getBytes(StandardCharsets.UTF_8))
    case other => throw wrongSignature(other)
  }
}

/** Function to work around the fact that Cypher cannot construct map literals
  * with dynamic keys. Based off of `apoc.map.fromPairs`
  */
object CypherMapFromPairs extends UserDefinedFunction {
  val name = "map.fromPairs"
  val isPure = true
  val signatures: Vector[UserDefinedFunctionSignature] = Vector(
    UserDefinedFunctionSignature(
      arguments = Vector("entries" -> Type.List(Type.ListOfAnything)),
      output = Type.Map,
      description = "Construct a map from a list of [key,value] entries"
    )
  )
  val category = "Map"

  def call(args: Vector[Value])(implicit idProvider: QuineIdProvider): Value = {
    val output = Map.newBuilder[String, Value]

    args match {
      case Vector(Expr.List(entries)) =>
        for (entry <- entries)
          entry match {
            case Expr.List(Vector(Expr.Str(key), value)) => output += key -> value
            case _ =>
              throw CypherException.TypeMismatch(
                expected = Seq(Type.ListOfAnything), // TODO: this isn't very informative!
                actualValue = entry,
                context = "key value pair in `map.fromPairs`"
              )
          }
      case other => throw wrongSignature(other)
    }

    Expr.Map(output.result())
  }
}

object CypherMapSortedProperties extends UserDefinedFunction {
  val name = "map.sortedProperties"
  val isPure = true
  val signatures: Vector[UserDefinedFunctionSignature] = Vector(
    UserDefinedFunctionSignature(
      arguments = Vector("map" -> Type.Map),
      output = Type.List(Type.ListOfAnything),
      description = "Extract from a map a list of [key,value] entries sorted by the key"
    )
  )
  val category = "Map"

  def call(args: Vector[Value])(implicit idProvider: QuineIdProvider): Value =
    args match {
      case Vector(Expr.Map(entries)) =>
        val sortedProperties = entries.toVector
          .sortBy(_._1)
          .map { case (k, v) => Expr.List(Vector(Expr.Str(k), v)) }
        Expr.List(sortedProperties)
      case other => throw wrongSignature(other)
    }
}

// TODO: this should support an optional `config` parameter (see QU-558 on optional parameters)
object CypherMapRemoveKey extends UserDefinedFunction {
  val name = "map.removeKey"
  val isPure = true
  val signatures: Vector[UserDefinedFunctionSignature] = Vector(
    UserDefinedFunctionSignature(
      arguments = Vector("map" -> Type.Map, "key" -> Type.Str),
      output = Type.Map,
      description = "remove the key from the map"
    )
  )
  val category = "Map"

  def call(args: Vector[Value])(implicit idProvider: QuineIdProvider): Value =
    args match {
      case Vector(Expr.Map(entries), Expr.Str(key)) => Expr.Map(entries - key)
      case other => throw wrongSignature(other)
    }
}

// TODO: handling around null cases is not the same as APOC
object CypherMapMerge extends UserDefinedFunction {
  val name = "map.merge"
  val isPure = true
  val signatures: Vector[UserDefinedFunctionSignature] = Vector(
    UserDefinedFunctionSignature(
      arguments = Vector("first" -> Type.Map, "second" -> Type.Map),
      output = Type.Map,
      description = "Merge two maps"
    )
  )
  val category = "Map"

  def call(args: Vector[Value])(implicit idProvider: QuineIdProvider): Value =
    args match {
      case Vector(Expr.Map(firstEntries), Expr.Map(secondEntries)) =>
        Expr.Map(firstEntries ++ secondEntries)
      case other => throw wrongSignature(other)
    }
}

object CypherMapDropNullValues extends UserDefinedFunction {
  val name = "map.dropNullValues"
  val isPure = true
  val signatures: Vector[UserDefinedFunctionSignature] = Vector(
    UserDefinedFunctionSignature(
      arguments = Vector("argument" -> Type.Map),
      output = Type.Map,
      description = "Keep only non-null from the map"
    )
  )
  val category = "Map"

  def call(args: Vector[Value])(implicit idProvider: QuineIdProvider): Value =
    args match {
      case Vector(Expr.Map(entries)) =>
        Expr.Map(entries.filter(_._2 != Expr.Null))
      case other => throw wrongSignature(other)
    }
}

object CypherTextSplit extends UserDefinedFunction {
  val name = "text.split"
  val isPure = true
  val signatures: Vector[UserDefinedFunctionSignature] = Vector(
    UserDefinedFunctionSignature(
      arguments = Vector("text" -> Type.Str, "regex" -> Type.Str),
      output = Type.List(Type.Str),
      description = "Splits the string around matches of the regex"
    ),
    UserDefinedFunctionSignature(
      arguments = Vector("text" -> Type.Str, "regex" -> Type.Str, "limit" -> Type.Integer),
      output = Type.List(Type.Str),
      description = "Splits the string around the first `limit` matches of the regex"
    )
  )
  val category = Category.STRING

  def call(args: Vector[Value])(implicit idProvider: QuineIdProvider): Value = {
    val arr: Array[String] = args match {
      case Vector(Expr.Str(t), Expr.Str(r)) => t.split(r)
      case Vector(Expr.Str(t), Expr.Str(r), Expr.Integer(l)) => t.split(r, l.toInt)
      case other => throw wrongSignature(other)
    }
    Expr.List(arr.view.map(Expr.Str).toVector)
  }
}

object CypherTextRegexFirstMatch extends UserDefinedFunction {
  val name = "text.regexFirstMatch"
  val isPure = true
  val signatures: Vector[UserDefinedFunctionSignature] = Vector(
    UserDefinedFunctionSignature(
      arguments = Vector("text" -> Type.Str, "regex" -> Type.Str),
      output = Type.List(Type.Str),
      description =
        "Parses the string `text` using the regular expression `regex` and returns the first set of capture group matches"
    )
  )
  val category = Category.STRING

  def call(args: Vector[Value])(implicit idProvider: QuineIdProvider): Value =
    args match {
      case Vector(Expr.Str(text), Expr.Str(regex)) =>
        Expr.List(
          for {
            m <- regex.r.findFirstMatchIn(text).toVector
            i <- 0 to m.groupCount
          } yield Expr.Str(m.group(i))
        )
      case other => throw wrongSignature(other)
    }
}

object CypherTextUrlEncode extends UserDefinedFunction {
  val name = "text.urlencode"
  val isPure = true
  val signatures: Vector[UserDefinedFunctionSignature] = Vector(
    UserDefinedFunctionSignature(
      arguments = Vector("text" -> Type.Str),
      output = Type.List(Type.Str),
      description = "URL-encodes (RFC 3986) the provided string, "
    ),
    UserDefinedFunctionSignature(
      arguments = Vector("text" -> Type.Str, "usePlusForSpace" -> Type.Bool),
      output = Type.List(Type.Str),
      description = "URL-encodes (RFC 3986) the provided string, optionally using `+` for spaces instead"
    )
  )
  val category = Category.STRING

  /** @see <https://datatracker.ietf.org/doc/html/rfc3986#section-2.2>
    */
  private val rfcReservedChars: Array[Byte] =
    Array(':', '/', '?', '#', '[', ']', '@', '!', '$', '&', '\'', '(', ')', '*', '+', ',', ';', '=').map(_.toByte)

  def call(args: Vector[Value])(implicit idProvider: QuineIdProvider): Value = {
    val (str, usePlus) = args match {
      case Vector(Expr.Str(str)) => str -> false
      case Vector(Expr.Str(str), Expr.Bool(usePlus)) => str -> usePlus
      case other =>
        throw wrongSignature(other)
    }

    if (usePlus) {
      val encodedBytes = new PercentCodec(rfcReservedChars, true).encode(str.getBytes(StandardCharsets.UTF_8))
      Expr.Str(new String(encodedBytes, StandardCharsets.US_ASCII))
    } else {
      val encodedBytes =
        new PercentCodec(rfcReservedChars :+ ' '.toByte, false).encode(str.getBytes(StandardCharsets.UTF_8))
      Expr.Str(new String(encodedBytes, StandardCharsets.US_ASCII))
    }

  }
}

object CypherTextUrlDecode extends UserDefinedFunction with LazyLogging {
  val name = "text.urldecode"
  val isPure = true
  val signatures: Vector[UserDefinedFunctionSignature] = Vector(
    UserDefinedFunctionSignature(
      arguments = Vector("text" -> Type.Str),
      output = Type.List(Type.Str),
      description = "URL-decodes (x-www-form-urlencoded) the provided string"
    ),
    UserDefinedFunctionSignature(
      arguments = Vector("text" -> Type.Str, "decodePlusAsSpace" -> Type.Bool),
      output = Type.List(Type.Str),
      description = "URL-decodes the provided string, using RFC3986 if decodePlusAsSpace = false"
    )
  )
  val category = Category.STRING

  def call(args: Vector[Value])(implicit idProvider: QuineIdProvider): Value = {
    val (str, strictRfc3986) = args match {
      case Vector(Expr.Str(str)) => str -> false
      case Vector(Expr.Str(str), Expr.Bool(shouldDecodePlus)) => str -> !shouldDecodePlus
      case other =>
        throw wrongSignature(other)
    }
    if (strictRfc3986) {
      try {
        val decodedBytes = new PercentCodec().decode(str.getBytes(StandardCharsets.UTF_8))
        Expr.Str(new String(decodedBytes, StandardCharsets.UTF_8))
      } catch {
        case err: DecoderException =>
          logger.info(s"""$name unable to URL-decode provided string: "$str"""", err)
          Expr.Null
      }
    } else {
      try Expr.Str(java.net.URLDecoder.decode(str, StandardCharsets.UTF_8))
      catch {
        case err: IllegalArgumentException =>
          logger.info(s"""$name unable to URL-decode provided string: "$str"""", err)
          Expr.Null
      }
    }
  }
}

object CypherDateTime extends UserDefinedFunction {
  val name = "datetime"
  val isPure = false // reads system time and zone
  val signatures: Seq[UserDefinedFunctionSignature] = Vector(
    UserDefinedFunctionSignature(
      arguments = Vector(),
      output = Type.DateTime,
      description = "Get the current date time"
    ),
    UserDefinedFunctionSignature(
      arguments = Vector("options" -> Type.Map),
      output = Type.DateTime,
      description = "Construct a date time from the options"
    ),
    UserDefinedFunctionSignature(
      arguments = Vector("datetime" -> Type.Str),
      output = Type.DateTime,
      description = "Parse a date time from a string"
    ),
    UserDefinedFunctionSignature(
      arguments = Vector("datetime" -> Type.Str, "format" -> Type.Str),
      output = Type.DateTime,
      description = "Parse a local date time from a string using a custom format"
    )
  )
  val category = Category.TEMPORAL

  private[cypher] val unitFields: List[(String, TemporalField)] =
    Expr.temporalFields.toList
      .sortBy(_._2.getRangeUnit.getDuration)
      .reverse

  private[cypher] def getBaseDate(option: Value): Either[JavaLocalDateTime, JavaZonedDateTime] =
    option match {
      case Expr.LocalDateTime(d) => Left(d)
      case Expr.DateTime(d) => Right(d)
      case other =>
        throw CypherException.TypeMismatch(
          Seq(Type.LocalDateTime, Type.DateTime),
          other,
          "`date` field in options map"
        )
    }

  private[cypher] def getTimeZone(option: Value): ZoneId =
    option match {
      case Expr.Str(tz) => TimeZone.getTimeZone(tz).toZoneId
      case other =>
        throw CypherException.TypeMismatch(
          Seq(Type.Str),
          other,
          "`timezone` field in options map"
        )
    }

  def call(args: Vector[Value])(implicit idp: QuineIdProvider): Value =
    args match {
      case Vector() => Expr.DateTime(JavaZonedDateTime.now())

      case Vector(Expr.Map(optionsMap)) =>
        val remainingOptions = scala.collection.mutable.Map(optionsMap.toSeq: _*)
        val timeZone = remainingOptions.remove("timezone").map(getTimeZone)
        val defaultedZone = timeZone.getOrElse(TimeZone.getDefault.toZoneId)
        val baseDate = remainingOptions.remove("date").map(getBaseDate)

        val initialZonedDateTime: JavaZonedDateTime = (baseDate, timeZone) match {
          case (Some(Left(localDateTime)), _) => JavaZonedDateTime.of(localDateTime, defaultedZone)
          case (Some(Right(zonedDateTime)), None) => zonedDateTime
          case (Some(Right(zonedDateTime)), Some(zone)) => zonedDateTime.withZoneSameInstant(zone)

          // When passing no arguments or just a timezone argument, use the current time
          case (None, _) if remainingOptions.isEmpty => JavaZonedDateTime.now(defaultedZone)

          // When passing other arguments, start at the absolute offset of Jan 1, 0000
          case (None, _) => JavaZonedDateTime.of(0, 1, 1, 0, 0, 0, 0, defaultedZone)
        }

        // TODO: consider detecting non-sensical combinations of units
        val zonedDateTime = CypherDateTime.unitFields.foldLeft(initialZonedDateTime) {
          case (accDateTime, (unitFieldName, temporalField)) =>
            remainingOptions.remove(unitFieldName) match {
              case None => accDateTime
              case Some(Expr.Integer(unitValue)) => accDateTime.`with`(temporalField, unitValue)
              case Some(other) =>
                throw CypherException.TypeMismatch(
                  Seq(Type.Integer),
                  other,
                  s"`$unitFieldName` field in options map"
                )
            }
        }

        // Disallow unknown fields
        if (remainingOptions.nonEmpty) {
          throw CypherException.Runtime(
            "Unknown fields in options map: " + remainingOptions.keys.mkString("`", "`, `", "`")
          )
        }

        Expr.DateTime(zonedDateTime)

      // TODO, support more formats here...
      case Vector(Expr.Str(temporalValue)) =>
        Expr.DateTime(JavaZonedDateTime.parse(temporalValue))

      case Vector(Expr.Str(temporalValue), Expr.Str(format)) =>
        val formatter = DateTimeFormatter.ofPattern(format, Locale.US)
        Expr.DateTime(JavaZonedDateTime.parse(temporalValue, formatter))

      case other => throw wrongSignature(other)
    }
}

object CypherLocalDateTime extends UserDefinedFunction {
  val name = "localdatetime"
  val isPure = false // reads system time and zone
  val signatures: Seq[UserDefinedFunctionSignature] = Vector(
    UserDefinedFunctionSignature(
      arguments = Vector(),
      output = Type.LocalDateTime,
      description = "Get the current local date time"
    ),
    UserDefinedFunctionSignature(
      arguments = Vector("options" -> Type.Map),
      output = Type.LocalDateTime,
      description = "Construct a local date time from the options"
    ),
    UserDefinedFunctionSignature(
      arguments = Vector("datetime" -> Type.Str),
      output = Type.LocalDateTime,
      description = "Parse a local date time from a string"
    ),
    UserDefinedFunctionSignature(
      arguments = Vector("datetime" -> Type.Str, "format" -> Type.Str),
      output = Type.LocalDateTime,
      description = "Parse a local date time from a string using a custom format"
    )
  )
  val category = Category.TEMPORAL

  def call(args: Vector[Value])(implicit idp: QuineIdProvider): Value =
    args match {
      case Vector() => Expr.LocalDateTime(JavaLocalDateTime.now())

      case Vector(Expr.Map(optionsMap)) =>
        val remainingOptions = scala.collection.mutable.Map(optionsMap.toSeq: _*)
        val timeZone = remainingOptions.remove("timezone").map(CypherDateTime.getTimeZone)
        if (timeZone.nonEmpty && remainingOptions.nonEmpty) {
          throw CypherException.Runtime("when specified, `timezone` must be the only option")
        }
        val baseDate = remainingOptions.remove("date").map(CypherDateTime.getBaseDate)

        val initialLocalDateTime: JavaLocalDateTime = (baseDate, timeZone) match {
          case (Some(Left(dateTime)), None) => dateTime
          case (Some(Right(zonedDateTime)), None) => zonedDateTime.toLocalDateTime
          case (None, Some(tz)) => JavaLocalDateTime.now(tz)

          // When passing no arguments or just a timezone argument, use the current time
          case (None, None) if remainingOptions.isEmpty => JavaLocalDateTime.now

          // When passing other arguments, start at the absolute offset of Jan 1, 0000
          case (None, None) => JavaLocalDateTime.of(0, 1, 1, 0, 0)
        }

        // TODO: consider detecting non-sensical combinations of units
        val localDateTime = CypherDateTime.unitFields.foldLeft(initialLocalDateTime) {
          case (accDateTime, (unitFieldName, temporalField)) =>
            remainingOptions.remove(unitFieldName) match {
              case None => accDateTime
              case Some(Expr.Integer(unitValue)) => accDateTime.`with`(temporalField, unitValue)
              case Some(other) =>
                throw CypherException.TypeMismatch(
                  Seq(Type.Integer),
                  other,
                  s"`$unitFieldName` field in options map"
                )
            }
        }

        // Disallow unknown fields
        if (remainingOptions.nonEmpty) {
          throw CypherException.Runtime(
            "Unknown fields in options map: " + remainingOptions.keys.mkString("`", "`, `", "`")
          )
        }

        Expr.LocalDateTime(localDateTime)

      // TODO, support more formats here...
      case Vector(Expr.Str(temporalValue)) =>
        Expr.LocalDateTime(JavaLocalDateTime.parse(temporalValue))

      case Vector(Expr.Str(temporalValue), Expr.Str(format)) =>
        val formatter = DateTimeFormatter.ofPattern(format, Locale.US)
        Expr.LocalDateTime(JavaLocalDateTime.parse(temporalValue, formatter))

      case other => throw wrongSignature(other)
    }
}

object CypherDuration extends UserDefinedFunction {
  val name = "duration"
  val isPure = true
  val signatures: Seq[UserDefinedFunctionSignature] = Vector(
    UserDefinedFunctionSignature(
      arguments = Vector("options" -> Type.Map),
      output = Type.Duration,
      description = "Construct a duration from the options"
    ),
    UserDefinedFunctionSignature(
      arguments = Vector("duration" -> Type.Str),
      output = Type.Duration,
      description = "Parse a duration from a string"
    )
  )
  val category = Category.TEMPORAL

  def call(args: Vector[Value])(implicit idp: QuineIdProvider): Value =
    args match {
      case Vector(Expr.Map(optionsMap)) =>
        var duration: JavaDuration = JavaDuration.ZERO

        for ((unitFieldName, value) <- optionsMap) {
          val unitQuantity: Long = value match {
            case Expr.Integer(unitValue) => unitValue
            case other =>
              throw CypherException.TypeMismatch(
                Seq(Type.Integer),
                other,
                s"`$unitFieldName` field in options map"
              )
          }
          val unit = Expr.temporalUnits.getOrElse(
            unitFieldName,
            throw CypherException.Runtime(s"Unknown field in options map: `$unitFieldName`")
          )
          duration = duration.plus(unitQuantity, unit)
        }

        Expr.Duration(duration)

      case Vector(Expr.Str(durationValue)) =>
        Expr.Duration(JavaDuration.parse(durationValue))

      case other => throw wrongSignature(other)
    }
}

object CypherDurationBetween extends UserDefinedFunction {
  val name = "duration.between"
  val isPure = true
  def signatures: Seq[UserDefinedFunctionSignature] = Vector(
    UserDefinedFunctionSignature(
      arguments = Vector("date1" -> Type.LocalDateTime, "date2" -> Type.LocalDateTime),
      output = Type.Duration,
      description = "Compute the duration between two local dates"
    ),
    UserDefinedFunctionSignature(
      arguments = Vector("date1" -> Type.DateTime, "date2" -> Type.DateTime),
      output = Type.Duration,
      description = "Compute the duration between two dates"
    )
  )
  val category = Category.TEMPORAL

  def call(args: Vector[Value])(implicit idp: QuineIdProvider): Value =
    args match {
      case Vector(Expr.LocalDateTime(d1), Expr.LocalDateTime(d2)) =>
        Expr.Duration(JavaDuration.between(d1, d2))

      case Vector(Expr.DateTime(d1), Expr.DateTime(d2)) =>
        Expr.Duration(JavaDuration.between(d1, d2))

      case other => throw wrongSignature(other)
    }
}

object CypherFormatTemporal extends UserDefinedFunction {
  val name = "temporal.format"
  val isPure = true
  val signatures: Seq[UserDefinedFunctionSignature] = Vector(
    UserDefinedFunctionSignature(
      arguments = Vector("date" -> Type.DateTime, "format" -> Type.Str),
      output = Type.Str,
      description = "Convert date time into string"
    ),
    UserDefinedFunctionSignature(
      arguments = Vector("date" -> Type.LocalDateTime, "format" -> Type.Str),
      output = Type.Str,
      description = "Convert local date time into string"
    )
  )
  val category = Category.TEMPORAL

  def call(args: Vector[Value])(implicit idp: QuineIdProvider): Value =
    args match {
      case Vector(Expr.LocalDateTime(date), Expr.Str(format)) =>
        val formatter = DateTimeFormatter.ofPattern(format, Locale.US)
        Expr.Str(date.format(formatter))

      case Vector(Expr.DateTime(date), Expr.Str(format)) =>
        val formatter = DateTimeFormatter.ofPattern(format, Locale.US)
        Expr.Str(date.format(formatter))

      case other => throw wrongSignature(other)
    }
}

// Behaviour of `RETURN coll.max(xs)` is consistent with `UNWIND xs AS x RETURN max(x)`
object CypherCollMax extends UserDefinedFunction {
  val name = "coll.max"
  val isPure = true
  val signatures: Seq[UserDefinedFunctionSignature] = Vector.tabulate(16) { (i: Int) =>
    if (i == 0) {
      UserDefinedFunctionSignature(
        arguments = Vector("value" -> Type.ListOfAnything),
        output = Type.Anything,
        description = "Computes the maximum of values in a list"
      )
    } else {
      // These are not provided by APOC
      UserDefinedFunctionSignature(
        arguments = Vector.tabulate(i)(j => s"input$j" -> Type.Anything),
        output = Type.Anything,
        description = "Computes the maximum argument"
      )
    }
  }
  val category = Category.LIST

  def call(args: Vector[Value])(implicit idp: QuineIdProvider): Value = {
    val inputs = args match {
      case Vector(Expr.List(values)) => values
      case other => other
    }
    if (inputs.isEmpty) Expr.Null else inputs.max(Value.ordering)
  }
}

// Behaviour of `RETURN coll.min(xs)` is consistent with `UNWIND xs AS x RETURN min(x)`
object CypherCollMin extends UserDefinedFunction {
  val name = "coll.min"
  val isPure = true
  val signatures: Seq[UserDefinedFunctionSignature] = Vector.tabulate(16) { (i: Int) =>
    if (i == 0) {
      UserDefinedFunctionSignature(
        arguments = Vector("value" -> Type.ListOfAnything),
        output = Type.Anything,
        description = "Computes the minimum of values in a list"
      )
    } else {
      // These are not provided by APOC
      UserDefinedFunctionSignature(
        arguments = Vector.tabulate(i)(j => s"input$j" -> Type.Anything),
        output = Type.Anything,
        description = "Computes the minimum argument"
      )
    }
  }
  val category = Category.LIST

  def call(args: Vector[Value])(implicit idp: QuineIdProvider): Value = {
    val inputs = args match {
      case Vector(Expr.List(values)) => values
      case other => other
    }
    if (inputs.isEmpty) Expr.Null else inputs.min(Value.ordering)
  }
}

object CypherMetaType extends UserDefinedFunction {
  val name = "meta.type"
  val isPure = true
  val signatures: Seq[UserDefinedFunctionSignature] = Vector(
    UserDefinedFunctionSignature(
      arguments = Vector("value" -> Type.Anything),
      output = Type.Str,
      description = "Inspect the (name of the) type of a value"
    )
  )
  val category = Category.SCALAR

  def call(args: Vector[Value])(implicit idp: QuineIdProvider): Value =
    args match {
      case Vector(v) => Expr.Str(v.typ.pretty)
      case other => throw wrongSignature(other)
    }
}

class CypherValueGen(outputType: Type, defaultSize: Long, randGen: (Long) => Value) extends UserDefinedFunction {
  val name: String = s"gen.${outputType.pretty.toLowerCase}"
  val category: String = Category.SCALAR
  val isPure: Boolean = false

  def call(arguments: Vector[Value])(implicit idProvider: QuineIdProvider): Value = {
    val size = arguments match {
      case Seq() => defaultSize
      case Seq(Expr.Integer(i)) => i
      case args => throw wrongSignature(args)
    }
    randGen(size)
  }

  val signatures: Seq[UserDefinedFunctionSignature] = {
    val sig = UserDefinedFunctionSignature(
      arguments = Vector("size" -> Type.Integer),
      output = outputType,
      description = s"Randomly generate a ${outputType.pretty.toLowerCase}."
    )
    Seq(sig.copy(arguments = Vector.empty), sig)
  }
}

class CypherValueGenFrom(outputType: Type, defaultSize: Long, randGen: (Long, Long) => Value)
    extends UserDefinedFunction {
  override val name: String = s"gen.${outputType.pretty.toLowerCase}.from"
  val category: String = Category.SCALAR
  override val isPure: Boolean = true

  override def call(arguments: Vector[Value])(implicit idProvider: QuineIdProvider): Value = {
    val (hash, size) = arguments match {
      case Seq(v) => v.hash.asLong() -> defaultSize
      case Seq(v, Expr.Integer(i)) => v.hash.asLong() -> i
      case args => throw wrongSignature(args)
    }
    randGen(hash, size)
  }

  override val signatures: Seq[UserDefinedFunctionSignature] = {
    val sig = UserDefinedFunctionSignature(
      arguments = Vector("fromValue" -> Type.Anything, "withSize" -> Type.Integer),
      output = outputType,
      description = s"Deterministically generate a random ${outputType.pretty.toLowerCase} from the provided input."
    )
    Seq(sig.copy(arguments = sig.arguments.dropRight(1)), sig)
  }
}

object CypherGenFroms {
  private def bytes(hash: Long, size: Int): Array[Byte] = {
    val b = Array.ofDim[Byte](size)
    new Random(hash).nextBytes(b)
    b
  }
  val all: List[CypherValueGenFrom] = List(
    new CypherValueGenFrom(
      Type.Str,
      8L,
      (hash: Long, size: Long) => Expr.Str(new Random(hash).alphanumeric.take(size.toInt).mkString)
    ),
    new CypherValueGenFrom(
      Type.Integer,
      Int.MaxValue,
      (hash: Long, size: Long) => Expr.Integer(new Random(hash).nextLong() % size) // Tolerating mod bias.
    ),
    new CypherValueGenFrom(
      Type.Floating,
      1L,
      (hash: Long, size: Long) => Expr.Floating(new Random(hash).nextDouble() * size)
    ),
    new CypherValueGenFrom(Type.Bool, 1L, (hash: Long, size: Long) => Expr.Bool(new Random(hash).nextBoolean())),
    new CypherValueGenFrom(Type.Bytes, 12L, (hash: Long, size: Long) => Expr.Bytes(bytes(hash, size.toInt))),
    new CypherValueGenFrom(
      Type.Node,
      0L,
      (hash: Long, size: Long) => Expr.Node(QuineId(Array.emptyByteArray), Set.empty, Map.empty)
    ) {
      override def call(arguments: Vector[Value])(implicit idProvider: QuineIdProvider): Value = {
        val size = Try(arguments(1).asLong("").toInt).getOrElse(4)
        val rand = new Random(arguments.head.hash.asLong())
        val props = (0 until size)
          .map(i => Symbol(i.toString) -> Expr.Str(rand.alphanumeric.take(size * 2).mkString))
          .toMap
        Expr.Node(idFrom(arguments.head), Set.empty, props)
      }
    }
  )
}
