package com.thatdot.quine.graph.cypher

import scala.collection.compat._
import scala.concurrent.Future
import scala.util.Try

import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.Timeout

import cats.implicits._

import com.thatdot.quine.graph.LiteralOpsGraph
import com.thatdot.quine.model.{Milliseconds, QuineId, QuineIdProvider}

/** Cypher user defined procedures (UDP) must extend this class
  *
  * @note instances of [[UserDefinedProcedure]] may be re-used for multiple
  * (possibly concurrent) function calls
  */
trait UserDefinedProcedure {

  /** What is the name of the UDP */
  def name: String

  /** Can this mutate the graph (or will it just be reading data) */
  def canContainUpdates: Boolean

  /** Is the procedure idempotent? See [[Query]] for full comment. */
  def isIdempotent: Boolean

  /** Can the procedure cause a full node scan? */
  def canContainAllNodeScan: Boolean

  /** How to call the UDP
    *
    * @note each vector in the output must have the size equal to `outputColumns`
    *
    * @param context variables at the point the UDP is called
    * @param arguments arguments passed into the UDP (after they've been evaluated)
    * @param location where is the query at when the procedure is invoked?
    * @return output rows of the UDP
    */
  def call(
    context: QueryContext,
    arguments: Seq[Value],
    location: ProcedureExecutionLocation
  )(implicit
    parameters: Parameters,
    timeout: Timeout
  ): Source[Vector[Value], _]

  /** Signature of the procedure */
  def signature: UserDefinedProcedureSignature

  final lazy val outputColumns: Columns.Specified = Columns.Specified(
    signature.outputs.view.map { case (outputName, _) => Symbol(outputName) }.toVector
  )

  /** Construct a wrong signature error based on [[signatures]]
    *
    * @param actualArguments actual arguments received
    * @return exception representing the mismatch
    */
  final protected def wrongSignature(actualArguments: Seq[Value]): CypherException.WrongSignature =
    CypherException.WrongSignature(signature.pretty(name), actualArguments, None)
}
object UserDefinedProcedure {

  /** Fetch information needed to build up a Cypher node from a node and time
    *
    * @param qid node ID
    * @param atTime moment in time to query
    * @param graph graph
    * @return Cypher-compatible representation of the node
    */
  def getAsCypherNode(qid: QuineId, atTime: Option[Milliseconds], graph: LiteralOpsGraph)(implicit
    timeout: Timeout
  ): Future[Expr.Node] =
    graph.literalOps
      .getPropsAndLabels(qid, atTime)
      .map { case (props, labels) =>
        Expr.Node(
          qid,
          labels.getOrElse(Set.empty),
          props.fmap(pv => Expr.fromQuineValue(pv.deserialized.get))
        )
      }(graph.nodeDispatcherEC)

  /** Extract from the Cypher value an ID
    *
    * @param value value from which to get ID
    * @param idProvider how IDs are encoded
    * @return ID if it could be extracted
    */
  def extractQuineId(value: Value)(implicit idProvider: QuineIdProvider): Option[QuineId] = {

    object ValueQid {
      def unapply(value: Value): Option[QuineId] = for {
        quineValue <- Try(Expr.toQuineValue(value)).toOption
        quineId <- idProvider.valueToQid(quineValue)
      } yield quineId
    }
    object StrQid {
      def unapply(value: Value): Option[QuineId] = value match {
        case Expr.Str(strId) => idProvider.qidFromPrettyString(strId).toOption
        case _ => None
      }
    }

    value match {
      case Expr.Node(qid, _, _) => Some(qid)
      case Expr.Bytes(id, _) => Some(QuineId(id))
      case ValueQid(qid) => Some(qid)
      case StrQid(qid) => Some(qid)
      case _ => None
    }
  }
}

/** Representation of a valid type for the procedure
  *
  * @param arguments name of the arguments and their type
  * @param outputs output columns
  * @param description explanation of what this UDP does
  */
final case class UserDefinedProcedureSignature(
  arguments: Seq[(String, Type)],
  outputs: Seq[(String, Type)],
  description: String
) {

  /** Pretty-print the signature
    *
    * @note this is defined to match the openCypher spec as much as possible
    */
  def pretty(name: String): String = {
    val outputsStr = if (outputs.isEmpty) {
      "VOID"
    } else {
      outputs.view
        .map { case (name, typ) => s"$name :: ${typ.pretty}" }
        .mkString("(", ", ", ")")
    }

    val inputsStr = arguments.view
      .map { case (name, typ) => s"$name :: ${typ.pretty}" }
      .mkString(", ")

    s"$name($inputsStr) :: $outputsStr"
  }
}
