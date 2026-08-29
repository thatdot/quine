package com.thatdot.quine.graph

import java.util.Base64

import scala.io.Source
import scala.util.Using

import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.graph.behavior.MultipleValuesStandingQueryPartSubscription
import com.thatdot.quine.graph.cypher.{
  AllPropertiesState,
  CrossState,
  EdgeSubscriptionReciprocalState,
  FilterMapState,
  LabelsState,
  LocalIdState,
  LocalPropertyState,
  MultipleValuesStandingQueryState,
  QueryContext,
  SubscribeAcrossEdgeState,
  UnitState,
  Value,
}
import com.thatdot.quine.graph.messaging.StandingQueryMessage.MultipleValuesStandingQuerySubscriber
import com.thatdot.quine.model.HalfEdge

/** The checked-in corpus of standing query states as serialized by an earlier version of
  * `MultipleValuesStandingQueryStateCodec`, and the canonical rendering used to state what those bytes mean.
  *
  * The corpus exists because a codec can only produce bytes in its current format: once the format changes, "new code
  * reads state written by old code" is no longer expressible as a round trip, and a round trip through new code can
  * hide a read that misinterprets old bytes symmetrically with the write that produced them. So the bytes are captured
  * once, from the format in use at capture time, and kept.
  *
  * Each fixture pairs those bytes with [[describe]] of the value they decoded to when captured. A deliberate format
  * change is expected to show up here as a diff in the descriptions, reviewed as part of the change; what must never
  * happen is old bytes silently decoding to something different.
  *
  * Regenerate (only when adding cases, never to make a failing comparison pass):
  * {{{
  * sbt "quine-core/Test/runMain com.thatdot.quine.graph.MultipleValuesStandingQueryStateFixtureGenerator"
  * }}}
  */
object MultipleValuesStandingQueryStateFixtures {

  val resourcePath: String = "/golden/multiple-values-standing-query-states.txt"

  /** Path used by the generator, relative to the root of the repository. */
  val sourcePath: String = "public/quine-core/src/test/resources/golden/multiple-values-standing-query-states.txt"

  /** A correlated scenario, being reciprocal states keyed the way an earlier funnel keyed them plus the state they
    * subscribe to, kept apart from the general corpus so a test of legacy-state handling can name its inputs.
    * See [[LegacyReciprocalFoldFixtures]] for the scenario's construction.
    */
  val foldResourcePath: String = "/golden/legacy-reciprocal-fold.txt"

  /** Path used by the generator for [[foldResourcePath]], relative to the root of the repository. */
  val foldSourcePath: String = "public/quine-core/src/test/resources/golden/legacy-reciprocal-fold.txt"

  final case class Fixture(description: String, bytes: Array[Byte])

  /** Every state the codec can write. A state absent from the corpus is a state whose old bytes nothing can read
    * back, so both the generator and the golden test fail on an omission rather than quietly covering less.
    */
  val expectedKinds: Seq[String] = Seq(
    "unit",
    "cross",
    "localProperty",
    "allProperties",
    "localId",
    "labels",
    "subscribeAcrossEdge",
    "reciprocal",
    "filterMap",
  )

  def kindOf(state: MultipleValuesStandingQueryState): String = renderState(state).takeWhile(_ != '(')

  def load(path: String = resourcePath): Seq[Fixture] = {
    val stream = getClass.getResourceAsStream(path)
    require(stream != null, s"Missing golden fixture resource $path")
    val lines = Using.resource(Source.fromInputStream(stream, "UTF-8"))(_.getLines().toVector)
    lines.filter(line => line.nonEmpty && !line.startsWith("#")).map { line =>
      line.split('\t') match {
        case Array(description, encoded) => Fixture(description, Base64.getDecoder.decode(encoded))
        case _ => throw new IllegalArgumentException(s"Malformed golden fixture line: $line")
      }
    }
  }

  def renderFile(fixtures: Seq[Fixture], header: Seq[String]): String = {
    val body = fixtures.map(f => s"${f.description}\t${Base64.getEncoder.encodeToString(f.bytes)}")
    (header.map("# " + _) ++ body).mkString("", "\n", "\n")
  }

  /** Canonical, order-independent rendering of everything the codec is responsible for carrying.
    *
    * Anything a reader must recover from the bytes belongs here; anything not rendered here is not checked by the
    * golden comparison. Mutable collections are sorted so that the rendering does not depend on iteration order, but
    * result rows keep their order, which the encoding is expected to preserve.
    */
  def describe(
    subscription: MultipleValuesStandingQueryPartSubscription,
    state: MultipleValuesStandingQueryState,
  ): String = {
    val subscribers = subscription.subscribers.toSeq.map(renderSubscriber).sorted.mkString("[", ",", "]")
    val head =
      s"subscription(forQuery=${renderPartId(subscription.forQuery)}," +
      s"global=${subscription.globalId.uuid},subscribers=$subscribers)"
    escapeControlCharacters(s"$head ${renderState(state)}")
  }

  private def renderState(state: MultipleValuesStandingQueryState): String = state match {
    case s: UnitState => s"unit(${renderPartId(s.queryPartId)})"
    case s: CrossState =>
      val accumulated = s.resultsAccumulator.toSeq
        .map { case (partId, rows) => s"${renderPartId(partId)}->${renderMaybeRows(rows)}" }
        .sorted
        .mkString("[", ",", "]")
      s"cross(${renderPartId(s.queryPartId)},accumulated=$accumulated)"
    case s: LocalPropertyState => s"localProperty(${renderPartId(s.queryPartId)})"
    case s: AllPropertiesState => s"allProperties(${renderPartId(s.queryPartId)})"
    case s: LocalIdState => s"localId(${renderPartId(s.queryPartId)})"
    case s: LabelsState => s"labels(${renderPartId(s.queryPartId)})"
    case s: SubscribeAcrossEdgeState =>
      val edges = s.contributionStore.entries.toSeq
        .map { case (halfEdge, rows) => s"${renderHalfEdge(halfEdge)}->${renderMaybeRows(rows)}" }
        .sorted
        .mkString("[", ",", "]")
      s"subscribeAcrossEdge(${renderPartId(s.queryPartId)},edges=$edges)"
    case s: EdgeSubscriptionReciprocalState =>
      s"reciprocal(${renderPartId(s.queryPartId)},halfEdge=${renderHalfEdge(s.halfEdge)}," +
        s"andThen=${renderPartId(s.andThenId)},cached=${renderMaybeRows(s.cachedResult)})"
    case s: FilterMapState =>
      s"filterMap(${renderPartId(s.queryPartId)},kept=${renderMaybeRows(s.keptResults)})"
  }

  private def renderSubscriber(subscriber: MultipleValuesStandingQuerySubscriber): String = subscriber match {
    case MultipleValuesStandingQuerySubscriber.NodeSubscriber(onNode, globalId, queryId) =>
      s"node(${renderQid(onNode)},${globalId.uuid},${renderPartId(queryId)})"
    case MultipleValuesStandingQuerySubscriber.GlobalSubscriber(globalId) =>
      s"global(${globalId.uuid})"
  }

  private def renderPartId(partId: MultipleValuesStandingQueryPartId): String = partId.uuid.toString

  private def renderQid(qid: QuineId): String = qid.toInternalString

  private def renderHalfEdge(halfEdge: HalfEdge): String =
    s"${halfEdge.edgeType.name}/${halfEdge.direction}/${renderQid(halfEdge.other)}"

  private def renderMaybeRows(rows: Option[Seq[QueryContext]]): String =
    rows.fold("unanswered")(_.map(renderRow).mkString("[", ",", "]"))

  private def renderRow(row: QueryContext): String =
    row.environment.toSeq
      .sortBy(_._1.name)
      .map { case (column, value) => s"${column.name}=${renderValue(value)}" }
      .mkString("{", ",", "}")

  private def renderValue(value: Value): String = value.pretty

  /** Keeps a description on one line, which is what makes the file readable as a diff. Applied to both the captured
    * and the recomputed description, so it cannot affect whether they compare equal.
    */
  private def escapeControlCharacters(description: String): String =
    description
      .replace("\\", "\\\\")
      .replace("\t", "\\t")
      .replace("\r", "\\r")
      .replace("\n", "\\n")
}
