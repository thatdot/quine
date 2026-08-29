package com.thatdot.quine.graph

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

import scala.collection.mutable

import org.scalacheck.Gen
import org.scalacheck.rng.Seed

import com.thatdot.quine.graph.MultipleValuesStandingQueryStateFixtures.Fixture
import com.thatdot.quine.graph.behavior.MultipleValuesStandingQueryPartSubscription
import com.thatdot.quine.graph.cypher.MultipleValuesStandingQueryState
import com.thatdot.quine.persistor.codecs.MultipleValuesStandingQueryStateCodec

/** Writes the golden fixture corpora read by [[MultipleValuesStandingQueryStateGoldenTest]], using whatever format
  * `MultipleValuesStandingQueryStateCodec` is in when it runs.
  *
  * Run from the root of the repository:
  * {{{
  * sbt "quine-core/Test/runMain com.thatdot.quine.graph.MultipleValuesStandingQueryStateFixtureGenerator"
  * }}}
  *
  * Rerunning this after a format change replaces old bytes with new ones, which is the one thing the corpus exists to
  * prevent. Run it to add coverage of a state that has no fixtures yet, or to capture a new format alongside the old
  * one under a new resource, not to make a failing comparison pass.
  */
object MultipleValuesStandingQueryStateFixtureGenerator extends ArbitraryInstances {

  /** How many fixtures to keep per kind of state. Enough to catch a field read at the wrong offset (which tends to
    * survive one arbitrary value and not four), few enough to review by eye.
    */
  private val casesPerKind: Int = 4

  /** Seeds are searched, not enumerated, because the generator picks the kind of state randomly. */
  private val seedsToSearch: Int = 2000

  /** Rewrites the descriptions of the existing corpora, leaving their bytes exactly as they are.
    *
    * This is the tool for a deliberate format change: it states, as a reviewable diff, what the bytes already on disk
    * now mean to the code that reads them. It cannot manufacture new old-format bytes, which is why it is separate
    * from generating fixtures.
    */
  private val refreshFlag: String = "--refresh-descriptions"

  def main(args: Array[String]): Unit = {
    val refreshOnly = args.contains(refreshFlag)
    val corpusHeader = Seq(
      "Standing query states serialized by MultipleValuesStandingQueryStateCodec, one per line, as",
      "<description>TAB<base64 of the serialized (subscription, state) pair>.",
      "",
      "Captured deliberately and kept: these are the only bytes in the old format the tests can ever have, because a",
      "codec can only write its current format. See MultipleValuesStandingQueryStateFixtures for the rendering, and",
      "MultipleValuesStandingQueryStateFixtureGenerator for how to add cases.",
    )
    val foldHeader = Seq(
      "Reciprocal states keyed per half edge, one per subscribing root, plus the andThen state whose subscriber",
      "set names them, serialized by MultipleValuesStandingQueryStateCodec. Same line format as the corpus beside",
      "this file.",
      "",
      "The part ids in these bytes could only be computed by the funnel in use at capture time; the current funnel",
      "keys a reciprocal by its constraints alone, so nothing can produce these ids again and the bytes are the",
      "record. Descriptions may be refreshed; the fixtures cannot be regenerated. See LegacyReciprocalFoldFixtures.",
    )
    write(
      Paths.get(MultipleValuesStandingQueryStateFixtures.sourcePath),
      if (refreshOnly) redescribe(MultipleValuesStandingQueryStateFixtures.resourcePath) else generate(),
      corpusHeader,
    )
    if (refreshOnly)
      write(
        Paths.get(MultipleValuesStandingQueryStateFixtures.foldSourcePath),
        redescribe(MultipleValuesStandingQueryStateFixtures.foldResourcePath),
        foldHeader,
      )
  }

  private def write(target: Path, fixtures: Seq[Fixture], header: Seq[String]): Unit = {
    Option(target.getParent).foreach(Files.createDirectories(_))
    Files.write(
      target,
      MultipleValuesStandingQueryStateFixtures.renderFile(fixtures, header).getBytes(StandardCharsets.UTF_8),
    )
    println(s"Wrote ${fixtures.size} fixtures to ${target.toAbsolutePath}")
  }

  def redescribe(resourcePath: String): Seq[Fixture] =
    MultipleValuesStandingQueryStateFixtures.load(resourcePath).map { fixture =>
      val (subscription, state) = MultipleValuesStandingQueryStateCodec.format.read(fixture.bytes).get
      fixture.copy(description = MultipleValuesStandingQueryStateFixtures.describe(subscription, state))
    }

  def generate(): Seq[Fixture] = {
    val byKind = mutable.LinkedHashMap.empty[String, Vector[Fixture]]
    def complete: Boolean = MultipleValuesStandingQueryStateFixtures.expectedKinds
      .forall(kind => byKind.get(kind).exists(_.size >= casesPerKind))
    var seedNumber = 1L
    while (seedNumber <= seedsToSearch && !complete) {
      val parameters = Gen.Parameters.default.withSize((seedNumber % 6L).toInt)
      val seed = Seed(seedNumber)
      val subscription: MultipleValuesStandingQueryPartSubscription =
        arbStandingQueryPartSubscription.arbitrary.pureApply(parameters, seed)
      val state: MultipleValuesStandingQueryState =
        arbStandingQueryState.arbitrary.pureApply(parameters, seed.next)
      val kind = MultipleValuesStandingQueryStateFixtures.kindOf(state)
      val kept = byKind.getOrElse(kind, Vector.empty)
      if (kept.size < casesPerKind) {
        val bytes = MultipleValuesStandingQueryStateCodec.format.write(subscription -> state)
        // Describe what the bytes decode to rather than what was generated: a fixture records what a reader must
        // recover from those bytes, including anything the format does not carry.
        val (decodedSubscription, decodedState) = MultipleValuesStandingQueryStateCodec.format.read(bytes).get
        val description = MultipleValuesStandingQueryStateFixtures.describe(decodedSubscription, decodedState)
        byKind += kind -> (kept :+ Fixture(description, bytes))
      }
      seedNumber += 1
    }
    val missing = MultipleValuesStandingQueryStateFixtures.expectedKinds.filterNot(byKind.contains)
    require(missing.isEmpty, s"No arbitrary instance produced: ${missing.mkString(", ")}")
    byKind.toSeq.sortBy(_._1).flatMap(_._2)
  }
}
