package com.thatdot.quine.webapp.components.dashboard

import scala.scalajs.js
import scala.util.matching.Regex

import com.raquo.laminar.api.L._

import com.thatdot.quine.routes.Counter
import com.thatdot.quine.webapp.components.ManualHistogramPlot

object CounterSummaryCard {

  /** Bucket labels are of the form "some.stuff-to-ignore.histogramName.x-y", where x and y are integers (or y may be "infinity").
    * We want to sort by x asc. We capture:
    *  - full histogram name (some.stuff-to-ignore.histogramName)
    *  - histogram name (histogramName)
    *  - x
    *  - y
    */
  val BucketLabel: Regex =
    new Regex(
      raw"(?:((?:.*\.)*(.*))\.)?(\d+)-(\d+|infinity)",
      "fullHistogramName",
      "histogramName",
      "x",
      "y",
    )

  val bucketLabelOrdering: Ordering[String] = Ordering.by[String, Option[Int]] {
    case BucketLabel(_, _, x, _) => Some(x.toInt)
    case unexpectedLabel =>
      org.scalajs.dom.console.warn(s"Got an unexpected bucket label: $unexpectedLabel")
      None
  }

  def apply(
    name: String,
    counters: Seq[Counter],
  ): HtmlElement = {
    val countersMap: Map[String, Double] =
      counters.collect { case Counter(BucketLabel(_, _, x, y), count) =>
        s"$x-$y" -> count.toDouble
      }.toMap

    Card(
      title = name,
      body = ManualHistogramPlot(
        buckets = countersMap,
        layout = js.Dynamic.literal(
          height = 300,
          margin = js.Dynamic.literal(
            t = 32,
            b = 32,
            l = 32,
            r = 64,
          ),
        ),
        sortBucketsBy = bucketLabelOrdering,
      ),
    )
  }
}
