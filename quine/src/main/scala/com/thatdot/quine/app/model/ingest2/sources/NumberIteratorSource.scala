package com.thatdot.quine.app.model.ingest2.sources

import scala.util.{Success, Try}

import org.apache.pekko.stream.scaladsl.Source

import com.thatdot.data.DataFoldableFrom
import com.thatdot.quine.app.ShutdownSwitch
import com.thatdot.quine.app.data.QuineDataFoldablesFrom
import com.thatdot.quine.app.model.ingest2.source.{DecodedSource, IngestBounds}
import com.thatdot.quine.app.routes.IngestMeter
import com.thatdot.quine.graph.cypher.{Expr, Value, Value => CypherValue}

case class NumberIteratorSource(
  bounds: IngestBounds = IngestBounds(),
  ingestMeter: IngestMeter,
) {

  def decodedSource: DecodedSource = new DecodedSource(ingestMeter) {
    type Decoded = CypherValue
    type Frame = CypherValue
    override val foldable: DataFoldableFrom[Value] = QuineDataFoldablesFrom.cypherValueDataFoldable

    def stream: Source[(Try[CypherValue], CypherValue), ShutdownSwitch] = {

      val sourceBase = Source.unfold(bounds.startAtOffset)(ln => Some(ln + 1 -> Expr.Integer(ln)))

      val bounded = bounds.ingestLimit.fold(sourceBase)(limit => sourceBase.take(limit))

      withKillSwitches(
        bounded
          .via(metered[Value](meter, _ => 1)) //TODO this counts values not bytes
          .map(sum => (Success(sum), sum)),
      )
    }
  }
}
