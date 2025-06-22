package com.thatdot.quine.app.model.ingest2.sources

import java.nio.ByteBuffer

import scala.util.{Success, Try}

import org.apache.pekko.stream.scaladsl.Source

import com.thatdot.data.{DataFoldableFrom, DataFolderTo}
import com.thatdot.quine.app.ShutdownSwitch
import com.thatdot.quine.app.model.ingest2.source.{DecodedSource, IngestBounds}
import com.thatdot.quine.app.routes.IngestMeter
import com.thatdot.quine.graph.cypher.Expr

case class NumberIteratorSource(
  bounds: IngestBounds = IngestBounds(),
  ingestMeter: IngestMeter,
) {

  def decodedSource: DecodedSource = new DecodedSource(ingestMeter) {
    type Decoded = Expr.Integer
    type Frame = Expr.Integer

    private val integerFold: DataFoldableFrom[Expr.Integer] = new DataFoldableFrom[Expr.Integer] {
      def fold[B](value: Expr.Integer, folder: DataFolderTo[B]): B = folder.integer(value.long)
    }

    override val foldable: DataFoldableFrom[Expr.Integer] = integerFold
    override val foldableFrame: DataFoldableFrom[Expr.Integer] = integerFold

    override def content(input: Expr.Integer): Array[Byte] =
      ByteBuffer.allocate(8).putLong(input.long).array()

    def stream: Source[(() => Try[Expr.Integer], Expr.Integer), ShutdownSwitch] = {

      val sourceBase = Source.unfold(bounds.startAtOffset)(ln => Some(ln + 1 -> Expr.Integer(ln)))

      val bounded = bounds.ingestLimit.fold(sourceBase)(limit => sourceBase.take(limit))

      withKillSwitches(
        bounded
          .via(metered[Expr.Integer](meter, _ => 1)) //TODO this counts values not bytes
          .map(sum => (() => Success(sum), sum)),
      )
    }
  }
}
