package com.thatdot.quine.app.model.outputs2.query.standing

import com.thatdot.data.DataFoldableFrom
import com.thatdot.quine.app.data.QuineDataFoldablesFrom
import com.thatdot.quine.graph.StandingQueryResult
import com.thatdot.quine.model.{QuineIdProvider, QuineValue}

sealed trait StandingQueryResultTransform {
  type Out
  def dataFoldableFrom: DataFoldableFrom[Out]
  def apply(standingQueryResult: StandingQueryResult): Out
}

object StandingQueryResultTransform {
  case class InlineData()(implicit idProvider: QuineIdProvider) extends StandingQueryResultTransform {
    override type Out = QuineValue
    override def dataFoldableFrom: DataFoldableFrom[Out] = QuineDataFoldablesFrom.quineValueDataFoldable
    override def apply(standingQueryResult: StandingQueryResult): Out = QuineValue(standingQueryResult.data)
  }
}
