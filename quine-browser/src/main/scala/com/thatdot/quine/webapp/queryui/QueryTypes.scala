package com.thatdot.quine.webapp.queryui

import com.thatdot.quine.webapp.QueryUiOptions

/** How should the query UI interpret the query?
  *
  * Extracted from QueryUi.scala so these framework-agnostic types
  * can be shared without depending on a specific UI framework.
  */
sealed abstract class UiQueryType
object UiQueryType {

  /** Query is text, and results should go in the green message bar */
  case object Text extends UiQueryType

  /** Query is for nodes/edges, and results should be spread across the canvas */
  case object Node extends UiQueryType

  /** Query is for nodes/edges, and results should explode out from one node
    *
    * @param explodeFromId from which node should results explode
    * @param syntheticEdgeLabel if set, also draw a purple dotted edge (with this
    *                           label) from the central node to all of the other nodes
    */
  final case class NodeFromId(explodeFromId: String, syntheticEdgeLabel: Option[String]) extends UiQueryType
}

/** How `vis` should structure nodes */
sealed abstract class NetworkLayout
object NetworkLayout {
  case object Graph extends NetworkLayout
  case object Tree extends NetworkLayout
}

/** How should queries be relayed to the backend? */
sealed abstract class QueryMethod
object QueryMethod {
  case object Restful extends QueryMethod
  case object RestfulV2 extends QueryMethod
  case object WebSocket extends QueryMethod
  case object WebSocketV2 extends QueryMethod

  def parseQueryMethod(options: QueryUiOptions): QueryMethod = {
    val useWs = options.queriesOverWs.getOrElse(false)
    val useV2Api = options.queriesOverV2Api.getOrElse(true)

    (useV2Api, useWs) match {
      case (true, true) => QueryMethod.WebSocketV2
      case (true, false) => QueryMethod.RestfulV2
      case (false, true) => QueryMethod.WebSocket
      case (false, false) => QueryMethod.Restful
    }
  }
}
