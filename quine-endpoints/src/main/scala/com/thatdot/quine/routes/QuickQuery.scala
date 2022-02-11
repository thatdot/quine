package com.thatdot.quine.routes

import endpoints4s.generic.{docs, title}

/** Enumeration for the kinds of queries we can issue */
sealed abstract class QuerySort
object QuerySort {
  case object Node extends QuerySort
  case object Text extends QuerySort
}

/** Enumeration for the supported query languages */
sealed abstract class QueryLanguage
object QueryLanguage {
  case object Cypher extends QueryLanguage
  case object Gremlin extends QueryLanguage
}

/** Queries like the ones that show up when right-clicking nodes
  *
  * TODO: use query parameters (challenge is how to render these nicely in the exploration UI)
  *
  * @param name human-readable title for the query
  * @param querySuffix query suffix
  * @param queryLanguage query language used
  * @param sort what should be done with query results?
  * @param edgeLabel virtual edge label (only relevant on node queries)
  */
@title("Quick Query Action")
@docs("Query that gets executed starting at some node (eg. by double-clicking or right-clicking).")
final case class QuickQuery(
  name: String,
  @docs("suffix of a traversal query (eg, `.values('someKey')` for Gremlin or `RETURN n.someKey` for Cypher)")
  querySuffix: String,
  @docs("query language used in the query suffix")
  queryLanguage: QueryLanguage,
  @docs("whether the query returns node or text results")
  sort: QuerySort,
  @docs(
    """if this label is set and the query is configured to return nodes, each of the nodes returned
      |will have an additional dotted edge which connect to the source node of the quick query""".stripMargin
  )
  edgeLabel: Option[String]
) {

  /** Synthesize a full query
    *
    * @param startingIds ID of the nodes on which to run the quick query
    * @return query that is ready to be run
    */
  def fullQuery(startingIds: Seq[String]): String = {
    val simpleNumberId = startingIds.forall(_ matches "-?\\d+")
    val idOrStrIds = startingIds
      .map { (startingId: String) =>
        if (simpleNumberId) startingId.toString else ujson.Str(startingId).toString
      }
      .mkString(", ")

    queryLanguage match {
      case QueryLanguage.Gremlin =>
        s"g.V($idOrStrIds)$querySuffix"

      case QueryLanguage.Cypher =>
        if (startingIds.length == 1) {
          s"MATCH (n) WHERE ${if (simpleNumberId) "id" else "strId"}(n) = $idOrStrIds $querySuffix"
        } else {
          s"UNWIND [$idOrStrIds] AS nId MATCH (n) WHERE ${if (simpleNumberId) "id" else "strId"}(n) = nId $querySuffix"
        }
    }
  }
}

object QuickQuery {

  /** Open up adjacent nodes */
  def adjacentNodes(queryLanguage: QueryLanguage): QuickQuery = {
    val querySuffix = queryLanguage match {
      case QueryLanguage.Gremlin =>
        ".both()"
      case QueryLanguage.Cypher =>
        "MATCH (n)--(m) RETURN DISTINCT m"
    }

    QuickQuery(
      name = "Adjacent Nodes",
      querySuffix,
      queryLanguage,
      sort = QuerySort.Node,
      edgeLabel = None
    )
  }

  /** Refresh the current node */
  def refreshNode(queryLanguage: QueryLanguage): QuickQuery = {
    val querySuffix = queryLanguage match {
      case QueryLanguage.Gremlin =>
        ""
      case QueryLanguage.Cypher =>
        "RETURN n"
    }

    QuickQuery(
      name = "Refresh",
      querySuffix,
      queryLanguage,
      sort = QuerySort.Node,
      edgeLabel = None
    )
  }

  /** Print out the properties of the node */
  def getProperties(queryLanguage: QueryLanguage): QuickQuery = {
    val querySuffix = queryLanguage match {
      case QueryLanguage.Gremlin =>
        ".as('n').valueMap().as('properties').select('n').id().as('id').select('id','properties')"
      case QueryLanguage.Cypher =>
        "RETURN id(n), properties(n)"
    }

    QuickQuery(
      name = "Local Properties",
      querySuffix,
      queryLanguage,
      sort = QuerySort.Text,
      edgeLabel = None
    )
  }

  val potterSiblings: QuickQuery = QuickQuery(
    name = "Siblings",
    querySuffix = "MATCH (n)-[:has_father|:has_mother]->(p)<-[:has_father|:has_mother]-(s) RETURN DISTINCT s",
    queryLanguage = QueryLanguage.Cypher,
    sort = QuerySort.Node,
    edgeLabel = Some("has sibling")
  )
}
