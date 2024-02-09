package com.thatdot.quine.compiler.cypher

import com.thatdot.quine.graph.cypher.Expr

class CypherMatrix extends CypherHarness("cypher-matrix-tests") {

  import QuineIdImplicitConversions._

  val neoNode: Expr.Node = Expr.Node(0L, Set(Symbol("Crew")), Map(Symbol("name") -> Expr.Str("Neo")))
  val morpheusNode: Expr.Node = Expr.Node(1L, Set(Symbol("Crew")), Map(Symbol("name") -> Expr.Str("Morpheus")))
  val trinityNode: Expr.Node = Expr.Node(2L, Set(Symbol("Crew")), Map(Symbol("name") -> Expr.Str("Trinity")))
  val cypherNode: Expr.Node =
    Expr.Node(3L, Set(Symbol("Crew"), Symbol("Matrix")), Map(Symbol("name") -> Expr.Str("Cypher")))
  val agentSmithNode: Expr.Node =
    Expr.Node(4L, Set(Symbol("Matrix")), Map(Symbol("name") -> Expr.Str("Agent Smith")))
  val architectNode: Expr.Node =
    Expr.Node(5L, Set(Symbol("Matrix")), Map(Symbol("name") -> Expr.Str("The Architect")))

  testQuery(
    """create (Neo:Crew {name:'Neo'}),
      |  (Morpheus:Crew {name: 'Morpheus'}),
      |  (Trinity:Crew {name: 'Trinity'}),
      |  (Cypher:Crew:Matrix {name: 'Cypher'}),
      |  (Smith:Matrix {name: 'Agent Smith'}),
      |  (Architect:Matrix {name:'The Architect'}),
      |  (Neo)-[:KNOWS]->(Morpheus),
      |  (Neo)-[:LOVES]->(Trinity),
      |  (Morpheus)-[:KNOWS]->(Trinity),
      |  (Morpheus)-[:KNOWS]->(Cypher), (Cypher)-[:KNOWS]->(Smith),
      |  (Smith)-[:CODED_BY]->(Architect)""".stripMargin,
    expectedColumns = Vector.empty,
    expectedRows = Seq.empty,
    expectedIsReadOnly = false,
    expectedIsIdempotent = false
  )

  testQuery(
    """match (n)
      |return n as thing
      |order by id(n)
      |union all
      |match ()-[r]->()
      |return r as thing
      |order by id(startNode(r)), id(endNode(r)), type(r)""".stripMargin,
    expectedColumns = Vector("thing"),
    expectedRows = Seq(
      Vector(agentSmithNode),
      Vector(neoNode),
      Vector(morpheusNode),
      Vector(trinityNode),
      Vector(architectNode),
      Vector(cypherNode),
      Vector(Expr.Relationship(agentSmithNode.id, Symbol("CODED_BY"), Map.empty, architectNode.id)),
      Vector(Expr.Relationship(neoNode.id, Symbol("KNOWS"), Map.empty, morpheusNode.id)),
      Vector(Expr.Relationship(neoNode.id, Symbol("LOVES"), Map.empty, trinityNode.id)),
      Vector(Expr.Relationship(morpheusNode.id, Symbol("KNOWS"), Map.empty, trinityNode.id)),
      Vector(Expr.Relationship(morpheusNode.id, Symbol("KNOWS"), Map.empty, cypherNode.id)),
      Vector(Expr.Relationship(cypherNode.id, Symbol("KNOWS"), Map.empty, agentSmithNode.id))
    ),
    expectedCanContainAllNodeScan = true
  )

  testQuery(
    "match (n:Crew)-[r]->(m) where n.name='Neo' return type(r), m.name",
    expectedColumns = Vector("type(r)", "m.name"),
    expectedRows = Seq(
      Vector(Expr.Str("KNOWS"), Expr.Str("Morpheus")),
      Vector(Expr.Str("LOVES"), Expr.Str("Trinity"))
    ),
    expectedCanContainAllNodeScan = true,
    ordered = false
  )

  testQuery(
    "match (n:Crew { name: 'Morpheus' })-[r:KNOWS]-(m) return m.name",
    expectedColumns = Vector("m.name"),
    expectedRows = Seq(
      Vector(Expr.Str("Neo")),
      Vector(Expr.Str("Cypher")),
      Vector(Expr.Str("Trinity"))
    ),
    expectedCanContainAllNodeScan = true,
    ordered = false
  )

  testQuery(
    "match (n: Crew)--(m) return n.name, collect(m.name)",
    expectedColumns = Vector("n.name", "collect(m.name)"),
    expectedRows = Seq(
      Vector(Expr.Str("Cypher"), Expr.List(Vector(Expr.Str("Agent Smith"), Expr.Str("Morpheus")))),
      Vector(Expr.Str("Neo"), Expr.List(Vector(Expr.Str("Trinity"), Expr.Str("Morpheus")))),
      Vector(
        Expr.Str("Morpheus"),
        Expr.List(Vector(Expr.Str("Cypher"), Expr.Str("Trinity"), Expr.Str("Neo")))
      ),
      Vector(Expr.Str("Trinity"), Expr.List(Vector(Expr.Str("Neo"), Expr.Str("Morpheus"))))
    ),
    expectedCanContainAllNodeScan = true,
    ordered = false
  )

  testQuery(
    "match (n: Crew)--(m) return n.name, m.name",
    expectedColumns = Vector("n.name", "m.name"),
    expectedRows = Seq(
      Vector(Expr.Str("Neo"), Expr.Str("Morpheus")),
      Vector(Expr.Str("Neo"), Expr.Str("Trinity")),
      Vector(Expr.Str("Cypher"), Expr.Str("Agent Smith")),
      Vector(Expr.Str("Cypher"), Expr.Str("Morpheus")),
      Vector(Expr.Str("Morpheus"), Expr.Str("Trinity")),
      Vector(Expr.Str("Morpheus"), Expr.Str("Cypher")),
      Vector(Expr.Str("Morpheus"), Expr.Str("Neo")),
      Vector(Expr.Str("Trinity"), Expr.Str("Neo")),
      Vector(Expr.Str("Trinity"), Expr.Str("Morpheus"))
    ),
    expectedCanContainAllNodeScan = true,
    ordered = false
  )

  testQuery(
    "match (n) where exists((n)-[:KNOWS]->()) return n.name",
    expectedColumns = Vector("n.name"),
    expectedRows = Seq(
      Vector(Expr.Str("Neo")),
      Vector(Expr.Str("Cypher")),
      Vector(Expr.Str("Morpheus"))
    ),
    expectedCanContainAllNodeScan = true,
    ordered = false
  )

  testQuery(
    "match (n) where n.name IS NOT NULL return count(*)",
    expectedColumns = Vector("count(*)"),
    expectedRows = Seq(Vector(Expr.Integer(6L))),
    expectedCanContainAllNodeScan = true,
    ordered = false
  )

  val neoMorpheusEdge: Expr.Relationship = Expr.Relationship(neoNode.id, Symbol("KNOWS"), Map.empty, morpheusNode.id)
  val morpheusTrinityEdge: Expr.Relationship =
    Expr.Relationship(morpheusNode.id, Symbol("KNOWS"), Map.empty, trinityNode.id)
  val morpheusCypherEdge: Expr.Relationship =
    Expr.Relationship(morpheusNode.id, Symbol("KNOWS"), Map.empty, cypherNode.id)
  val cypherAgentSmithEdge: Expr.Relationship =
    Expr.Relationship(cypherNode.id, Symbol("KNOWS"), Map.empty, agentSmithNode.id)
  val agentSmithArchitectEdge: Expr.Relationship =
    Expr.Relationship(agentSmithNode.id, Symbol("CODED_BY"), Map.empty, architectNode.id)

  testQuery(
    "match (n) return n.name, (n)-->()-->()",
    expectedColumns = Vector("n.name", "(n)-->()-->()"),
    expectedRows = Seq(
      Vector(
        Expr.Str("Neo"),
        Expr.List(
          Vector(
            Expr.Path(
              neoNode,
              Vector(
                neoMorpheusEdge -> morpheusNode,
                morpheusCypherEdge -> cypherNode
              )
            ),
            Expr.Path(
              neoNode,
              Vector(
                neoMorpheusEdge -> morpheusNode,
                morpheusTrinityEdge -> trinityNode
              )
            )
          )
        )
      ),
      Vector(
        Expr.Str("Morpheus"),
        Expr.List(
          Vector(
            Expr.Path(
              morpheusNode,
              Vector(
                morpheusCypherEdge -> cypherNode,
                cypherAgentSmithEdge -> agentSmithNode
              )
            )
          )
        )
      ),
      Vector(
        Expr.Str("Cypher"),
        Expr.List(
          Vector(
            Expr.Path(
              cypherNode,
              Vector(
                cypherAgentSmithEdge -> agentSmithNode,
                agentSmithArchitectEdge -> architectNode
              )
            )
          )
        )
      ),
      Vector(Expr.Str("Agent Smith"), Expr.List(Vector.empty)),
      Vector(Expr.Str("The Architect"), Expr.List(Vector.empty)),
      Vector(Expr.Str("Trinity"), Expr.List(Vector.empty))
    ),
    expectedCanContainAllNodeScan = true,
    ordered = false
  )
}
