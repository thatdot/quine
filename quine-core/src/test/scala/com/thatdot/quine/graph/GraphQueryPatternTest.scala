package com.thatdot.quine.graph

import scala.collection.compat.immutable._

import cats.data.NonEmptyList
import org.scalatest.funsuite.AnyFunSuite

import com.thatdot.quine.graph.InvalidQueryPattern.{HasACycle, NotConnected}
import com.thatdot.quine.graph.cypher.MultipleValuesStandingQuery
import com.thatdot.quine.model._

class GraphQueryPatternTest extends AnyFunSuite {

  val labelsProp: Symbol = Symbol("_LABEL")

  import GraphQueryPattern._

  val node1: NodePattern = NodePattern(
    id = NodePatternId(1),
    labels = Set(),
    qidOpt = None,
    properties = Map(
      Symbol("foo") -> PropertyValuePattern.AnyValue,
      Symbol("bar") -> PropertyValuePattern.Value(QuineValue.Str("DEADBEEF"))
    )
  )
  val node1Labelled: NodePattern = node1.copy(id = NodePatternId(11), labels = Set(Symbol("LABELLED_NODE")))

  val node2: NodePattern = NodePattern(
    id = NodePatternId(2),
    labels = Set(),
    qidOpt = None,
    properties = Map.empty
  )

  val node3: NodePattern = NodePattern(
    id = NodePatternId(3),
    labels = Set(),
    qidOpt = None,
    properties = Map(
      Symbol("qux") -> PropertyValuePattern.Value(QuineValue.Str("0011223344")),
      Symbol("bar") -> PropertyValuePattern.AnyValue
    )
  )

  val node4: NodePattern = NodePattern(
    id = NodePatternId(4),
    labels = Set(),
    qidOpt = Some(IdentityIdProvider.customIdFromString("123456").get),
    properties = Map(
      Symbol("qux") -> PropertyValuePattern.Value(QuineValue.Str("0011223344")),
      Symbol("bar") -> PropertyValuePattern.AnyValue
    )
  )

  val node5: NodePattern = NodePattern(
    id = NodePatternId(5),
    labels = Set(),
    qidOpt = Some(IdentityIdProvider.customIdFromString("5678abcd").get),
    properties = Map.empty
  )

  val node6: NodePattern = NodePattern(
    id = NodePatternId(6),
    labels = Set(),
    qidOpt = None,
    properties = Map(
      Symbol("quux") -> PropertyValuePattern.AnyValue,
      Symbol("quz") -> PropertyValuePattern.AnyValue
    )
  )

  val node7: NodePattern = NodePattern(
    id = NodePatternId(7),
    labels = Set(),
    qidOpt = None,
    properties = Map(
      Symbol("quux") -> PropertyValuePattern.Value(QuineValue.Integer(4L)),
      Symbol("quz") -> PropertyValuePattern.Value(QuineValue.Integer(4L))
    )
  )

  val node8: NodePattern = NodePattern(
    id = NodePatternId(8),
    labels = Set(),
    qidOpt = None,
    properties = Map(
      Symbol("bax") -> PropertyValuePattern.AnyValue
    )
  )

  val node9: NodePattern = NodePattern(
    id = NodePatternId(9),
    labels = Set(),
    qidOpt = None,
    properties = Map(
      Symbol("box") -> PropertyValuePattern.Value(QuineValue.Integer(1234L))
    )
  )

  test("Single pattern") {
    val singlePattern = GraphQueryPattern(
      nodes = NonEmptyList.of(node1),
      edges = Seq.empty,
      startingPoint = node1.id,
      toExtract = Seq(ReturnColumn.Id(node1.id, false, Symbol("id"))),
      filterCond = None,
      toReturn = Nil,
      distinct = true
    )
    val expectedBranch = SingleBranch(
      DomainNodeEquiv(
        None,
        Map(
          Symbol("foo") -> (PropertyComparisonFunctions.Wildcard -> None),
          Symbol("bar") -> (PropertyComparisonFunctions.Identicality -> Some(
            PropertyValue(QuineValue.Str("DEADBEEF"))
          ))
        ),
        Set()
      ),
      None,
      List.empty
    )
    assert(singlePattern.compiledDomainGraphBranch(labelsProp)._1 == expectedBranch)
  }

  test("Linear pattern") {
    val linePattern = {
      val edgeA = EdgePattern(node1.id, node2.id, true, Symbol("a"))
      val edgeB = EdgePattern(node2.id, node3.id, true, Symbol("b"))

      GraphQueryPattern(
        nodes = NonEmptyList.of(node1, node2, node3),
        edges = Seq(edgeA, edgeB),
        startingPoint = node1.id,
        toExtract = Seq(ReturnColumn.Id(node1.id, false, Symbol("id"))),
        filterCond = None,
        toReturn = Nil,
        distinct = true
      )
    }
    val expected = SingleBranch(
      DomainNodeEquiv(
        None,
        Map(
          Symbol("foo") -> (PropertyComparisonFunctions.Wildcard -> None),
          Symbol("bar") -> (PropertyComparisonFunctions.Identicality -> Some(
            PropertyValue(QuineValue.Str("DEADBEEF"))
          ))
        ),
        Set()
      ),
      None,
      List(
        DomainEdge(
          GenericEdge(Symbol("a"), EdgeDirection.Outgoing),
          DependsUpon,
          SingleBranch(
            DomainNodeEquiv.empty,
            None,
            List(
              DomainEdge(
                GenericEdge(Symbol("b"), EdgeDirection.Outgoing),
                DependsUpon,
                SingleBranch(
                  DomainNodeEquiv(
                    None,
                    Map(
                      Symbol("qux") -> (PropertyComparisonFunctions.Identicality -> Some(
                        PropertyValue(QuineValue.Str("0011223344"))
                      )),
                      Symbol("bar") -> (PropertyComparisonFunctions.Wildcard -> None)
                    ),
                    Set()
                  ),
                  None,
                  List()
                )
              )
            )
          )
        )
      )
    )
    assert(linePattern.compiledDomainGraphBranch(labelsProp)._1 == expected)
  }

  test("Explicitly rooted linear pattern") {
    val rootedPattern = {
      val edgeA = EdgePattern(node1.id, node2.id, true, Symbol("a"))
      val edgeB = EdgePattern(node2.id, node3.id, true, Symbol("b"))

      GraphQueryPattern(
        nodes = NonEmptyList.of(node1, node2, node3),
        edges = Seq(edgeA, edgeB),
        startingPoint = node2.id,
        toExtract = Seq(ReturnColumn.Id(node2.id, false, Symbol("id"))),
        filterCond = None,
        toReturn = Nil,
        distinct = true
      )
    }
    val expected = SingleBranch(
      DomainNodeEquiv.empty,
      None,
      List(
        DomainEdge(
          GenericEdge(Symbol("a"), EdgeDirection.Incoming),
          DependsUpon,
          SingleBranch(
            DomainNodeEquiv(
              None,
              Map(
                Symbol("foo") -> (PropertyComparisonFunctions.Wildcard -> None),
                Symbol("bar") -> (PropertyComparisonFunctions.Identicality -> Some(
                  PropertyValue(QuineValue.Str("DEADBEEF"))
                ))
              ),
              Set()
            ),
            None,
            List()
          )
        ),
        DomainEdge(
          GenericEdge(Symbol("b"), EdgeDirection.Outgoing),
          DependsUpon,
          SingleBranch(
            DomainNodeEquiv(
              None,
              Map(
                Symbol("qux") -> (PropertyComparisonFunctions.Identicality -> Some(
                  PropertyValue(QuineValue.Str("0011223344"))
                )),
                Symbol("bar") -> (PropertyComparisonFunctions.Wildcard -> None)
              ),
              Set()
            ),
            None,
            List()
          )
        )
      )
    )
    assert(rootedPattern.compiledDomainGraphBranch(labelsProp)._1 == expected)
  }

  test("Tree pattern") {
    val treePattern = {
      val edgeA = EdgePattern(node1.id, node2.id, true, Symbol("a"))
      val edgeB = EdgePattern(node1.id, node3.id, true, Symbol("b"))
      val edgeC = EdgePattern(node2.id, node4.id, true, Symbol("c"))
      val edgeD = EdgePattern(node2.id, node5.id, true, Symbol("d"))
      val edgeE = EdgePattern(node2.id, node6.id, true, Symbol("e"))
      val edgeF = EdgePattern(node3.id, node7.id, true, Symbol("f"))

      GraphQueryPattern(
        nodes = NonEmptyList.of(node1, node2, node3, node4, node5, node6, node7),
        edges = Seq(edgeA, edgeB, edgeC, edgeD, edgeE, edgeF),
        startingPoint = node4.id,
        toExtract = Seq(ReturnColumn.Id(node4.id, false, Symbol("id"))),
        filterCond = None,
        toReturn = Nil,
        distinct = true
      )
    }
    val expected = SingleBranch(
      DomainNodeEquiv(
        None,
        Map(
          Symbol("qux") -> (PropertyComparisonFunctions.Identicality -> Some(
            PropertyValue(QuineValue.Str("0011223344"))
          )),
          Symbol("bar") -> (PropertyComparisonFunctions.Wildcard -> None)
        ),
        Set()
      ),
      node4.qidOpt,
      List(
        DomainEdge(
          GenericEdge(Symbol("c"), EdgeDirection.Incoming),
          DependsUpon,
          SingleBranch(
            DomainNodeEquiv.empty,
            None,
            List(
              DomainEdge(
                GenericEdge(Symbol("a"), EdgeDirection.Incoming),
                DependsUpon,
                SingleBranch(
                  DomainNodeEquiv(
                    None,
                    Map(
                      Symbol("foo") -> (PropertyComparisonFunctions.Wildcard -> None),
                      Symbol("bar") -> (PropertyComparisonFunctions.Identicality -> Some(
                        PropertyValue(QuineValue.Str("DEADBEEF"))
                      ))
                    ),
                    Set()
                  ),
                  None,
                  List(
                    DomainEdge(
                      GenericEdge(Symbol("b"), EdgeDirection.Outgoing),
                      DependsUpon,
                      SingleBranch(
                        DomainNodeEquiv(
                          None,
                          Map(
                            Symbol("qux") -> (PropertyComparisonFunctions.Identicality -> Some(
                              PropertyValue(QuineValue.Str("0011223344"))
                            )),
                            Symbol("bar") -> (PropertyComparisonFunctions.Wildcard -> None)
                          ),
                          Set()
                        ),
                        None,
                        List(
                          DomainEdge(
                            GenericEdge(Symbol("f"), EdgeDirection.Outgoing),
                            DependsUpon,
                            SingleBranch(
                              DomainNodeEquiv(
                                None,
                                Map(
                                  Symbol(
                                    "quux"
                                  ) -> (PropertyComparisonFunctions.Identicality -> Some(
                                    PropertyValue(QuineValue.Integer(4L))
                                  )),
                                  Symbol(
                                    "quz"
                                  ) -> (PropertyComparisonFunctions.Identicality -> Some(
                                    PropertyValue(QuineValue.Integer(4L))
                                  ))
                                ),
                                Set()
                              ),
                              None,
                              List()
                            )
                          )
                        )
                      )
                    )
                  )
                )
              ),
              DomainEdge(
                GenericEdge(Symbol("d"), EdgeDirection.Outgoing),
                DependsUpon,
                SingleBranch(
                  DomainNodeEquiv.empty,
                  node5.qidOpt,
                  List()
                )
              ),
              DomainEdge(
                GenericEdge(Symbol("e"), EdgeDirection.Outgoing),
                DependsUpon,
                SingleBranch(
                  DomainNodeEquiv(
                    None,
                    Map(
                      Symbol("quux") -> (PropertyComparisonFunctions.Wildcard -> None),
                      Symbol("quz") -> (PropertyComparisonFunctions.Wildcard -> None)
                    ),
                    Set()
                  ),
                  None,
                  List()
                )
              )
            )
          )
        )
      )
    )
    assert(treePattern.compiledDomainGraphBranch(labelsProp)._1 == expected)
  }

  test("Disconnected pattern") {
    val disconnectedPattern = {
      val edgeA = EdgePattern(node1.id, node2.id, true, Symbol("a"))

      GraphQueryPattern(
        nodes = NonEmptyList.of(node1, node2, node3),
        edges = Seq(edgeA),
        startingPoint = node1.id,
        toExtract = Seq(ReturnColumn.Id(node1.id, false, Symbol("id"))),
        filterCond = None,
        toReturn = Nil,
        distinct = true
      )
    }

    val expected = NotConnected
    assert(
      intercept[InvalidQueryPattern](disconnectedPattern.compiledDomainGraphBranch(labelsProp)) == expected
    )
    assert(
      intercept[InvalidQueryPattern](
        disconnectedPattern.compiledMultipleValuesStandingQuery(labelsProp, IdentityIdProvider)
      ) == expected
    )
  }

  test("Diamond pattern") {
    val diamondPattern = {
      val edgeA = EdgePattern(node1.id, node2.id, true, Symbol("a"))
      val edgeB = EdgePattern(node2.id, node3.id, true, Symbol("b"))
      val edgeC = EdgePattern(node4.id, node3.id, true, Symbol("c"))
      val edgeD = EdgePattern(node1.id, node4.id, true, Symbol("d"))

      GraphQueryPattern(
        nodes = NonEmptyList.of(node1, node2, node3, node4),
        edges = Seq(edgeA, edgeB, edgeC, edgeD),
        startingPoint = node1.id,
        toExtract = Seq(ReturnColumn.Id(node1.id, false, Symbol("id"))),
        filterCond = None,
        toReturn = Nil,
        distinct = true
      )
    }

    assert(intercept[InvalidQueryPattern](diamondPattern.compiledDomainGraphBranch(labelsProp)) == HasACycle)
  }

  test("Complex graph pattern") {
    val graphPattern = {
      val edgeA = EdgePattern(node1.id, node2.id, true, Symbol("a"))
      val edgeB = EdgePattern(node7.id, node1.id, true, Symbol("b"))
      val edgeC = EdgePattern(node2.id, node7.id, false, Symbol("c"))
      val edgeD = EdgePattern(node5.id, node8.id, true, Symbol("d"))
      val edgeE = EdgePattern(node3.id, node5.id, true, Symbol("e"))
      val edgeF = EdgePattern(node3.id, node9.id, false, Symbol("f"))
      val edgeG = EdgePattern(node3.id, node4.id, true, Symbol("g"))
      val edgeH = EdgePattern(node4.id, node2.id, true, Symbol("h"))
      val edgeI = EdgePattern(node6.id, node8.id, true, Symbol("i"))
      val edgeJ = EdgePattern(node7.id, node6.id, false, Symbol("j"))
      val edgeK = EdgePattern(node1.id, node8.id, true, Symbol("k"))

      GraphQueryPattern(
        nodes = NonEmptyList.of(node1, node2, node3, node4, node5, node6, node7, node8, node9),
        edges = Seq(edgeA, edgeB, edgeC, edgeD, edgeE, edgeF, edgeG, edgeH, edgeI, edgeJ, edgeK),
        startingPoint = node1.id,
        toExtract = Seq(ReturnColumn.Id(node1.id, false, Symbol("id"))),
        filterCond = None,
        toReturn = Nil,
        distinct = true
      )
    }

    assert(intercept[InvalidQueryPattern](graphPattern.compiledDomainGraphBranch(labelsProp)) == HasACycle)
  }

  test("compiling a cypher GraphQueryPattern with ID constraint") {

    val treePattern = {
      val edgeA = EdgePattern(node1.id, node2.id, true, Symbol("a"))
      val edgeB = EdgePattern(node1.id, node3.id, true, Symbol("b"))
      val edgeC = EdgePattern(node2.id, node4.id, true, Symbol("c"))
      val edgeD = EdgePattern(node2.id, node5.id, true, Symbol("d"))
      val edgeE = EdgePattern(node2.id, node6.id, true, Symbol("e"))
      val edgeF = EdgePattern(node3.id, node7.id, true, Symbol("f"))

      GraphQueryPattern(
        nodes = NonEmptyList.of(node1, node2, node3, node4, node5, node6, node7),
        edges = Seq(edgeA, edgeB, edgeC, edgeD, edgeE, edgeF),
        startingPoint = node4.id,
        toExtract = Seq(ReturnColumn.Id(node4.id, false, Symbol("id"))),
        filterCond = None,
        toReturn = Nil,
        distinct = true
      )
    }

    val expected = MultipleValuesStandingQuery.Cross(
      ArraySeq(
        MultipleValuesStandingQuery.LocalProperty(
          Symbol("qux"),
          MultipleValuesStandingQuery.LocalProperty.Equal(cypher.Expr.Str("0011223344")),
          None
        ),
        MultipleValuesStandingQuery
          .LocalProperty(Symbol("bar"), MultipleValuesStandingQuery.LocalProperty.Any, None),
        MultipleValuesStandingQuery.FilterMap(
          Some(
            cypher.Expr.Equal(
              cypher.Expr.Variable(Symbol("__local_id")),
              cypher.Expr.Bytes(IdentityIdProvider.customIdFromString("123456").get)
            )
          ),
          MultipleValuesStandingQuery.LocalId(Symbol("__local_id"), false),
          true,
          List()
        ),
        MultipleValuesStandingQuery.LocalId(Symbol("id"), false),
        MultipleValuesStandingQuery.SubscribeAcrossEdge(
          Some(Symbol("c")),
          Some(EdgeDirection.Incoming),
          MultipleValuesStandingQuery.Cross(
            ArraySeq(
              MultipleValuesStandingQuery.SubscribeAcrossEdge(
                Some(Symbol("a")),
                Some(EdgeDirection.Incoming),
                MultipleValuesStandingQuery.Cross(
                  ArraySeq(
                    MultipleValuesStandingQuery
                      .LocalProperty(
                        Symbol("foo"),
                        MultipleValuesStandingQuery.LocalProperty.Any,
                        None
                      ),
                    MultipleValuesStandingQuery.LocalProperty(
                      Symbol("bar"),
                      MultipleValuesStandingQuery.LocalProperty.Equal(cypher.Expr.Str("DEADBEEF")),
                      None
                    ),
                    MultipleValuesStandingQuery.SubscribeAcrossEdge(
                      Some(Symbol("b")),
                      Some(EdgeDirection.Outgoing),
                      MultipleValuesStandingQuery.Cross(
                        ArraySeq(
                          MultipleValuesStandingQuery.LocalProperty(
                            Symbol("qux"),
                            MultipleValuesStandingQuery.LocalProperty.Equal(cypher.Expr.Str("0011223344")),
                            None
                          ),
                          MultipleValuesStandingQuery.LocalProperty(
                            Symbol("bar"),
                            MultipleValuesStandingQuery.LocalProperty.Any,
                            None
                          ),
                          MultipleValuesStandingQuery.SubscribeAcrossEdge(
                            Some(Symbol("f")),
                            Some(EdgeDirection.Outgoing),
                            MultipleValuesStandingQuery.Cross(
                              ArraySeq(
                                MultipleValuesStandingQuery.LocalProperty(
                                  Symbol("quux"),
                                  MultipleValuesStandingQuery.LocalProperty.Equal(cypher.Expr.Integer(4)),
                                  None
                                ),
                                MultipleValuesStandingQuery.LocalProperty(
                                  Symbol("quz"),
                                  MultipleValuesStandingQuery.LocalProperty.Equal(cypher.Expr.Integer(4)),
                                  None
                                )
                              ),
                              emitSubscriptionsLazily = true
                            )
                          )
                        ),
                        emitSubscriptionsLazily = true
                      )
                    )
                  ),
                  emitSubscriptionsLazily = true
                )
              ),
              MultipleValuesStandingQuery.SubscribeAcrossEdge(
                Some(Symbol("d")),
                Some(EdgeDirection.Outgoing),
                MultipleValuesStandingQuery.FilterMap(
                  Some(
                    cypher.Expr.Equal(
                      cypher.Expr.Variable(Symbol("__local_id")),
                      cypher.Expr.Bytes(IdentityIdProvider.customIdFromString("5678ABCD").get)
                    )
                  ),
                  MultipleValuesStandingQuery.LocalId(Symbol("__local_id"), false),
                  true,
                  List()
                )
              ),
              MultipleValuesStandingQuery.SubscribeAcrossEdge(
                Some(Symbol("e")),
                Some(EdgeDirection.Outgoing),
                MultipleValuesStandingQuery.Cross(
                  ArraySeq(
                    MultipleValuesStandingQuery
                      .LocalProperty(
                        Symbol("quux"),
                        MultipleValuesStandingQuery.LocalProperty.Any,
                        None
                      ),
                    MultipleValuesStandingQuery
                      .LocalProperty(
                        Symbol("quz"),
                        MultipleValuesStandingQuery.LocalProperty.Any,
                        None
                      )
                  ),
                  emitSubscriptionsLazily = true
                )
              )
            ),
            emitSubscriptionsLazily = true
          )
        )
      ),
      emitSubscriptionsLazily = true
    )

    val actual = treePattern.compiledMultipleValuesStandingQuery(labelsProp, IdentityIdProvider)

    assert(expected === actual)
  }

  test("compiling a cypher GQP with label constraint") {
    val graphPattern = GraphQueryPattern(
      nodes = NonEmptyList.of(node1Labelled),
      edges = Seq(),
      startingPoint = node1Labelled.id,
      toExtract = Seq(
        ReturnColumn
          .Property(node1Labelled.id, Symbol("foo"), Symbol("pulledValue"))
      ),
      filterCond = None,
      toReturn = Nil,
      distinct = false
    )

    val actual = graphPattern.compiledMultipleValuesStandingQuery(labelsProp, IdentityIdProvider)

    val expected = MultipleValuesStandingQuery.Cross(
      ArraySeq(
        MultipleValuesStandingQuery.LocalProperty(
          Symbol("foo"),
          MultipleValuesStandingQuery.LocalProperty.Any,
          Some(Symbol("pulledValue"))
        ),
        MultipleValuesStandingQuery.LocalProperty(
          Symbol("bar"),
          MultipleValuesStandingQuery.LocalProperty.Equal(cypher.Expr.Str("DEADBEEF")),
          None
        ),
        MultipleValuesStandingQuery.FilterMap(
          Some(
            cypher.Expr.AnyInList(
              Symbol("__label"),
              cypher.Expr.Variable(Symbol("__label_list")),
              cypher.Expr.Equal(cypher.Expr.Variable(Symbol("__label")), cypher.Expr.Str("LABELLED_NODE"))
            )
          ),
          MultipleValuesStandingQuery.LocalProperty(
            Symbol("_LABEL"),
            MultipleValuesStandingQuery.LocalProperty.Any,
            Some(Symbol("__label_list"))
          ),
          dropExisting = true,
          List()
        )
      ),
      emitSubscriptionsLazily = true
    )

    assert(expected === actual)
  }

  test("compiling a cypher pattern containing both `id` and `strId`") {
    val graphPattern = GraphQueryPattern(
      NonEmptyList.of(
        NodePattern(
          NodePatternId(0),
          Set(),
          None,
          Map.empty
        )
      ),
      List(),
      NodePatternId(0),
      Seq(
        ReturnColumn.Property(NodePatternId(0), Symbol("name"), Symbol("n.name")),
        ReturnColumn.Id(NodePatternId(0), false, Symbol("id(n)")),
        ReturnColumn.Id(NodePatternId(0), true, Symbol("strId(n)"))
      ),
      None,
      Nil,
      distinct = false
    )

    val actual = graphPattern.compiledMultipleValuesStandingQuery(labelsProp, IdentityIdProvider)

    val expected = MultipleValuesStandingQuery.Cross(
      ArraySeq(
        MultipleValuesStandingQuery.LocalProperty(
          Symbol("name"),
          MultipleValuesStandingQuery.LocalProperty.Unconditional,
          Some(Symbol("n.name"))
        ),
        MultipleValuesStandingQuery.LocalId(
          Symbol("id(n)"),
          formatAsString = false
        ),
        MultipleValuesStandingQuery.LocalId(
          Symbol("strId(n)"),
          formatAsString = true
        )
      ),
      emitSubscriptionsLazily = true
    )

    assert(expected === actual)
  }
}
