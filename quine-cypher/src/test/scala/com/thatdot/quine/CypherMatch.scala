package com.thatdot.quine

import org.scalatest.AppendedClues
import org.scalatest.matchers.should.Matchers

import com.thatdot.quine.compiler.cypher.CypherHarness
import com.thatdot.quine.graph.cypher.{Columns, CompiledQuery, EntryPoint, Expr, Location, Parameters, Query, Value}

class CypherMatch extends CypherHarness("cypher-match-rewrite-tests") with Matchers with AppendedClues {

  private def normalize[Start <: Location](compiledQuery: CompiledQuery[Start]): CompiledQuery[Start] = {
    val fixedParamsToSubstitute: Map[Expr.Parameter, Value] = compiledQuery.fixedParameters.params.zipWithIndex.map {
      case (paramValue, index) =>
        // unfixedParameters always precede fixedParameters, so the Expr.Parameter referring to a fixed parameters
        // will have "name" offset by the number of unfixed parameters
        Expr.Parameter(index + compiledQuery.unfixedParameters.length) -> paramValue
    }.toMap

    compiledQuery
      .copy(
        query = compiledQuery.query.substitute(fixedParamsToSubstitute),
        fixedParameters = Parameters.empty,
      )
  }

  it("compiles p-exp to return") {
    val compiled = normalize(compiler.cypher.compile("MATCH p=(a) RETURN p", cache = false))
    assert(
      compiled.query === Query.AdjustContext(
        dropExisting = true,
        toAdd = Vector((Symbol("p"), Expr.Variable(Symbol("p")))),
        adjustThis = Query.Return(
          toReturn = Query.AdjustContext(
            dropExisting = false,
            toAdd = Vector((Symbol("p"), Expr.PathExpression(Vector(Expr.Variable(Symbol("a")))))),
            adjustThis = Query.AnchoredEntry(
              entry = EntryPoint.AllNodesScan,
              andThen = Query.LocalNode(
                labelsOpt = Some(Vector()),
                propertiesOpt = None,
                bindName = Some(Symbol("a")),
                columns = Columns.Specified(Vector(Symbol("a"))),
              ),
              columns = Columns.Specified(Vector(Symbol("a"))),
            ),
            columns = Columns.Specified(Vector(Symbol("a"), Symbol("p"))),
          ),
          orderBy = None,
          distinctBy = None,
          drop = None,
          take = None,
          columns = Columns.Specified(Vector(Symbol("a"), Symbol("p"))),
        ),
        columns = Columns.Specified(Vector(Symbol("p"))),
      ),
    )
  }
}
