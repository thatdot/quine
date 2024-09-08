package com.thatdot.quine.compiler.cypher

import scala.annotation.nowarn
import scala.util.Try

import org.apache.pekko.stream.scaladsl.Sink

import org.opencypher.v9_0.expressions.functions.Category
import org.scalatest.AppendedClues
import org.scalatest.Inside.inside
import org.scalatest.matchers.should.Matchers

import com.thatdot.quine.compiler
import com.thatdot.quine.graph.cypher.{
  Aggregator,
  Columns,
  CompiledQuery,
  CypherException,
  Expr,
  Func,
  Location,
  Parameters,
  Query,
  Type,
  UserDefinedFunction,
  UserDefinedFunctionSignature,
  Value,
}
import com.thatdot.quine.model.QuineIdProvider
import com.thatdot.quine.util.Log._

// disable warnings raised by giving names to parts of pattern matches to make the large pattern matches more readable
@nowarn(
  "msg=pattern var .+ in method .+ is never used",
)
class CypherReturn extends CypherHarness("cypher-return-tests") with Matchers with AppendedClues {
  val XSym: Symbol = Symbol("x")
  val SumXSym: Symbol = Symbol("sum(x)")
  val queryPrefix = "UNWIND range(0,3) AS x "
  val QueryPrefixCompiled: Query.Unwind[Location.Anywhere] = Query.Unwind(
    Expr.Function(
      Func.Range,
      Vector(
        Expr.Integer(0),
        Expr.Integer(3),
      ),
    ),
    XSym,
    Query.Unit(Columns.Omitted),
    Columns.Specified(Vector(XSym)),
  )

  // utility to normalize a CompiledQuery by removing as many [[Expr.Parameters]] as possible -- ie, the fixed parameters
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

  describe("non-aggregating RETURN") {
    it("compiles a direct RETURN") {
      val compiled = normalize(compiler.cypher.compile(queryPrefix + "RETURN x", cache = false))
      assert(
        compiled.query === Query.AdjustContext(
          dropExisting = true,
          Vector(XSym -> Expr.Variable(XSym)),
          Query.Return(
            QueryPrefixCompiled,
            None,
            None,
            None,
            None,
            Columns.Specified(Vector(XSym)),
          ),
          Columns.Specified(Vector(XSym)),
        ),
      )
    }
    testQuery(
      queryPrefix + "RETURN x",
      expectedColumns = Vector("x"),
      expectedRows = Seq(
        Vector(Expr.Integer(0)),
        Vector(Expr.Integer(1)),
        Vector(Expr.Integer(2)),
        Vector(Expr.Integer(3)),
      ),
    )
    it("compiles a paginated return") {
      val compiled = normalize(compiler.cypher.compile(queryPrefix + "RETURN x SKIP 2 LIMIT 2", cache = false))
      assert(
        compiled.query === Query.AdjustContext(
          dropExisting = true,
          Vector(XSym -> Expr.Variable(XSym)),
          Query.Return(
            QueryPrefixCompiled,
            None,
            None,
            drop = Some(Expr.Integer(2)),
            take = Some(Expr.Integer(2)),
            Columns.Specified(Vector(XSym)),
          ),
          Columns.Specified(Vector(XSym)),
        ),
      )
    }
    testQuery(
      queryPrefix + "RETURN x SKIP 2 LIMIT 2",
      expectedColumns = Vector("x"),
      expectedRows = Seq(
        Vector(Expr.Integer(2)),
        Vector(Expr.Integer(3)),
      ),
    )
    it("compiles a SKIP with a parameter") {
      val compiled = normalize(
        compiler.cypher
          .compile(queryPrefix + "RETURN x SKIP $skipThisMany", cache = false, unfixedParameters = Seq("skipThisMany")),
      )

      compiled.query should matchPattern {
        case Query.AdjustContext(
              true,
              Vector((XSym, Expr.Variable(XSym))),
              Query.Return(
                QueryPrefixCompiled,
                None,
                None,
                drop @ Some(Expr.Parameter(_)),
                None,
                Columns.Specified(Vector(XSym)),
              ),
              Columns.Specified(Vector(XSym)),
            ) =>
      }
    }
    testQuery(
      queryPrefix + "RETURN x SKIP $skipThisMany",
      expectedColumns = Vector("x"),
      expectedRows = Seq(
        Vector(Expr.Integer(1)),
        Vector(Expr.Integer(2)),
        Vector(Expr.Integer(3)),
      ),
      parameters = Map("skipThisMany" -> Expr.Integer(1)),
    )
    it("compiles a nontrivial SKIP with a parameter") {
      val compiled = normalize(
        compiler.cypher
          .compile(queryPrefix + "RETURN x SKIP $lastSkip + 10", cache = false, unfixedParameters = Seq("lastSkip")),
      )

      compiled.query should matchPattern {
        case Query.AdjustContext(
              true,
              Vector((XSym, Expr.Variable(XSym))),
              Query.Return(
                QueryPrefixCompiled,
                None,
                None,
                drop @ Some(Expr.Add(Expr.Parameter(_), Expr.Integer(10))),
                None,
                Columns.Specified(Vector(XSym)),
              ),
              Columns.Specified(Vector(XSym)),
            ) =>
      }
    }
    testQuery(
      queryPrefix + "RETURN x SKIP $lastSkip + 10",
      expectedColumns = Vector("x"),
      expectedRows = Seq(
        Vector(Expr.Integer(1)),
        Vector(Expr.Integer(2)),
        Vector(Expr.Integer(3)),
      ),
      parameters = Map("lastSkip" -> Expr.Integer(-9)),
    )
    it("compiles a LIMIT with a parameter") {
      val compiled = normalize(
        compiler.cypher
          .compile(
            queryPrefix + "RETURN x LIMIT $yieldThisMany",
            cache = false,
            unfixedParameters = Seq("yieldThisMany"),
          ),
      )

      compiled.query should matchPattern {
        case Query.AdjustContext(
              true,
              Vector((XSym, Expr.Variable(XSym))),
              Query.Return(
                QueryPrefixCompiled,
                None,
                None,
                None,
                take @ Some(Expr.Parameter(_)),
                Columns.Specified(Vector(XSym)),
              ),
              Columns.Specified(Vector(XSym)),
            ) =>
      }
    }
    testQuery(
      queryPrefix + "RETURN x LIMIT $yieldThisMany",
      expectedColumns = Vector("x"),
      expectedRows = Seq(
        Vector(Expr.Integer(0)),
        Vector(Expr.Integer(1)),
        Vector(Expr.Integer(2)),
      ),
      parameters = Map("yieldThisMany" -> Expr.Integer(3)),
    )
    it("compiles a DISTINCT return") {
      val compiled = normalize(compiler.cypher.compile(queryPrefix + "RETURN DISTINCT x", cache = false))
      assert(
        compiled.query === Query.AdjustContext(
          dropExisting = true,
          Vector(XSym -> Expr.Variable(XSym)),
          Query.Return(
            QueryPrefixCompiled,
            None,
            distinctBy = Some(Seq(Expr.Variable(XSym))),
            drop = None,
            take = None,
            Columns.Specified(Vector(XSym)),
          ),
          Columns.Specified(Vector(XSym)),
        ),
      )
    }
    testQuery(
      queryPrefix + "RETURN DISTINCT x",
      expectedColumns = Vector("x"),
      expectedRows = Seq(
        Vector(Expr.Integer(0)),
        Vector(Expr.Integer(1)),
        Vector(Expr.Integer(2)),
        Vector(Expr.Integer(3)),
      ),
    )
    it("compiles an ORDERed return") {
      val compiled = normalize(compiler.cypher.compile(queryPrefix + "RETURN x ORDER BY x DESC", cache = false))
      assert(
        compiled.query === Query.AdjustContext(
          dropExisting = true,
          Vector(XSym -> Expr.Variable(XSym)),
          Query.Return(
            QueryPrefixCompiled,
            orderBy = Some(Seq(Expr.Variable(XSym) -> false)),
            distinctBy = None,
            drop = None,
            take = None,
            Columns.Specified(Vector(XSym)),
          ),
          Columns.Specified(Vector(XSym)),
        ),
      )
    }
    testQuery(
      queryPrefix + "RETURN x ORDER BY x DESC",
      expectedColumns = Vector("x"),
      expectedRows = Seq(
        Vector(Expr.Integer(3)),
        Vector(Expr.Integer(2)),
        Vector(Expr.Integer(1)),
        Vector(Expr.Integer(0)),
      ),
    )
    it("compiles an ORDERed DISTINCT return") {
      pending
      val compiled = normalize(compiler.cypher.compile(queryPrefix + "RETURN DISTINCT x ORDER BY x ASC", cache = false))

      // TODO This query ideally would compile as follows, but due to OC's naming deduplication, it does not. Instead,
      // the query compiles nondeterministically (varying in variable names). These optimizations seem to kick in when
      // a query uses ORDER BY
      assert(
        compiled.query === Query.AdjustContext(
          dropExisting = true,
          Vector(XSym -> Expr.Variable(XSym)),
          Query.Return(
            QueryPrefixCompiled,
            orderBy = Some(Seq(Expr.Variable(XSym) -> true)),
            distinctBy = Some(Seq(Expr.Variable(XSym))),
            drop = None,
            take = None,
            Columns.Specified(Vector(XSym)),
          ),
          Columns.Specified(Vector(XSym)),
        ),
      )
    }
    testQuery(
      queryPrefix + "RETURN DISTINCT x ORDER BY x ASC",
      expectedColumns = Vector("x"),
      expectedRows = Seq(
        Vector(Expr.Integer(0)),
        Vector(Expr.Integer(1)),
        Vector(Expr.Integer(2)),
        Vector(Expr.Integer(3)),
      ),
    )
    it("rejects queries using incorrect scopes for SKIP or LIMIT") {
      pending
      assertThrows[CypherException.Compile](
        compiler.cypher
          .compile(
            queryPrefix + "RETURN x SKIP x",
            cache = false,
          ),
      ).withClue("should not allow using variables from the main query in the SKIP/LIMIT")
      assertThrows[CypherException.Compile](
        compiler.cypher
          .compile(
            queryPrefix + "RETURN x SKIP sum(x)",
            cache = false,
          ),
      )
        .withClue(
          "should not allow using aggregated variables from the main query in the SKIP/LIMIT",
        )
      assertThrows[CypherException.Compile](
        compiler.cypher
          .compile(
            queryPrefix + "RETURN x LIMIT x+1",
            cache = false,
          ),
      ).withClue(
        "should not allow using variable from the main query in the SKIP/LIMIT, even in complex expressions",
      )
      assertThrows[CypherException.Compile](
        compiler.cypher
          .compile(
            queryPrefix + "RETURN x SKIP count(*) - 1",
            cache = false,
          ),
      ).withClue(
        "should not allow using non-variable aggregations from the main query in the SKIP/LIMIT",
      )
    }
    it("does not leak DISTINCT/ORDER BY-scoped variables into SKIP/LIMIT scope") {
      assertThrows[CypherException.Compile](
        compiler.cypher.compile(queryPrefix + "RETURN DISTINCT x ORDER BY x ASC SKIP x", cache = false),
      ).withClue(
        "variables used in DISTINCT/ORDER BY should not be usable by SKIP",
      )
      assertThrows[CypherException.Compile](
        compiler.cypher.compile(queryPrefix + "RETURN DISTINCT x ORDER BY x ASC LIMIT x", cache = false),
      ).withClue(
        "variables used in DISTINCT/ORDER BY should not be usable by LIMIT",
      )
      assertThrows[CypherException.Compile](
        compiler.cypher.compile(queryPrefix + "RETURN DISTINCT x ORDER BY x ASC SKIP sum(x)", cache = false),
      ).withClue(
        "variables used in DISTINCT/ORDER BY should not be usable in aggregate by SKIP",
      )
      assertThrows[CypherException.Compile](
        compiler.cypher.compile(queryPrefix + "RETURN DISTINCT x ORDER BY x ASC LIMIT sum(x)", cache = false),
      ).withClue(
        "variables used in DISTINCT/ORDER BY should not be usable in aggregate by LIMIT",
      )
      assertThrows[CypherException.Compile](
        compiler.cypher.compile(queryPrefix + "RETURN DISTINCT x ORDER BY x ASC SKIP x+1", cache = false),
      ).withClue(
        "variables used in DISTINCT/ORDER BY should not be usable in nontrivial expressions by SKIP",
      )
    }
  }

  describe("aggregating RETURN") {
    it("compiles a direct RETURN") {
      val compiled = normalize(compiler.cypher.compile(queryPrefix + "RETURN sum(x)", cache = false))
      assert(
        compiled.query ===
          Query.Return(
            Query.AdjustContext(
              dropExisting = true,
              Vector(SumXSym -> Expr.Variable(SumXSym)),
              Query.EagerAggregation(
                Vector(),
                Vector(SumXSym -> Aggregator.sum(distinct = false, Expr.Variable(XSym))),
                QueryPrefixCompiled,
                keepExisting = false,
                Columns.Specified(Vector(SumXSym)),
              ),
              Columns.Specified(Vector(SumXSym)),
            ),
            None,
            None,
            None,
            None,
            Columns.Specified(Vector(SumXSym)),
          ),
      )
    }
    testQuery(
      queryPrefix + "RETURN sum(x)",
      expectedColumns = Vector("sum(x)"),
      expectedRows = Seq(
        Vector(Expr.Integer(6)),
      ),
    )
    it("compiles a paginated return") {
      // yes, this query is semantically nonsense... but it should still compile, so that's good enough
      val compiled = normalize(compiler.cypher.compile(queryPrefix + "RETURN sum(x) SKIP 5 LIMIT 1", cache = false))
      assert(
        compiled.query ===
          Query.Return(
            Query.AdjustContext(
              dropExisting = true,
              Vector(SumXSym -> Expr.Variable(SumXSym)),
              Query.EagerAggregation(
                Vector(),
                Vector(SumXSym -> Aggregator.sum(distinct = false, Expr.Variable(XSym))),
                QueryPrefixCompiled,
                keepExisting = false,
                Columns.Specified(Vector(SumXSym)),
              ),
              Columns.Specified(Vector(SumXSym)),
            ),
            None,
            None,
            drop = Some(Expr.Integer(5)),
            take = Some(Expr.Integer(1)),
            Columns.Specified(Vector(SumXSym)),
          ),
      )
    }
    testQuery(
      queryPrefix + "RETURN sum(x) SKIP 5 LIMIT 1",
      expectedColumns = Vector("sum(x)"),
      expectedRows = Seq(),
    )
    it("compiles a nontrivial SKIP with a parameter") {
      val compiled = normalize(
        compiler.cypher
          .compile(
            queryPrefix + "RETURN sum(x) SKIP $lastSkip + 12",
            cache = false,
            unfixedParameters = Seq("lastSkip"),
          ),
      )

      compiled.query should matchPattern {
        case Query.Return(
              Query.AdjustContext(
                dropExisting @ true,
                Vector((SumXSym, Expr.Variable(SumXSym))),
                Query.EagerAggregation(
                  Vector(),
                  Vector((SumXSym, Aggregator.sum(distinct @ false, Expr.Variable(XSym)))),
                  QueryPrefixCompiled,
                  keepExisting @ false,
                  Columns.Specified(Vector(SumXSym)),
                ),
                Columns.Specified(Vector(SumXSym)),
              ),
              None,
              None,
              drop @ Some(Expr.Add(Expr.Parameter(_), Expr.Integer(12))),
              None,
              Columns.Specified(Vector(SumXSym)),
            ) =>
      }
    }
    testQuery(
      queryPrefix + "RETURN sum(x) SKIP $lastSkip + 12",
      expectedColumns = Vector("sum(x)"),
      expectedRows = Seq(
        Vector(Expr.Integer(6)),
      ),
      parameters = Map("lastSkip" -> Expr.Integer(-12)),
    )
    it("compiles an ORDERed return that operates over both aggregated and non-aggregated data") {
      // These queries are reaching Lewis Carroll levels of ludicrousness
      val compiled =
        normalize(compiler.cypher.compile(queryPrefix + "RETURN x, sum(x) ORDER BY x DESC, sum(x) ASC", cache = false))

      /** when returning ordered rows (ORDER BY), opencypher is aggressively "helpful" in deduplicating column
        * names. Unfortunately, the deduplicated names do not appear consistent across runs, meaning that these queries
        * are harder to statically analyze (eg, in a unit test). The most important part of this test is the structure
        * verification (ie, that the "inside" blocks match). Some auxilliary assertions (like "column names are
        * preserved") have been added, but the only stand-out important "assert" statement is the assertion that
        * the EagerAggregation uses the same column to calculate both return items. This assertion reads as follows:
        *
        *     assert(generatedMappedX === generatedAggregatedX)
        */
      inside(compiled.query) {
        case Query.AdjustContext(
              dropExisting @ true,
              toAdd @ Vector((XSym, Expr.Variable(generatedXSymbol)), (SumXSym, Expr.Variable(SumXSym))),
              adjustThis,
              columns,
            ) =>
          // extract the name openCypher chose to give the "x" symbol during compilation
          val IntermediateXSym = generatedXSymbol
          assert(
            columns === Columns.Specified(Vector(XSym, SumXSym)),
            "the outermost query should return 2 columns: x and sum(x)",
          )
          inside(adjustThis) {
            case Query.Return(
                  Query.Sort(
                    by @ Vector((Expr.Variable(IntermediateXSym), false), (Expr.Variable(SumXSym), true)),
                    Query.AdjustContext(
                      dropExisting @ true,
                      toAdd @ Vector(
                        (IntermediateXSym, Expr.Variable(IntermediateXSym)),
                        (SumXSym, Expr.Variable(SumXSym)),
                      ),
                      toAdjust,
                      adjustedColumns,
                    ),
                    sortColumns,
                  ),
                  None,
                  None,
                  None,
                  None,
                  returnColumns,
                ) =>
              assert(sortColumns === Columns.Specified(Vector(IntermediateXSym, SumXSym)))
              assert(returnColumns === Columns.Specified(Vector(IntermediateXSym, SumXSym)))
              assert(adjustedColumns === Columns.Specified(Vector(IntermediateXSym, SumXSym)))
              inside(toAdjust) {
                case Query.EagerAggregation(
                      aggregateAlong @ Vector((IntermediateXSym, Expr.Variable(generatedMappedX))),
                      aggregateWith @ Vector(
                        (SumXSym, Aggregator.sum(distinct @ false, Expr.Variable(generatedAggregatedX))),
                      ),
                      toAggregate,
                      keepExisting @ false,
                      aggregatedColumns,
                    ) =>
                  /** generatedMappedX is the column OC is renaming to IntermediateXSym
                    * generatedAggregatedX is the column OC is passing to the "sum" aggregator
                    * these are both the same x, so they should match (and be some variant of Symbol(x))
                    */
                  assert(generatedMappedX === generatedAggregatedX)
                  val UnprojectedXSym = generatedAggregatedX

                  assert(aggregatedColumns === Columns.Specified(Vector(IntermediateXSym, SumXSym)))

                  /** at this point, the behavior of RETURN is verified, but while we're here, let's make sure the
                    * innermost query is sane. This is an alpha-conversion of QueryPrefixCompiled (replacing XSym
                    * with UnprojectedXSym)
                    */
                  assert(
                    toAggregate === Query.Unwind(
                      Expr.Function(
                        Func.Range,
                        Vector(
                          Expr.Integer(0),
                          Expr.Integer(3),
                        ),
                      ),
                      UnprojectedXSym,
                      Query.Unit(Columns.Omitted),
                      Columns.Specified(Vector(UnprojectedXSym)),
                    ),
                  )
              }
          }
      }
    }
    testQuery(
      queryPrefix + "RETURN x, sum(x) ORDER BY x DESC, sum(x) ASC",
      expectedColumns = Vector("x", "sum(x)"),
      expectedRows = Seq(
        Vector(Expr.Integer(3), Expr.Integer(3)),
        Vector(Expr.Integer(2), Expr.Integer(2)),
        Vector(Expr.Integer(1), Expr.Integer(1)),
        Vector(Expr.Integer(0), Expr.Integer(0)),
      ),
    )
    it("compiles an ORDERed DISTINCT return") {
      val compiled =
        normalize(compiler.cypher.compile(queryPrefix + "RETURN DISTINCT sum(x) ORDER BY sum(x) ASC", cache = false))
      assert(
        compiled.query ===
          Query.Return( // TODO this Sort can be optimized out in cases like this (ie, when the ORDER BY contains only aggregated returnItems)
            Query.Sort(
              by = Vector(Expr.Variable(SumXSym) -> true),
              toSort = Query.AdjustContext(
                dropExisting = true,
                Vector(SumXSym -> Expr.Variable(SumXSym)),
                Query.EagerAggregation(
                  Vector(),
                  Vector(SumXSym -> Aggregator.sum(distinct = false, Expr.Variable(XSym))),
                  QueryPrefixCompiled,
                  keepExisting = false,
                  Columns.Specified(Vector(SumXSym)),
                ),
                Columns.Specified(Vector(SumXSym)),
              ),
              Columns.Specified(Vector(SumXSym)),
            ),
            orderBy = None, // handled in the above Sort
            distinctBy = Some(Seq(Expr.Variable(SumXSym))),
            None,
            None,
            Columns.Specified(Vector(SumXSym)),
          ),
      )
    }
    testQuery(
      queryPrefix + "RETURN DISTINCT sum(x) ORDER BY sum(x) ASC",
      expectedColumns = Vector("sum(x)"),
      expectedRows = Seq(
        Vector(Expr.Integer(6)),
      ),
    )
    it("rejects an ORDERing over unprojected columns") {
      assertThrows[CypherException.Compile](
        compiler.cypher.compile(queryPrefix + "RETURN sum(x) ORDER BY x ASC", cache = false),
      )
    }
    it("rejects queries using incorrect scopes for SKIP or LIMIT") {
      pending
      assertThrows[CypherException.Compile](
        compiler.cypher
          .compile(
            queryPrefix + "RETURN x, sum(x) SKIP x",
            cache = false,
          ),
      ).withClue(
        "should not allow using variables from the main query in the SKIP/LIMIT",
      )
      assertThrows[CypherException.Compile](
        compiler.cypher
          .compile(
            queryPrefix + "RETURN x SKIP sum(x)",
            cache = false,
          ),
      ).withClue(
        "should not allow using aggregated variables from the main query in the SKIP/LIMIT",
      )
      assertThrows[CypherException.Compile](
        compiler.cypher
          .compile(
            queryPrefix + "RETURN x, sum(x) LIMIT x+1",
            cache = false,
          ),
      ).withClue(
        "should not allow using variable from the main query in the SKIP/LIMIT, even in complex expressions",
      )
      assertThrows[CypherException.Compile](
        compiler.cypher
          .compile(
            queryPrefix + "RETURN x, sum(x) SKIP count(*) - 1",
            cache = false,
          ),
      ).withClue(
        "should not allow using non-variable aggregations from the main query in the SKIP/LIMIT",
      )
      assertThrows[CypherException.Compile](
        compiler.cypher
          .compile(
            queryPrefix + "RETURN x, sum(x) SKIP sum(x) - 1",
            cache = false,
          ),
      ).withClue(
        "should not allow using variable aggregations from the main query in the SKIP/LIMIT",
      )
      assertThrows[CypherException.Compile](
        compiler.cypher
          .compile(
            queryPrefix + "RETURN x, sum(x) ORDER BY x SKIP x",
            cache = false,
          ),
      ).withClue(
        "ORDER BY should not leak variables to non-ORDER BY clauses from the main query (SKIP)",
      )
      assertThrows[CypherException.Compile](
        compiler.cypher
          .compile(
            queryPrefix + "RETURN x, sum(x) ORDER BY x LIMIT x",
            cache = false,
          ),
      ).withClue(
        "ORDER BY should not leak variables to non-ORDER BY clauses from the main query (LIMIT)",
      )
    }
  }
  describe("doesn't throw the kitchen sink") {

    /** TODO matching for a specific structure here has some marginal value, but given the complexity of
      *   "compiles an ORDERed return that operates over both aggregated and non-aggregated data" such a test may
      *   not be possible to write in a coherent way. asserting no-throws is much weaker, but it's easy to follow
      */

    val kitchenSink =
      queryPrefix + "RETURN DISTINCT x, sum(x), collect(distinct $returnThisStatically) AS r ORDER BY x DESC, r ASC, sum(x) ASC SKIP $skipThisMany LIMIT $limitMoreThanThis + 2"
    val attemptedCompilation = Try(
      compiler.cypher.compile(
        kitchenSink,
        cache = false,
        unfixedParameters = Seq("skipThisMany", "limitMoreThanThis", "returnThisStatically"),
      ),
    )
    it("should compile") {
      assert(attemptedCompilation.isSuccess)
    }
    // verify its results
    testQuery(
      query = kitchenSink,
      expectedColumns = Vector("x", "sum(x)", "r"),
      expectedRows = Seq(
        Vector(Expr.Integer(2), Expr.Integer(2), Expr.List(Expr.Str("hello"))),
        Vector(Expr.Integer(1), Expr.Integer(1), Expr.List(Expr.Str("hello"))),
      ),
      parameters = Map(
        "skipThisMany" -> Expr.Integer(1),
        "limitMoreThanThis" -> Expr.Integer(0),
        "returnThisStatically" -> Expr.Str("hello"),
      ),
    )
  }

  registerUserDefinedFunction(SnitchFunction)
  it("only evaluates projected expressions once") {
    SnitchFunction.reset()
    val theQuery = queryCypherValues("RETURN snitch(rand())", cypherHarnessNamespace)(graph)
    assert(theQuery.columns === Vector(Symbol("snitch(rand())")))
    theQuery.results.runWith(Sink.seq).map { results =>
      assert(results.length === 1, "query only requests 1 row")
      val row = results.head
      assert(row.length === 1, "query only requests 1 value")
      val returnedResult = row.head
      assert(SnitchFunction.snitched.size() === 1, "the RETURNed expression should be computed only once")
      val snitchedResult = SnitchFunction.snitched.poll()
      assert(returnedResult === snitchedResult, "the RETURNed value should match the value extracted via side-channel")
    }
  }
}
// Test function to "snitch" (ie, side-channel out) all values passed through it
object SnitchFunction extends UserDefinedFunction {
  val snitched = new java.util.concurrent.ConcurrentLinkedQueue[Value]()

  def reset(): Unit = snitched.clear()

  val name = "snitch"

  val isPure = false

  val category = Category.SCALAR

  val signatures: Vector[UserDefinedFunctionSignature] = Vector(
    UserDefinedFunctionSignature(
      arguments = Vector("input" -> Type.Anything),
      output = Type.Anything,
      description = "Returns the value provided after snitching it",
    ),
  )

  def call(args: Vector[Value])(implicit idp: QuineIdProvider, logConfig: LogConfig): Value =
    args match {
      case Vector(x) => snitched.add(x); x
      case _ => throw new Exception("This should never happen!")
    }
}
