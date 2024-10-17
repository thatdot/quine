package com.thatdot.quine.graph.cypher

import cats.data.{NonEmptyList, State}
import cats.implicits._
import com.google.common.collect.Interners

import com.thatdot.cypher.phases.SymbolAnalysisModule.SymbolTable
import com.thatdot.language.ast._

sealed trait BinOp

object BinOp {
  case object Merge extends BinOp
  case object Append extends BinOp
}

sealed trait QuinePattern

sealed trait Output

object QuinePattern {
  private val interner = Interners.newWeakInterner[QuinePattern]()

  case object QuineUnit extends QuinePattern

  case class Node(binding: Identifier) extends QuinePattern
  case class Edge(edgeLabel: Symbol, remotePattern: QuinePattern) extends QuinePattern

  case class Fold(init: QuinePattern, over: List[QuinePattern], f: BinOp, output: Output) extends QuinePattern

  def mkNode(binding: Identifier): QuinePattern =
    interner.intern(Node(binding))

  def mkEdge(edgeLabel: Symbol, remotePattern: QuinePattern): QuinePattern =
    interner.intern(Edge(edgeLabel, remotePattern))

  def mkFold(init: QuinePattern, over: List[QuinePattern], f: BinOp, output: Output): QuinePattern =
    interner.intern(Fold(interner.intern(init), over.map(interner.intern), f, output))
}

object Compiler {

  def expressionFromDesc(exp: com.thatdot.language.ast.Expression): Expr = exp match {
    case Expression.Apply(_, name, args, _) =>
      val f = Func.builtinFunctions.find(_.name == name.name).get
      Expr.Function(f, args.map(expressionFromDesc).toVector)
    case Expression.BinOp(_, op, lhs, rhs, _) =>
      op match {
        case Operator.Plus => Expr.Add(expressionFromDesc(lhs), expressionFromDesc(rhs))
        case _ => ???
      }
    case com.thatdot.language.ast.Expression.Ident(_, name, _) => Expr.Variable(name.name)
    case _ =>
      println(exp)
      ???
  }

  sealed trait DependencyGraph

  object DependencyGraph {
    case class Independent(steps: NonEmptyList[DependencyGraph]) extends DependencyGraph
    case class Dependent(first: DependencyGraph, second: DependencyGraph) extends DependencyGraph
    case class Step(instruction: Instruction) extends DependencyGraph
    case object Empty extends DependencyGraph

    def empty: DependencyGraph = Empty
  }

  case class DependencyGraphState(
    graph: DependencyGraph,
    deferred: List[(Set[Identifier], Instruction)],
    symbolTable: SymbolTable,
  )

  type DependencyGraphProgram[A] = State[DependencyGraphState, A]

  def pure[A](a: A): DependencyGraphProgram[A] = State.pure(a)
  def inspect[A](view: DependencyGraphState => A): DependencyGraphProgram[A] = State.inspect(view)
  def mod(update: DependencyGraphState => DependencyGraphState): DependencyGraphProgram[Unit] =
    State.modify(update)
  def noop: DependencyGraphProgram[Unit] = pure(())

  def analyzeExpressionDeps(expression: Expression): DependencyGraphProgram[Set[Identifier]] =
    expression match {
      case apply: Expression.Apply =>
        apply.args.foldM(Set.empty[Identifier])((deps, exp) => analyzeExpressionDeps(exp).map(deps union _))
      case _: Expression.AtomicLiteral => pure(Set.empty[Identifier])
      case binary: Expression.BinOp =>
        for {
          leftDeps <- analyzeExpressionDeps(binary.lhs)
          rightDeps <- analyzeExpressionDeps(binary.rhs)
        } yield leftDeps union rightDeps
      case id: Expression.Ident => pure(Set(id.identifier))
      case _ =>
        println(expression)
        ???
    }

  def analyzeInstructionDeps(instruction: Instruction): DependencyGraphProgram[Set[Identifier]] =
    instruction match {
      case _: Instruction.LocalNode => pure(Set.empty)
      case proj: Instruction.Proj => analyzeExpressionDeps(proj.expression)
      case filter: Instruction.Filter => analyzeExpressionDeps(filter.expression)
      case _ =>
        println(instruction)
        ???
    }

  def analyzeInstructionIntros(instruction: Instruction): Set[Identifier] = instruction match {
    case local: Instruction.LocalNode => Set(local.binding)
    case _: Instruction.Proj => Set.empty
    case _ =>
      println(instruction)
      ???
  }

  val tryDeferred: DependencyGraphProgram[Unit] =
    inspect(_.deferred) >>= (deferred => deferred.sortBy(_._1.size).traverse_(p => insertIntoGraph(p._2)))

  def defer(instruction: Instruction, deps: Set[Identifier]): DependencyGraphProgram[Unit] =
    mod(st => st.copy(deferred = st.deferred :+ (deps -> instruction)))

  def reduceDeps(
    instruction: Instruction,
    steps: List[DependencyGraph],
    unused: List[DependencyGraph],
    used: List[DependencyGraph],
    deps: Set[Identifier],
  ): (Set[Identifier], DependencyGraph) =
    steps match {
      case Nil =>
        deps -> (if (unused.isEmpty) DependencyGraph.Empty
                 else DependencyGraph.Independent(NonEmptyList.fromListUnsafe(unused)))
      case h :: t =>
        val tryInsertAtHead = depthFirstInsertPure(instruction, h, deps)
        if (tryInsertAtHead._1.isEmpty) {
          val independentSteps = unused ::: t
          val comesFirst = DependencyGraph.Independent(NonEmptyList.fromListUnsafe(used))
          val depStep = DependencyGraph.Dependent(comesFirst, tryInsertAtHead._2)
          val resultStep = DependencyGraph.Independent(NonEmptyList.fromListUnsafe(independentSteps :+ depStep))
          Set.empty[Identifier] -> resultStep
        } else {
          if (tryInsertAtHead._1.size < deps.size) {
            reduceDeps(instruction, t, unused, h :: used, tryInsertAtHead._1)
          } else {
            reduceDeps(instruction, t, h :: unused, used, deps)
          }
        }
    }

  def depthFirstInsertPure(
    instruction: Instruction,
    graph: DependencyGraph,
    deps: Set[Identifier],
  ): (Set[Identifier], DependencyGraph) =
    graph match {
      case ind: DependencyGraph.Independent =>
        if (deps.isEmpty) {
          deps -> DependencyGraph.Independent(NonEmptyList.of(ind, DependencyGraph.Step(instruction)))
        } else {
          reduceDeps(instruction, ind.steps.toList, Nil, Nil, deps)
        }
      case dep: DependencyGraph.Dependent =>
        if (deps.isEmpty) {
          deps -> DependencyGraph.Independent(NonEmptyList.of(dep, DependencyGraph.Step(instruction)))
        } else {
          val tryInsertFirst = depthFirstInsertPure(instruction, dep.first, deps)
          if (tryInsertFirst._1.isEmpty) {
            Set.empty[Identifier] -> DependencyGraph.Dependent(
              dep.first,
              DependencyGraph.Independent(NonEmptyList.of(dep.second, tryInsertFirst._2)),
            )
          } else {
            val unmet = tryInsertFirst._1
            val tryInsertSecond = depthFirstInsertPure(instruction, dep.second, unmet)
            if (tryInsertSecond._1.isEmpty) {
              Set.empty[Identifier] -> DependencyGraph.Dependent(
                dep.first,
                DependencyGraph.Dependent(dep.second, tryInsertSecond._2),
              )
            } else {
              val remaining = tryInsertSecond._1
              remaining -> dep
            }
          }
        }
      case step: DependencyGraph.Step =>
        if (deps.isEmpty) {
          deps -> DependencyGraph.Independent(NonEmptyList.of(step, DependencyGraph.Step(instruction)))
        } else {
          val intros = analyzeInstructionIntros(step.instruction)
          val unmet = deps diff intros
          if (unmet.isEmpty)
            unmet -> DependencyGraph.Dependent(step, DependencyGraph.Step(instruction))
          else
            unmet -> step
        }
      case DependencyGraph.Empty =>
        deps -> (if (deps.isEmpty) DependencyGraph.Step(instruction) else DependencyGraph.Empty)
    }

  def depthFirstInsert(
    instruction: Instruction,
    deps: Set[Identifier],
  ): DependencyGraphProgram[Set[Identifier]] =
    for {
      graph <- inspect(_.graph)
      insertResult = depthFirstInsertPure(instruction, graph, deps)
      _ <- mod(st => st.copy(graph = insertResult._2)).whenA(insertResult._1.isEmpty)
    } yield insertResult._1

  def insertIntoGraph(instruction: Instruction): DependencyGraphProgram[Unit] =
    for {
      deps <- analyzeInstructionDeps(instruction)
      unmetDeps <- depthFirstInsert(instruction, deps)
      _ <- if (unmetDeps.isEmpty) tryDeferred else defer(instruction, unmetDeps)
    } yield ()

  def buildDependencyGraph(program: List[Instruction]): DependencyGraphProgram[Unit] =
    program.traverse_(insertIntoGraph)

  def instructionToQuinePattern(instruction: Instruction): QuinePattern =
    instruction match {
      case Instruction.Filter(_) => QuinePattern.QuineUnit
      case Instruction.LocalNode(binding) => QuinePattern.mkNode(binding)
      case Instruction.Proj(_, _) => QuinePattern.QuineUnit
      case _ =>
        println(instruction)
        ???
    }

  def compileFromDependencyGraph(graph: DependencyGraph, symbolTable: SymbolTable): QuinePattern =
    graph match {
      case DependencyGraph.Independent(steps) =>
        QuinePattern.mkFold(
          init = QuinePattern.QuineUnit,
          over = steps.toList.map(step => compileFromDependencyGraph(step, symbolTable)),
          f = BinOp.Merge,
          output = null,
        )
      case DependencyGraph.Dependent(first, second) =>
        QuinePattern.mkFold(
          init = compileFromDependencyGraph(first, symbolTable),
          over = List(compileFromDependencyGraph(second, symbolTable)),
          f = BinOp.Merge,
          output = null,
        )
      case DependencyGraph.Step(instruction) => instructionToQuinePattern(instruction)
      case DependencyGraph.Empty => QuinePattern.QuineUnit
    }

  def compile(program: List[Instruction], symbolTable: SymbolTable): QuinePattern = {
    val dependencyGraph =
      buildDependencyGraph(program).runS(DependencyGraphState(DependencyGraph.empty, Nil, symbolTable)).value
    if (dependencyGraph.deferred.nonEmpty) {
      sys.error(s"Unresolved dependencies! ${dependencyGraph.deferred}")
    }
    compileFromDependencyGraph(dependencyGraph.graph, symbolTable)
  }
}
