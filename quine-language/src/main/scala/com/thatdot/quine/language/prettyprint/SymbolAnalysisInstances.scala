package com.thatdot.quine.language.prettyprint

import com.thatdot.quine.cypher.phases.SymbolAnalysisModule._
import com.thatdot.quine.cypher.phases.SymbolAnalysisState
import com.thatdot.quine.language.diagnostic.Diagnostic

trait SymbolAnalysisInstances extends CypherASTInstances {
  import Doc._

  implicit val diagnosticPrettyPrint: PrettyPrint[Diagnostic] =
    PrettyPrint.instance {
      case Diagnostic.ParseError(line, char, message) =>
        concat(text(s"ParseError($line:$char): "), text(message))
      case Diagnostic.SymbolAnalysisWarning(message) =>
        concat(text("SymbolAnalysisWarning: "), text(message))
      case Diagnostic.SymbolAnalysisError(message) =>
        concat(text("SymbolAnalysisError: "), text(message))
      case Diagnostic.TypeCheckError(message) =>
        concat(text("TypeCheckError: "), text(message))
    }

  implicit lazy val symbolTableEntryPrettyPrint: PrettyPrint[SymbolTableEntry] =
    PrettyPrint.instance {
      case SymbolTableEntry.NodeEntry(source, identifier, labels, maybeProperties) =>
        val propsDoc = maybeProperties match {
          case Some(props) => concat(text(", properties = "), expressionPrettyPrint.doc(props))
          case None => empty
        }
        concat(
          text("NodeEntry("),
          nest(
            1,
            concat(
              line,
              text("id = "),
              text(identifier.toString),
              text(","),
              line,
              text("labels = "),
              setPrettyPrint[Symbol].doc(labels),
              propsDoc,
              text(","),
              line,
              text("source = "),
              sourcePrettyPrint.doc(source),
            ),
          ),
          line,
          text(")"),
        )

      case SymbolTableEntry.EdgeEntry(source, identifier, edgeType, direction) =>
        concat(
          text("EdgeEntry("),
          nest(
            1,
            concat(
              line,
              text("id = "),
              text(identifier.toString),
              text(","),
              line,
              text("edgeType = "),
              symbolPrettyPrint.doc(edgeType),
              text(","),
              line,
              text("direction = "),
              directionPrettyPrint.doc(direction),
              text(","),
              line,
              text("source = "),
              sourcePrettyPrint.doc(source),
            ),
          ),
          line,
          text(")"),
        )

      case SymbolTableEntry.UnwindEntry(source, identifier, from) =>
        concat(
          text("UnwindEntry("),
          nest(
            1,
            concat(
              line,
              text("id = "),
              text(identifier.toString),
              text(","),
              line,
              text("from = "),
              expressionPrettyPrint.doc(from),
              text(","),
              line,
              text("source = "),
              sourcePrettyPrint.doc(source),
            ),
          ),
          line,
          text(")"),
        )

      case SymbolTableEntry.ForeachEntry(source, identifier, from) =>
        concat(
          text("ForeachEntry("),
          nest(
            1,
            concat(
              line,
              text("id = "),
              text(identifier.toString),
              text(","),
              line,
              text("from = "),
              expressionPrettyPrint.doc(from),
              text(","),
              line,
              text("source = "),
              sourcePrettyPrint.doc(source),
            ),
          ),
          line,
          text(")"),
        )

      case SymbolTableEntry.ProcedureYieldEntry(source, identifier, procedureName, resultField) =>
        concat(
          text("ProcedureYieldEntry("),
          nest(
            1,
            concat(
              line,
              text("id = "),
              text(identifier.toString),
              text(","),
              line,
              text("procedureName = "),
              symbolPrettyPrint.doc(procedureName),
              text(","),
              line,
              text("resultField = "),
              symbolPrettyPrint.doc(resultField),
              text(","),
              line,
              text("source = "),
              sourcePrettyPrint.doc(source),
            ),
          ),
          line,
          text(")"),
        )

      case SymbolTableEntry.ExpressionEntry(source, identifier, exp) =>
        concat(
          text("ExpressionEntry("),
          nest(
            1,
            concat(
              line,
              text("id = "),
              text(identifier.toString),
              text(","),
              line,
              text("exp = "),
              expressionPrettyPrint.doc(exp),
              text(","),
              line,
              text("source = "),
              sourcePrettyPrint.doc(source),
            ),
          ),
          line,
          text(")"),
        )

      case SymbolTableEntry.QuineToCypherIdEntry(source, identifier, cypherIdentifier) =>
        concat(
          text("QuineToCypherIdEntry("),
          nest(
            1,
            concat(
              line,
              text("id = "),
              text(identifier.toString),
              text(","),
              line,
              text("cypherName = "),
              symbolPrettyPrint.doc(cypherIdentifier),
              text(","),
              line,
              text("source = "),
              sourcePrettyPrint.doc(source),
            ),
          ),
          line,
          text(")"),
        )
    }

  implicit val typeEntryPrettyPrint: PrettyPrint[TypeEntry] =
    PrettyPrint.instance { te =>
      concat(
        text("TypeEntry("),
        nest(
          1,
          concat(
            line,
            text("identifier = "),
            text(te.identifier),
            text(","),
            line,
            text("ty = "),
            typePrettyPrint.doc(te.ty),
            text(","),
            line,
            text("source = "),
            sourcePrettyPrint.doc(te.source),
          ),
        ),
        line,
        text(")"),
      )
    }

  implicit val symbolTablePrettyPrint: PrettyPrint[SymbolTable] =
    PrettyPrint.instance { st =>
      val refDocs = st.references.map(symbolTableEntryPrettyPrint.doc)
      val typeVarDocs = st.typeVars.map(typeEntryPrettyPrint.doc)
      concat(
        text("SymbolTable("),
        nest(
          1,
          concat(
            line,
            text("references = ["),
            nest(1, concat(line, intercalate(concat(text(","), line), refDocs))),
            line,
            text("],"),
            line,
            text("typeVars = ["),
            nest(1, concat(line, intercalate(concat(text(","), line), typeVarDocs))),
            line,
            text("]"),
          ),
        ),
        line,
        text(")"),
      )
    }

  implicit val symbolTableStatePrettyPrint: PrettyPrint[SymbolTableState] =
    PrettyPrint.instance { sts =>
      val errorDocs = sts.errors.toList.map(e => text(s""""$e""""))
      val warningDocs = sts.warnings.toList.map(w => text(s""""$w""""))
      val scopeDocs = sts.currentScope.toList.map { case (id, sym) =>
        concat(text(id.toString), text(" -> "), symbolPrettyPrint.doc(sym))
      }
      concat(
        text("SymbolTableState("),
        nest(
          1,
          concat(
            line,
            text("table = "),
            symbolTablePrettyPrint.doc(sts.table),
            text(","),
            line,
            text("errors = ["),
            intercalate(text(", "), errorDocs),
            text("],"),
            line,
            text("warnings = ["),
            intercalate(text(", "), warningDocs),
            text("],"),
            line,
            text("currentScope = {"),
            intercalate(text(", "), scopeDocs),
            text("},"),
            line,
            text("currentFreshId = "),
            text(sts.currentFreshId.toString),
          ),
        ),
        line,
        text(")"),
      )
    }

  implicit val symbolAnalysisStatePrettyPrint: PrettyPrint[SymbolAnalysisState] =
    PrettyPrint.instance { sas =>
      val diagDocs = sas.diagnostics.map(diagnosticPrettyPrint.doc)
      concat(
        text("SymbolAnalysisState("),
        nest(
          1,
          concat(
            line,
            text("diagnostics = ["),
            nest(1, concat(line, intercalate(concat(text(","), line), diagDocs))),
            line,
            text("],"),
            line,
            text("symbolTable = "),
            symbolTablePrettyPrint.doc(sas.symbolTable),
            text(","),
            line,
            text("cypherText = "),
            text(s""""${sas.cypherText}""""),
          ),
        ),
        line,
        text(")"),
      )
    }
}

object SymbolAnalysisInstances extends SymbolAnalysisInstances
