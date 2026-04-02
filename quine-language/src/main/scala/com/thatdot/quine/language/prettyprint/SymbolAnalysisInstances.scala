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

  implicit lazy val symbolTableEntryPrettyPrint: PrettyPrint[BindingEntry] =
    PrettyPrint.instance { case BindingEntry(source, identifier, originalName) =>
      val nameDoc = originalName match {
        case Some(name) => concat(text(", name = "), symbolPrettyPrint.doc(name))
        case None => empty
      }
      concat(
        text("BindingEntry("),
        nest(
          1,
          concat(
            line,
            text("id = "),
            text(identifier.toString),
            nameDoc,
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
            text(te.identifier.id.toString),
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
            text(","),
            line,
            text("freshId = "),
            text(sas.freshId.toString),
          ),
        ),
        line,
        text(")"),
      )
    }
}

object SymbolAnalysisInstances extends SymbolAnalysisInstances
