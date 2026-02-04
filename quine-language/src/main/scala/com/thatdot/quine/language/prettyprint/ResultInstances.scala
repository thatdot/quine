package com.thatdot.quine.language.prettyprint

import com.thatdot.quine.language.{AnalyzeResult, ParseResult, TypeCheckResult}

trait ResultInstances extends SymbolAnalysisInstances {
  import Doc._

  private def collection(open: String, close: String, docs: List[Doc]): Doc =
    if (docs.isEmpty) text(s"$open$close")
    else
      concat(
        text(open),
        nest(1, concat(line, intercalate(concat(text(","), line), docs))),
        line,
        text(close),
      )

  implicit val parseResultPrettyPrint: PrettyPrint[ParseResult] =
    PrettyPrint.instance { pr =>
      val astDoc = pr.ast match {
        case Some(q) => queryPrettyPrint.doc(q)
        case None => text("None")
      }
      val diagDocs = pr.diagnostics.map(diagnosticPrettyPrint.doc)
      concat(
        text("ParseResult("),
        nest(
          1,
          concat(
            line,
            text("ast = "),
            astDoc,
            text(","),
            line,
            text("diagnostics = "),
            collection("[", "]", diagDocs),
          ),
        ),
        line,
        text(")"),
      )
    }

  implicit val analyzeResultPrettyPrint: PrettyPrint[AnalyzeResult] =
    PrettyPrint.instance { ar =>
      val astDoc = ar.ast match {
        case Some(q) => queryPrettyPrint.doc(q)
        case None => text("None")
      }
      val diagDocs = ar.diagnostics.map(diagnosticPrettyPrint.doc)
      concat(
        text("AnalyzeResult("),
        nest(
          1,
          concat(
            line,
            text("ast = "),
            astDoc,
            text(","),
            line,
            text("symbolTable = "),
            symbolTablePrettyPrint.doc(ar.symbolTable),
            text(","),
            line,
            text("diagnostics = "),
            collection("[", "]", diagDocs),
          ),
        ),
        line,
        text(")"),
      )
    }

  implicit val typeCheckResultPrettyPrint: PrettyPrint[TypeCheckResult] =
    PrettyPrint.instance { tr =>
      val astDoc = tr.ast match {
        case Some(q) => queryPrettyPrint.doc(q)
        case None => text("None")
      }
      val typeEnvDocs = tr.typeEnv.toList.map { case (sym, ty) =>
        concat(symbolPrettyPrint.doc(sym), text(" -> "), typePrettyPrint.doc(ty))
      }
      val diagDocs = tr.diagnostics.map(diagnosticPrettyPrint.doc)
      concat(
        text("TypeCheckResult("),
        nest(
          1,
          concat(
            line,
            text("ast = "),
            astDoc,
            text(","),
            line,
            text("symbolTable = "),
            symbolTablePrettyPrint.doc(tr.symbolTable),
            text(","),
            line,
            text("typeEnv = "),
            collection("Map(", ")", typeEnvDocs),
            text(","),
            line,
            text("diagnostics = "),
            collection("[", "]", diagDocs),
          ),
        ),
        line,
        text(")"),
      )
    }
}

object ResultInstances extends ResultInstances
