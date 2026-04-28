package com.thatdot.quine.webapp.components.streams

import scala.concurrent.Future

import com.raquo.laminar.api.L._
import io.circe.Json
import org.scalajs.macrotaskexecutor.MacrotaskExecutor.Implicits._

import com.thatdot.quine.openapi._

/** Multi-step form for creating a new ingest stream.
  *
  * Pure form: receives a schema to render, emits assembled JSON via `onSubmit`.
  * The parent handles the API call and reports the result.
  */
object CreateIngestForm {

  private val StepSource = 0
  private val StepConfig = 1

  def apply(
    spec: ParsedSpec,
    createSchema: Option[SchemaNode],
    onSubmit: Json => Future[Either[String, Json]],
    onComplete: () => Unit,
    onCancel: () => Unit,
  ): HtmlElement = {
    val currentStep = Var(StepSource)
    val formState = Var(Json.obj())
    val nameVar = Var("")
    val submit = new SubmitState

    div(
      // Form header with step tabs
      div(
        cls := "mb-3",
        ul(
          cls := "nav nav-tabs",
          renderTab("1. Source Type", StepSource, currentStep),
          renderTab("2. Configure", StepConfig, currentStep),
        ),
      ),
      // Step content
      child <-- currentStep.signal.map {
        case StepSource =>
          renderSourceStep(formState, createSchema, spec, currentStep)
        case StepConfig =>
          renderConfigStep(nameVar, formState, createSchema, spec)
        case _ => div()
      },
      ErrorAlert(submit.error.signal),
      // Navigation buttons
      div(
        cls := "d-flex justify-content-between mt-3",
        child <-- currentStep.signal.map {
          case StepSource =>
            button(
              cls := "btn btn-secondary",
              "Back",
              onClick --> { _ => onCancel() },
            )
          case StepConfig =>
            button(
              cls := "btn btn-secondary",
              "Back",
              onClick --> { _ =>
                // Clear source type selection and go back to Step 1
                createSchema.foreach { schema =>
                  findSourceUnion(schema, spec).foreach { union =>
                    val discPath = union.path :+ union.propertyName
                    formState.update(SchemaFormState.removeAt(_, discPath))
                  }
                }
                currentStep.set(StepSource)
              },
            )
          case _ => emptyNode
        },
        child <-- currentStep.signal.map {
          case StepConfig =>
            FormSubmit.submitButton(
              idleLabel = "Create Ingest Stream",
              busyLabel = "Creating...",
              state = submit,
              canSubmit = nameVar.signal.map(_.trim.nonEmpty),
            ) { () =>
              val body = formState.now().deepMerge(Json.obj("name" -> Json.fromString(nameVar.now())))
              submit.run(onSubmit(body))(_ => onComplete())
            }
          case _ => emptyNode
        },
      ),
    )
  }

  private def renderTab(label: String, step: Int, currentStep: Var[Int]): HtmlElement =
    li(
      cls := "nav-item",
      a(
        cls <-- currentStep.signal.map(s =>
          if (s == step) "nav-link active" else if (s > step) "nav-link" else "nav-link disabled",
        ),
        href := "#",
        label,
        onClick.preventDefault --> { _ =>
          if (currentStep.now() >= step) currentStep.set(step)
        },
      ),
    )

  /** The discriminated-union info located within a request schema.
    * `path` is where the union lives in the form state, e.g. `List("source")`
    * means the union is nested under that property — empty when the request
    * schema is itself the union.
    */
  private case class SourceUnion(
    propertyName: String,
    variants: List[(String, SchemaNode)],
    path: List[String],
  )

  /** Locate the discriminated source-type union in the request schema. The
    * request body is an object with a `source` property whose schema is the
    * union. We explicitly prefer a property named `source` so sibling unions
    * (e.g. `transformation`) don't get picked up by map-iteration order.
    */
  private def findSourceUnion(schema: SchemaNode, spec: ParsedSpec): Option[SourceUnion] = {
    val resolved = OpenApiParser.resolveNode(schema, spec.schemas)
    SchemaAnalysis.discriminatedUnion(resolved, spec) match {
      case Some(union) =>
        Some(SourceUnion(union.propertyName, union.variants, Nil))
      case None =>
        val props = resolved.properties.getOrElse(Map.empty)
        def unionAt(propName: String): Option[SourceUnion] =
          props.get(propName).flatMap { propSchema =>
            val resolvedProp = OpenApiParser.resolveNode(propSchema, spec.schemas)
            SchemaAnalysis.discriminatedUnion(resolvedProp, spec).map { union =>
              SourceUnion(union.propertyName, union.variants, List(propName))
            }
          }
        unionAt("source").orElse {
          props.keys.to(LazyList).flatMap(k => unionAt(k).toList).headOption
        }
    }
  }

  private def renderSourceStep(
    formState: Var[Json],
    requestSchema: Option[SchemaNode],
    spec: ParsedSpec,
    currentStep: Var[Int],
  ): HtmlElement =
    div(
      // Source type selector — driven by oneOf discriminator (at the root or under a property)
      requestSchema match {
        case Some(schema) =>
          findSourceUnion(schema, spec) match {
            case Some(union) =>
              renderSourceTypeSelector(union, formState, currentStep)
            case None =>
              div(cls := "text-body-secondary", "Select a source type (schema has no discriminator)")
          }
        case None =>
          div(cls := "alert alert-warning", "Could not find request schema for create ingest endpoint.")
      },
    )

  private def renderSourceTypeSelector(
    union: SourceUnion,
    formState: Var[Json],
    currentStep: Var[Int],
  ): HtmlElement =
    div(
      cls := "mb-3",
      h6(cls := "mb-1", "Choose a source type"),
      p(cls := "text-body-secondary small mb-3", "What system will feed data into your stream?"),
      div(
        cls := "row g-2",
        union.variants.map { case (discValue, schema) =>
          val name = schema.title.getOrElse[String](SchemaFormState.humanizeFieldName(discValue))
          val desc = schema.description.getOrElse("")
          val iconEl = IngestSourceIcons.forSourceType(discValue, name)
          div(
            cls := "col-md-6 col-lg-4",
            div(
              cls := "card h-100 border",
              styleAttr := "cursor: pointer",
              onClick --> { _ =>
                val resetUnion = Json.obj(union.propertyName -> Json.fromString(discValue))
                formState.update(SchemaFormState.setAt(_, union.path, resetUnion))
                currentStep.set(StepConfig)
              },
              div(
                cls := "card-body d-flex align-items-center p-2",
                div(
                  cls := "me-3",
                  styleAttr := "font-size: 1.5rem; width: 2rem; flex-shrink: 0; text-align: center",
                  iconEl,
                ),
                div(
                  cls := "flex-grow-1 overflow-hidden",
                  div(cls := "fw-semibold small text-truncate", name),
                  if (desc.nonEmpty)
                    small(
                      cls := "text-body-secondary",
                      styleAttr := "display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden",
                      desc,
                    )
                  else emptyNode,
                ),
              ),
            ),
          )
        },
      ),
    )

  /** A field to render, with enough context to place it in the correct form state path
    * and determine whether it belongs in the primary or advanced section.
    */
  private case class FieldEntry(
    name: String,
    schema: SchemaNode,
    basePath: List[String],
    isRequired: Boolean,
    hasDefault: Boolean,
  ) {
    def isPrimary: Boolean = isRequired && !hasDefault
  }

  /** Check whether a schema node (possibly a $ref) has a default value. */
  private def schemaHasDefault(node: SchemaNode, spec: ParsedSpec): Boolean = {
    val resolved = OpenApiParser.resolveNode(node, spec.schemas)
    resolved.default.isDefined
  }

  /** Collect all renderable fields from the source variant and top-level schema,
    * classified as primary (required, no default) or advanced (everything else).
    */
  private def collectFields(
    sourceVariant: SchemaNode,
    topLevelSchema: SchemaNode,
    unionPath: List[String],
    spec: ParsedSpec,
  ): List[FieldEntry] = {
    // Source variant fields (rendered under unionPath)
    val sourceProps = sourceVariant.properties.getOrElse(Map.empty)
    val sourceRequired = sourceVariant.required.getOrElse(Set.empty)
    val sourceFields = sourceProps.toList.map { case (name, schema) =>
      FieldEntry(name, schema, unionPath, sourceRequired.contains(name), schemaHasDefault(schema, spec))
    }

    // Top-level fields (rendered at root), skipping name and the source union property
    val resolved = OpenApiParser.resolveNode(topLevelSchema, spec.schemas)
    val skip = Set("name") ++ unionPath.headOption.toSet
    val topProps = resolved.properties.map(_.filterNot { case (k, _) => skip.contains(k) }).getOrElse(Map.empty)
    val topRequired = resolved.required.getOrElse(Set.empty)
    val topFields = topProps.toList.map { case (name, schema) =>
      FieldEntry(name, schema, Nil, topRequired.contains(name), schemaHasDefault(schema, spec))
    }

    sourceFields ++ topFields
  }

  /** Render a list of field entries as form fields in a row layout. */
  private def renderFields(
    fields: List[FieldEntry],
    spec: ParsedSpec,
    formState: Var[Json],
  ): HtmlElement =
    div(
      cls := "row g-2",
      fields.map { entry =>
        val colClass = if (SchemaFormRenderer.isSimpleField(entry.schema, spec)) "col-lg-6" else "col-12"
        div(
          cls := colClass,
          SchemaFormRenderer.render(
            entry.schema,
            spec,
            entry.basePath :+ entry.name,
            formState,
            isRequired = entry.isRequired,
          ),
        )
      },
    )

  private def renderConfigStep(
    nameVar: Var[String],
    formState: Var[Json],
    requestSchema: Option[SchemaNode],
    spec: ParsedSpec,
  ): HtmlElement = {
    val advancedExpanded = Var(false)

    div(
      // Name input — at the top of the configuration step
      div(
        cls := "mb-3",
        label(
          cls := "form-label",
          "Ingest Stream Name",
          span(cls := "text-danger ms-1", "*"),
        ),
        input(
          cls := "form-control",
          typ := "text",
          placeholder := "my-ingest-stream",
          controlled(
            value <-- nameVar.signal,
            onInput.mapToValue --> nameVar,
          ),
        ),
        div(cls := "form-text", "A unique name for this ingest stream."),
      ),
      // Source-specific + top-level fields, split into primary and advanced
      requestSchema match {
        case Some(schema) =>
          findSourceUnion(schema, spec) match {
            case Some(union) =>
              val discPath = union.path :+ union.propertyName
              val selectedType = SchemaFormState.getAt(formState.now(), discPath).flatMap(_.asString)

              selectedType match {
                case Some(typeName) =>
                  union.variants.toMap.get(typeName) match {
                    case Some(vs) =>
                      val filteredVariant = vs.copy(
                        properties = vs.properties.map(_.removed(union.propertyName)),
                      )
                      val allFields =
                        if (union.path.nonEmpty) collectFields(filteredVariant, schema, union.path, spec)
                        else collectFields(filteredVariant, SchemaNode(), union.path, spec)
                      val (primary, advanced) = allFields.partition(_.isPrimary)

                      div(
                        // Primary fields — required, no defaults
                        if (primary.nonEmpty) renderFields(primary, spec, formState) else div(),
                        // Advanced configuration — collapsible
                        if (advanced.nonEmpty)
                          div(
                            cls := "mt-4 pt-3 border-top",
                            div(
                              cls := "d-flex align-items-center cursor-pointer mb-3",
                              styleAttr := "cursor: pointer",
                              onClick --> { _ => advancedExpanded.update(!_) },
                              i(
                                cls <-- advancedExpanded.signal.map(e =>
                                  if (e) "cil-chevron-bottom me-2" else "cil-chevron-right me-2",
                                ),
                              ),
                              h6(cls := "mb-0", "Advanced Configuration"),
                            ),
                            div(
                              display <-- advancedExpanded.signal.map(if (_) "block" else "none"),
                              renderFields(advanced, spec, formState),
                            ),
                          )
                        else div(),
                      )
                    case None =>
                      div(cls := "text-body-secondary", s"No schema found for type '$typeName'")
                  }
                case None =>
                  div(cls := "alert alert-warning", "Please select a source type in the previous step.")
              }
            case None =>
              val resolved = OpenApiParser.resolveNode(schema, spec.schemas)
              SchemaFormRenderer.render(resolved, spec, Nil, formState)
          }
        case None =>
          div(cls := "alert alert-warning", "No request schema available.")
      },
    )
  }

}
