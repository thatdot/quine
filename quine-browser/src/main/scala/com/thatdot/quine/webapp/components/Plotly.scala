package com.thatdot.quine.webapp.components

import scala.scalajs.js

import com.raquo.laminar.api.L._
import org.scalajs.dom

/** Laminar component for rendering plotly.js charts.
  *
  * Uses the plain plotly.js API directly (not react-plotly.js). Supports both
  * static charts (via `apply`) and reactive charts (via `reactive`) that
  * efficiently update using `Plotly.react`.
  *
  * @see [[https://plotly.com/javascript/]]
  */
object Plotly {

  /** Parameters for rendering a Plotly chart. All fields mirror the plotly.js API.
    *
    * @param data plotly trace data
    * @param layout plotly layout configuration
    * @param config optional plotly config (responsive=true is always added)
    * @param style optional inline styles for the container
    * @param useResizeHandler if true, attach a ResizeObserver to resize on container changes
    * @param onClick handler for plotly_click events
    * @param onSunburstClick handler for plotly_sunburstclick events
    */
  final case class Props(
    data: js.Array[js.Object],
    layout: js.Object = js.Dynamic.literal(),
    config: js.UndefOr[js.Object] = js.undefined,
    style: js.Object = js.Dynamic.literal(),
    useResizeHandler: Boolean = false,
    onClick: js.UndefOr[js.Function1[js.Any, Unit]] = js.undefined,
    onSunburstClick: js.UndefOr[js.Function1[js.Any, Unit]] = js.undefined,
  )

  private val responsiveConfig: js.Object = js.Dynamic.literal(responsive = true)

  /** Merge user config with responsive defaults.
    * User config takes precedence.
    */
  private def mergeConfig(userConfig: js.UndefOr[js.Object]): js.Object =
    userConfig.fold(responsiveConfig) { cfg =>
      js.Object.assign(js.Dynamic.literal(), responsiveConfig, cfg)
    }

  /** Render a static plotly.js chart. Data/layout are set once on mount.
    *
    * @param data plotly trace data
    * @param layout plotly layout configuration
    * @param config optional plotly config (responsive=true is always added)
    * @param onClick handler for plotly_click events
    * @param onSunburstClick handler for plotly_sunburstclick events
    */
  def apply(
    data: js.Array[js.Object],
    layout: js.Object,
    config: js.UndefOr[js.Object] = js.undefined,
    onClick: Option[js.Function1[js.Any, Unit]] = None,
    onSunburstClick: Option[js.Function1[js.Any, Unit]] = None,
  ): HtmlElement = {
    val mergedConfig = mergeConfig(config)

    div(
      width := "100%",
      height := "100%",
      onMountCallback { ctx =>
        val el = ctx.thisNode.ref
        PlotlyJS.newPlot(el, data, layout, mergedConfig)
        val plotlyEl = el.asInstanceOf[PlotlyElement]
        onClick.foreach(h => plotlyEl.on("plotly_click", h))
        onSunburstClick.foreach(h => plotlyEl.on("plotly_sunburstclick", h))
      },
      onUnmountCallback { el =>
        PlotlyJS.purge(el.ref)
      },
    )
  }

  /** Render a reactive plotly.js chart that re-renders when the props signal emits.
    *
    * Uses `Plotly.react` for efficient differential updates instead of
    * destroying and recreating the chart.
    *
    * @param propsSignal signal of chart props; each emission triggers an update
    */
  def reactive(propsSignal: Signal[Props]): HtmlElement = {
    var elRef: Option[dom.HTMLElement] = None
    var resizeObserver: Option[dom.ResizeObserver] = None
    var currentUseResize = false

    div(
      onMountCallback { ctx =>
        elRef = Some(ctx.thisNode.ref)
      },
      propsSignal --> { props =>
        elRef.foreach { el =>
          applyStyle(el, props.style)
          val mergedConfig = mergeConfig(props.config)
          PlotlyJS.react(el, props.data, props.layout, mergedConfig)
          bindEvents(el, props)
          if (props.useResizeHandler && !currentUseResize) {
            resizeObserver = Some(attachResizeObserver(el))
            currentUseResize = true
          }
        }
      },
      onUnmountCallback { el =>
        resizeObserver.foreach(_.disconnect())
        resizeObserver = None
        elRef = None
        PlotlyJS.purge(el.ref)
      },
    )
  }

  private def applyStyle(el: dom.HTMLElement, style: js.Object): Unit = {
    val styleDict = style.asInstanceOf[js.Dictionary[String]]
    styleDict.foreach { case (key, value) =>
      el.style.setProperty(camelToKebab(key), value)
    }
  }

  private def bindEvents(el: dom.HTMLElement, props: Props): Unit = {
    val plotlyEl = el.asInstanceOf[js.Dynamic]
    // Remove previous listeners to avoid duplicates on re-render
    plotlyEl.removeAllListeners("plotly_click")
    plotlyEl.removeAllListeners("plotly_sunburstclick")
    props.onClick.foreach { handler =>
      plotlyEl.on("plotly_click", handler)
    }
    props.onSunburstClick.foreach { handler =>
      plotlyEl.on("plotly_sunburstclick", handler)
    }
  }

  private def attachResizeObserver(el: dom.HTMLElement): dom.ResizeObserver = {
    val obs = new dom.ResizeObserver((_, _) => PlotlyJS.Plots.resize(el))
    obs.observe(el)
    obs
  }

  private def camelToKebab(s: String): String =
    s.replaceAll("([A-Z])", "-$1").toLowerCase
}
