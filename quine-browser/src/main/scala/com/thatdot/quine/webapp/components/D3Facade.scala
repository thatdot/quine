package com.thatdot.quine.webapp.components

import scala.scalajs.js
import scala.scalajs.js.annotation._

import org.scalajs.dom

/** Minimal Scala.js facade for D3.js v7.
  *
  * D3's API is heavily chainable and dynamic, so this facade exposes
  * the top-level namespace and key generators. Most interaction happens
  * through `js.Dynamic` for D3 selections and transitions.
  *
  * @see [[https://d3js.org/]]
  */
@js.native
@JSImport("d3", JSImport.Namespace)
object D3 extends js.Object {

  /** Select a single element.
    * @see [[https://d3js.org/d3-selection/selecting#select]]
    */
  def select(selector: String): js.Dynamic = js.native
  def select(node: dom.Element): js.Dynamic = js.native

  /** Select all matching elements.
    * @see [[https://d3js.org/d3-selection/selecting#selectAll]]
    */
  def selectAll(selector: String): js.Dynamic = js.native

  /** Create a horizontal link generator (bezier curve for flow diagrams).
    * @see [[https://d3js.org/d3-shape/link#linkHorizontal]]
    */
  def linkHorizontal(): js.Dynamic = js.native

  /** Create a linear scale.
    * @see [[https://d3js.org/d3-scale/linear]]
    */
  def scaleLinear(): js.Dynamic = js.native

  /** Create an easing transition on a selection (called on selections, not directly).
    * @see [[https://d3js.org/d3-transition]]
    */
  def transition(): js.Dynamic = js.native

  /** Timer utility for periodic animations.
    * @see [[https://d3js.org/d3-timer#interval]]
    */
  def interval(callback: js.Function1[Double, Unit], delay: Double): js.Dynamic = js.native

  /** Create a namespace-aware element (for SVG).
    * @see [[https://d3js.org/d3-selection/creating#create]]
    */
  def create(name: String): js.Dynamic = js.native

  /** Tableau 10 categorical color scheme — 10 distinct hex color strings.
    * @see [[https://d3js.org/d3-scale-chromatic/categorical#schemeTableau10]]
    */
  def schemeTableau10: js.Array[String] = js.native

  /** ColorBrewer Dark2 palette — 8 distinct dark colors, pairs well with Tableau10
    * when you need a second palette that doesn't clash.
    * @see [[https://d3js.org/d3-scale-chromatic/categorical#schemeDark2]]
    */
  def schemeDark2: js.Array[String] = js.native

  /** Parse a CSS color string into a d3 color object with alpha & format methods.
    * @see [[https://d3js.org/d3-color#color]]
    */
  def color(specifier: String): js.Dynamic = js.native

  /** Convert a color to HSL for brightness/saturation manipulation.
    * @see [[https://d3js.org/d3-color#hsl]]
    */
  def hsl(color: js.Any): js.Dynamic = js.native

  /** Linear easing function (constant speed) — pass to `.ease(...)` on a transition.
    * @see [[https://d3js.org/d3-ease#easeLinear]]
    */
  def easeLinear: js.Function1[Double, Double] = js.native
}
