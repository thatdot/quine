package com.thatdot.quine.app.v2api

/** In API V2, neither API objects nor internal objects depend on each other
  * (in principle, they could be extracted into independent modules), but
  * since conversion from each to the other is necessary, this package provides
  * a unique scope that depends on both, in order to perform such translation.
  * {{{
  * ┌──────────────────────┐
  * │app.v2api ┌──────────┐│
  * │          │converters││
  * │          └┬──────┬──┘│
  * │┌──────────▼┐     │   │
  * ││definitions│     │   │
  * │└───────────┘     │   │
  * └──────────────────┼───┘
  * ┌──────────────────▼───┐
  * │app.model             │
  * └──────────────────────┘
  * }}}
  */
package object converters {}
