package com.thatdot.quine.webapp.components.landing

sealed abstract class LandingCommand
object LandingCommand {
  case object Refresh extends LandingCommand
  case object RefreshMetrics extends LandingCommand
  case object RefreshIngests extends LandingCommand
  case object RefreshStandingQueries extends LandingCommand
  case object RefreshClusterStatus extends LandingCommand
  case object RefreshConfig extends LandingCommand
}
