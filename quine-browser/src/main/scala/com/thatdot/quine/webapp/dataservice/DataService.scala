package com.thatdot.quine.webapp.dataservice

/** Central read-side API surface: the composition of every data capability slice, so app
  * roots hold one service object while pages and narrow components declare just the
  * slice(s) they consume. Each slice exposes server state as shared signals (one fetch
  * per resource) and, where it accepts writes, seals its own command vocabulary behind a
  * slice-scoped dispatch observer — a component typed to one slice cannot send another
  * slice's commands.
  * This is implemented by [[OssDataService]] and [[EnterpriseDataService]]
  */
trait DataService
    extends NamespaceService
    with ClusterHealthService
    with BackpressureService
    with StandingQueryService
    with IngestStreamService
    with QueryUiConfigService
    with TapQueryService
    with WiretapService
