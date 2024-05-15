package com.thatdot.quine.app.v2api.definitions

sealed trait ApiCommand
case object AdminGetProperties extends ApiCommand
case object GetNamespaces extends ApiCommand
case object CreateNamespace extends ApiCommand
case object DeleteNamespace extends ApiCommand
