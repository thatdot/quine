package com.thatdot.quine.webapp.hooks

import org.scalajs.dom.window
import slinky.core.facade.Hooks._

object LocalStorageHook {
  def useLocalStorage(key: String, initialValue: String): (String, String => Unit) = {
    val (storedValue, setStoredValue) = useState(
      window.localStorage.getItem(key)
    )

    val setValue = (value: String) => {
      setStoredValue(value)
      window.localStorage.setItem(key, value)
    }

    (storedValue, setValue)
  }
}
