import { PortalStyle } from "./_componentStyles"

import { adminService } from "../_services"
import { useEffect, useState } from "react"

export const ConfigurationPortal = () => {
  const [quineConfig, setQuineConfig] = useState<any>({})

  const updateStandingQueries = () => {
    adminService.getQuineConfiguration().then((jsonResponse) => {
      setQuineConfig(jsonResponse)
      console.log(jsonResponse)
    })
  }

  useEffect(() => {
    updateStandingQueries()
  }, [])  
  
  return(
    <div style={ PortalStyle }>
      <h2>Configuration</h2>
      <pre style={{
        textAlign: 'start',
        overflow: 'auto',
        width: '90%',
        margin: 'auto',
        padding: '1%'
      }}>{JSON.stringify(quineConfig, null, 2)}</pre>
    </div>
  )
}
