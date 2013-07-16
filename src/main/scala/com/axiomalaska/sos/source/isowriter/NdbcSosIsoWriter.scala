package com.axiomalaska.sos.source.isowriter

import com.axiomalaska.sos.source.StationQuery

class NdbcSosIsoWriter(private val stationQuery:StationQuery,
                       private val templateFile: String,
                       private val isoDirectory: String,
                       private val overwrite: Boolean)
  extends SosIsoWriter(stationQuery, templateFile, isoDirectory, overwrite) {

  protected val sosName = "National Data Buoy Center SOS"
  protected val orgName = "National Data Buoy Center"
  override val role = "distributor"
  protected val contactPhone = "(228) 688-2805"
  protected val contactWebId = "ndbc"
  protected val baseUrl = "http://sdftest.ndbc.noaa.gov/sos/server.php"
}
