package com.axiomalaska.sos.harvester.iso

import com.axiomalaska.sos.harvester.StationQuery;

class NoaaNosCoOpsIsoWriter(private val stationQuery:StationQuery,
                            private val templateFile: String,
                            private val isoDirectory: String,
                            private val overwrite: Boolean)
  extends SosIsoWriter(stationQuery, templateFile, isoDirectory, overwrite) {

  protected val sosName = "NOAA NOS CO-OPS SOS"
  protected val orgName = "NOAA NOS CO-OPS"
  override val role = "originator"
  protected val contactPhone = "(301) 713-2815"
  protected val contactWebId = "coops"
  protected val baseUrl = "http://opendap.co-ops.nos.noaa.gov/ioos-dif-sos/SOS"
}
