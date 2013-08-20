package com.axiomalaska.sos.harvester.iso

import com.axiomalaska.sos.harvester.StationQuery;
import com.axiomalaska.sos.harvester.data.LocalStation

class NdbcSosIsoWriter(private val stationQuery:StationQuery,
                       private val templateFile: String,
                       private val isoDirectory: String,
                       private val overwrite: Boolean)
  extends SosIsoWriter(stationQuery, templateFile, isoDirectory, overwrite) {

  protected val sosName = "National Data Buoy Center SOS"
  protected val orgName = "National Data Buoy Center"
  override val role = "originator"
  protected val contactPhone = "(228) 688-2805"
  protected val contactWebId = "ndbc"
  protected val baseUrl = "http://sdftest.ndbc.noaa.gov/sos/server.php"

  override def getServiceInformation(station: LocalStation): List[ServiceIdentification] = {
    // base class method inserts station's platformType field as the serviceType, so usually "BUOY"
    // we override it here with a service specific value
    val orig = super.getServiceInformation(station).head
    List(new ServiceIdentification(orig.srvAbstract, "National Data Buoy Center SOS", orig.id, orig.citation, orig.extent, orig.ops))
  }
}
