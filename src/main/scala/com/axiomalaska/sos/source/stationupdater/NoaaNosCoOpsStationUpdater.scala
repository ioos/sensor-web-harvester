package com.axiomalaska.sos.source.stationupdater

import org.apache.log4j.Logger
import com.axiomalaska.sos.source.BoundingBox
import com.axiomalaska.sos.source.StationQuery
import com.axiomalaska.sos.source.data.SourceId
import com.axiomalaska.sos.source.BoundingBox
import com.axiomalaska.sos.source.StationQuery
import com.axiomalaska.sos.source.data.SourceId
import com.axiomalaska.sos.source.SourceUrls

class NoaaNosCoOpsStationUpdater(private val stationQuery: StationQuery,
  private val boundingBox: BoundingBox) extends 
  SosStationUpdater(stationQuery, boundingBox) {

  // ---------------------------------------------------------------------------
  // SosStationUpdater Members
  // ---------------------------------------------------------------------------
  
  protected val serviceUrl = SourceUrls.NOAA_NOS_CO_OPS_SOS
  protected val source = stationQuery.getSource(SourceId.NOAA_NOS_CO_OPS)

  // ---------------------------------------------------------------------------
  // StationUpdater Members
  // ---------------------------------------------------------------------------
  
  val name = "NOAA NOS CO-OPS"
}