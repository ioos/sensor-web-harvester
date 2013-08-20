package com.axiomalaska.sos.harvester.source.stationupdater

import org.apache.log4j.Logger

import com.axiomalaska.sos.harvester.BoundingBox;
import com.axiomalaska.sos.harvester.source.SourceUrls;
import com.axiomalaska.sos.harvester.StationQuery;
import com.axiomalaska.sos.harvester.data.SourceId;

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