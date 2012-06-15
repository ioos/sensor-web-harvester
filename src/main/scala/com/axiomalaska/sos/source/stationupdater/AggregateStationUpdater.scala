package com.axiomalaska.sos.source.stationupdater

import com.axiomalaska.sos.source.BoundingBox
import com.axiomalaska.sos.source.StationQuery

class AggregateStationUpdater(private val stationQuery: StationQuery,
  private val boundingBoxOption: Option[BoundingBox]) extends StationUpdater {

  // ---------------------------------------------------------------------------
  // Public Members
  // ---------------------------------------------------------------------------

  def update(){
    getStationUpdaters().par.foreach(_.update)
  }
  
  // ---------------------------------------------------------------------------
  // Private Members
  // ---------------------------------------------------------------------------

  private def getStationUpdaters():List[StationUpdater] = {
    List(new HadsStationUpdater(stationQuery, boundingBoxOption), 
        new NdbcStationUpdater(stationQuery, boundingBoxOption),
        new NoaaNosCoOpsStationUpdater(stationQuery, boundingBoxOption),
        new NoaaWeatherStationUpdater(stationQuery, boundingBoxOption),
        new RawsStationUpdater(stationQuery, boundingBoxOption),
        new SnoTelStationUpdater(stationQuery, boundingBoxOption),
        new UsgsWaterStationUpdater(stationQuery, boundingBoxOption))
  }
}