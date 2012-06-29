package com.axiomalaska.sos.source.stationupdater

import com.axiomalaska.sos.source.BoundingBox
import com.axiomalaska.sos.source.StationQuery

class AggregateStationUpdater(private val stationQuery: StationQuery,
  private val boundingBox: BoundingBox) extends StationUpdater {

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
    List(new HadsStationUpdater(stationQuery, boundingBox), 
        new NdbcStationUpdater(stationQuery, boundingBox),
        new NoaaNosCoOpsStationUpdater(stationQuery, boundingBox),
        new NoaaWeatherStationUpdater(stationQuery, boundingBox),
        new RawsStationUpdater(stationQuery, boundingBox),
        new SnoTelStationUpdater(stationQuery, boundingBox),
        new UsgsWaterStationUpdater(stationQuery, boundingBox))
  }
}