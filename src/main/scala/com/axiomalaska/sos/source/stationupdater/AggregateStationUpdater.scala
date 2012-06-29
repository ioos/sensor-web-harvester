package com.axiomalaska.sos.source.stationupdater

import com.axiomalaska.sos.source.BoundingBox
import com.axiomalaska.sos.source.StationQuery
import org.apache.log4j.Logger

class AggregateStationUpdater(private val stationQuery: StationQuery,
  private val boundingBox: BoundingBox, 
  private val logger: Logger = Logger.getRootLogger()) extends StationUpdater {

  // ---------------------------------------------------------------------------
  // Public Members
  // ---------------------------------------------------------------------------

  def update(){
    for(stationUpdater <- getStationUpdaters()){
      logger.info("----- Starting updating source " + stationUpdater.name + " ------")
      stationUpdater.update()
    }
  }
  
  val name = "Aggregate"
  
  // ---------------------------------------------------------------------------
  // Private Members
  // ---------------------------------------------------------------------------

  private def getStationUpdaters():List[StationUpdater] = {
    List(new HadsStationUpdater(stationQuery, boundingBox, logger), 
        new NdbcStationUpdater(stationQuery, boundingBox, logger),
        new NoaaNosCoOpsStationUpdater(stationQuery, boundingBox, logger),
        new NoaaWeatherStationUpdater(stationQuery, boundingBox, logger),
        new RawsStationUpdater(stationQuery, boundingBox, logger),
        new SnoTelStationUpdater(stationQuery, boundingBox, logger),
        new UsgsWaterStationUpdater(stationQuery, boundingBox, logger))
  }
}