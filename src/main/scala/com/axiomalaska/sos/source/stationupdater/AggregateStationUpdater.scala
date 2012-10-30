package com.axiomalaska.sos.source.stationupdater

import com.axiomalaska.sos.source.BoundingBox
import com.axiomalaska.sos.source.StationQuery
import org.apache.log4j.Logger
import scala.util.Random
import java.util.Calendar

class AggregateStationUpdater(private val stationQuery: StationQuery,
  private val boundingBox: BoundingBox, 
  private val sources: String,
  private val logger: Logger = Logger.getRootLogger()) extends StationUpdater {

  private val random = new Random(Calendar.getInstance.getTime.getTime)
  
  // ---------------------------------------------------------------------------
  // Public Members
  // ---------------------------------------------------------------------------

  def update(){
    val stationUpdaters = getStationUpdaters()
  
    for(stationUpdater <- random.shuffle(stationUpdaters)){
      logger.info("----- Starting updating source " + stationUpdater.name + " ------")
      stationUpdater.update()
    }
  }
  
  val name = "Aggregate"
  
  // ---------------------------------------------------------------------------
  // Private Members
  // ---------------------------------------------------------------------------

  private def getStationUpdaters():List[StationUpdater] = {
    var retval: List[StationUpdater] = List()
    if (sources.contains("all") || sources.contains("glos")) {
      retval = new GlosStationUpdater(stationQuery, boundingBox, logger) :: retval
    }
    if (sources.contains("all") || sources.contains("storet")) {
      retval = new StoretStationUpdater(stationQuery, boundingBox, logger) :: retval
    }
    if (sources.contains("all") || sources.contains("hads")) {
      retval = new HadsStationUpdater(stationQuery, boundingBox, logger) :: retval
    }
    if (sources.contains("all") || sources.contains("ndbc")) {
      retval = new NdbcStationUpdater(stationQuery, boundingBox, logger) :: retval
    }
    if (sources.contains("all") || sources.contains("noaanoscoops")) {
      retval = new NoaaNosCoOpsStationUpdater(stationQuery, boundingBox, logger) :: retval
    }
    if (sources.contains("all") || sources.contains("noaaweather")) {
      retval = new NoaaWeatherStationUpdater(stationQuery, boundingBox, logger) :: retval
    }
    if (sources.contains("all") || sources.contains("raws")) {
      retval = new RawsStationUpdater(stationQuery, boundingBox, logger) :: retval
    }
    if (sources.contains("all") || sources.contains("snotel")) {
      retval = new SnoTelStationUpdater(stationQuery, boundingBox, logger) :: retval
    }
    if (sources.contains("all") || sources.contains("nerrs")) {
      retval = new NerrsStationUpdater(stationQuery, boundingBox, logger) :: retval
    }
    if (sources.contains("all") || sources.contains("usgswater")) {
      retval = new UsgsWaterStationUpdater(stationQuery, boundingBox, logger) :: retval
    }
    return retval
  }
}