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
    if (sources.contains("all")) {
      logger info "adding all sources"
      return List(new GlosStationUpdater(stationQuery, boundingBox, logger),
              new StoretStationUpdater(stationQuery, boundingBox, logger),
              new HadsStationUpdater(stationQuery, boundingBox, logger),
              new NdbcStationUpdater(stationQuery, boundingBox, logger),
              new NoaaNosCoOpsStationUpdater(stationQuery, boundingBox, logger),
              new NoaaWeatherStationUpdater(stationQuery, boundingBox, logger),
              new RawsStationUpdater(stationQuery, boundingBox, logger),
              new SnoTelStationUpdater(stationQuery, boundingBox, logger),
              new NerrsStationUpdater(stationQuery, boundingBox, logger),
              new UsgsWaterStationUpdater(stationQuery, boundingBox, logger))
    }
    if (sources.contains("glos")) {
      logger.info("adding GLOS updater")
      retval = new GlosStationUpdater(stationQuery, boundingBox, logger) :: retval
    }
    if (sources.contains("storet")) {
      logger.info("adding STORET updater")
      retval = new StoretStationUpdater(stationQuery, boundingBox, logger) :: retval
    }
    if (sources.contains("hads")) {
      logger.info("adding HADS updater")
      retval = new HadsStationUpdater(stationQuery, boundingBox, logger) :: retval
    }
    if (sources.contains("ndbc")) {
      logger.info("adding NDBC updater")
      retval = new NdbcSosStationUpdater(stationQuery, boundingBox, logger) :: retval
    }
    if (sources.contains("noaanoscoops")) {
      logger.info("adding NOAA-NOSCOOPS updater")
      retval = new NoaaNosCoOpsStationUpdater(stationQuery, boundingBox, logger) :: retval
    }
    if (sources.contains("noaaweather")) {
      logger.info("adding NOAA-WEATHER updater")
      retval = new NoaaWeatherStationUpdater(stationQuery, boundingBox, logger) :: retval
    }
    if (sources.contains("raws")) {
      logger.info("adding RAWS updater")
      retval = new RawsStationUpdater(stationQuery, boundingBox, logger) :: retval
    }
    if (sources.contains("snotel")) {
      logger.info("adding SNOTEL updater")
      retval = new SnoTelStationUpdater(stationQuery, boundingBox, logger) :: retval
    }
    if (sources.contains("nerrs")) {
      logger.info("adding NERRS updater")
      retval = new NerrsStationUpdater(stationQuery, boundingBox, logger) :: retval
    }
    if (sources.contains("usgswater")) {
      logger.info("adding USGS-WATER updater")
      retval = new UsgsWaterStationUpdater(stationQuery, boundingBox, logger) :: retval
    }
    return retval
  }
}