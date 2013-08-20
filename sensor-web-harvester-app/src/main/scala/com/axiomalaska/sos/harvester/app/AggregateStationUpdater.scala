package com.axiomalaska.sos.harvester.app
import org.apache.log4j.Logger
import scala.util.Random
import java.util.Calendar
import com.axiomalaska.sos.harvester.StationQuery
import com.axiomalaska.sos.harvester.BoundingBox
import com.axiomalaska.sos.harvester.source.stationupdater._

class AggregateStationUpdater(private val stationQuery: StationQuery,
  private val boundingBox: BoundingBox, 
  private val sources: String) extends StationUpdater {

  private val random = new Random(Calendar.getInstance.getTime.getTime)
  private val LOGGER = Logger.getLogger(getClass())
    
  // ---------------------------------------------------------------------------
  // Public Members
  // ---------------------------------------------------------------------------

  def update(){
    val stationUpdaters = getStationUpdaters()
  
    for(stationUpdater <- random.shuffle(stationUpdaters)){
      LOGGER.info("----- Starting updating source " + stationUpdater.name + " ------")
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
      LOGGER info "adding all sources"
      return List(new GlosStationUpdater(stationQuery, boundingBox),
              new StoretStationUpdater(stationQuery, boundingBox),
              new HadsStationUpdater(stationQuery, boundingBox),
              new NdbcSosStationUpdater(stationQuery, boundingBox),
              new NoaaNosCoOpsStationUpdater(stationQuery, boundingBox),
              new NoaaWeatherStationUpdater(stationQuery, boundingBox),
              new RawsStationUpdater(stationQuery, boundingBox),
              new SnoTelStationUpdater(stationQuery, boundingBox),
              new NerrsStationUpdater(stationQuery, boundingBox),
              new UsgsWaterStationUpdater(stationQuery, boundingBox))
    }
    if (sources.contains("glos")) {
      LOGGER.info("adding GLOS updater")
      retval ::= new GlosStationUpdater(stationQuery, boundingBox)
    }
    if (sources.contains("storet")) {
      LOGGER.info("adding STORET updater")
      retval ::= new StoretStationUpdater(stationQuery, boundingBox)
    }
    if (sources.contains("hads")) {
      LOGGER.info("adding HADS updater")
      retval ::= new HadsStationUpdater(stationQuery, boundingBox)
    }
    if (sources.contains("ndbc")) {
      LOGGER.info("adding NDBC updater")
      retval ::= new NdbcSosStationUpdater(stationQuery, boundingBox)
    }
    if (sources.contains("noaanoscoops")) {
      LOGGER.info("adding NOAA-NOSCOOPS updater")
      retval ::= new NoaaNosCoOpsStationUpdater(stationQuery, boundingBox)
    }
    if (sources.contains("noaaweather")) {
      LOGGER.info("adding NOAA-WEATHER updater")
      retval ::= new NoaaWeatherStationUpdater(stationQuery, boundingBox)
    }
    if (sources.contains("raws")) {
      LOGGER.info("adding RAWS updater")
      retval ::= new RawsStationUpdater(stationQuery, boundingBox)
    }
    if (sources.contains("snotel")) {
      LOGGER.info("adding SNOTEL updater")
      retval ::= new SnoTelStationUpdater(stationQuery, boundingBox)
    }
    if (sources.contains("nerrs")) {
      LOGGER.info("adding NERRS updater")
      retval ::= new NerrsStationUpdater(stationQuery, boundingBox)
    }
    if (sources.contains("usgswater")) {
      LOGGER.info("adding USGS-WATER updater")
      retval ::= new UsgsWaterStationUpdater(stationQuery, boundingBox)
    }
    return retval
  }
}