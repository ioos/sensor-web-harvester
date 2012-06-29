package com.axiomalaska.sos.source

import org.junit._
import Assert._
import com.axiomalaska.sos.source.stationupdater.RawsStationUpdater
import com.axiomalaska.sos.source.stationupdater.NdbcStationUpdater
import com.axiomalaska.sos.tools.HttpSender
import org.jsoup.Jsoup
import scala.collection.JavaConversions._
import com.axiomalaska.sos.data.Location
import com.axiomalaska.sos.source.stationupdater.HadsStationUpdater
import com.axiomalaska.sos.source.stationupdater.NoaaNosCoOpsStationUpdater
import com.axiomalaska.sos.source.stationupdater.NoaaWeatherStationUpdater
import com.axiomalaska.sos.source.stationupdater.SnoTelStationUpdater
import com.axiomalaska.sos.source.stationupdater.UsgsWaterStationUpdater
import javax.measure.Measure
import javax.measure.unit.NonSI
import javax.measure.unit.SI
import com.axiomalaska.sos.tools.HttpPart
import com.axiomalaska.sos.source.observationretriever.SnoTelObservationRetriever
import com.axiomalaska.sos.ObservationUpdater
import com.axiomalaska.sos.source.data.SourceId
import com.axiomalaska.sos.StationRetriever
import com.axiomalaska.sos.data.SosStation
import com.axiomalaska.sos.source.data.LocalStation
import com.axiomalaska.sos.source.observationretriever.NoaaWeatherObservationRetriever

@Test
class AppTest {

  /**
   * Eureka, CA to East of Yuma, AZ
   */
  def califorinaBoundingBox():BoundingBox ={
    BoundingBox(new Location(32.6666, -124.1245), 
        new Location(40.7641, -114.0830))
  }

  /**
   * All
   */
  def worldBoundingBox():BoundingBox ={
    BoundingBox(new Location(32.6666, -124.1245), 
        new Location(35.7641, -114.0830))
  }
  
  @Test
  def testOK() = assertTrue(true)
  
  private def findPhenomenonIdName(id:Long):String ={
    id match{
      case 54 => "BATTERY"
      case 55 => "BATTERY_MAXIMUM"
      case 56 => "BATTERY_MINIMUM"
      case 58 => "SOLAR_RADIATION"
      case 57 => "SOLAR_RADIATION_AVERAGE"
      case 59 => "SOLAR_RADIATION_MAXIMUM"
      case 60 => "SOLAR_RADIATION_MINIMUM"
      case 15 => "RELATIVE_HUMIDITY"
      case 40 => "RELATIVE_HUMIDITY_MAXIMUM"
      case 41 => "RELATIVE_HUMIDITY_MINIMUM"
      case 42 => "RELATIVE_HUMIDITY_AVERAGE"
      case 61 => "DISSOLVED_OXYGEN_SATURATION"
      case 34 => "DISSOLVED_OXYGEN"
      case 2 => "AIR_TEMPERATURE"
      case 35 => "AIR_TEMPERATURE_AVERAGE"
      case 36 => "AIR_TEMPERATURE_MAXIMUM"
      case 37 => "AIR_TEMPERATURE_MINIMUM"
      case 46 => "WIND_GUST_DIRECTION"
      case 22 => "WIND_GUST"
      case 23 => "WIND_SPEED"
      case 45 => "WIND_VERTICAL_VELOCITY"
      case 21 => "WIND_DIRECTION"
      case 24 => "WIND_WAVE_PERIOD"
      case 49 => "WIND_WAVE_HEIGHT"
      case 50 => "WIND_WAVE_DIRECTION"
      case 25 => "SWELL_PERIOD"
      case 48 => "SWELL_HEIGHT"
      case 51 => "SWELL_WAVE_DIRECTION"
      case 52 => "DOMINANT_WAVE_DIRECTION"
      case 53 => "DOMINANT_WAVE_PERIOD" 
      case 26 => "SIGNIFICANT_WAVE_HEIGHT" 
      case 47 => "AVERAGE_WAVE_PERIOD"
      case 7 => "CURRENT_DIRECTION"
      case 8 => "CURRENT_SPEED"
      case 63 => "AIR_CO2"
      case 62 => "SEA_WATER_CO2"
      case 33 => "PH_WATER"
      case 20 => "SEA_WATER_TEMPERATURE"
      case 9 => "DEW_POINT"
      case 44 => "WATER_TEMPERATURE_INTRAGRAVEL"
      case 64 => "SNOW_PILLOW"
      case 65 => "SNOW_DEPTH"
      case 66 => "SNOW_WATER_EQUIVALENT"
      case 38 => "PRECIPITATION_INCREMENT"
      case 39 => "PRECIPITATION_ACCUMULATION"
      case 67 => "REFLECTED_SHORTWAVE_RADIATION"
      case 68 => "INCOMING_SHORTWAVE_RADIATION"
      case 69 => "PHOTOSYNTHETICALLY_ACTIVE_RADIATION"
      case 70 => "WIND_GENERATOR_CURRENT"
      case 16 => "SALINITY"
      case 71 => "PANEL_TEMPERATURE"
      case 5 => "CONDUCTIVITY" 
      case 72 => "REAL_DIELECTRIC_CONSTANT" 
      case 1 => "BAROMETRIC_PRESSURE"
      case 73 => "FUEL_MOISTURE"
      case 17 => "WATER_TURBIDITY"
      case 74 => "FUEL_TEMPERATURE"
      case 75 => "STREAM_FLOW" 
      case 76 => "SOIL_MOISTURE_PERCENT" 
      case 77 => "GROUND_TEMPERATURE_OBSERVED" 
      case 78 => "DEPTH_TO_WATER_LEVEL"
      case 19 => "SEA_FLOOR_DEPTH_BELOW_SEA_SURFACE" 
      case 79 => "RESERVOIR_WATER_SURFACE_ABOVE_DATUM"
      case 80 => "STREAM_GAGE_HEIGHT" 
      case 32 => "WATER_LEVEL" 
      case 43 => "WATER_LEVEL_PREDICTIONS" 
    }
  }
  
//  @Test
//  def updateSos(){
//    val factory = new ObservationUpdaterFactory()
//    val queryBuilder = new StationQueryBuilder(
//        "jdbc:postgresql://localhost:5432/sensor", "sensoruser", "sensor")
//
//    queryBuilder.withStationQuery(stationQuery => {
//      val observationUpdater = factory.buildNoaaWeatherObservationUpdater(
//          "http://192.168.8.15:8080/sos/sos", stationQuery)
//          
//      observationUpdater.update()
//    })
//  }
//  
//  @Test
//  def test(){
//    val httpSender = new HttpSender()
//    val rawData =
//      httpSender.sendGetMessage("http://www.nws.noaa.gov/data/obhistory/KBAN.html")
//  }
//  
//  @Test
//  def updateSosOne(){
//    val factory = new ObservationUpdaterFactory()
//    val queryBuilder = new StationQueryBuilder(
//        "jdbc:postgresql://localhost:5432/sensor",
//        "sensoruser", "sensor")
//
//    queryBuilder.withStationQuery(stationQuery => {
//      val stationRetriever = new StationRetriever2(stationQuery)
//      val observationRetriever = new NoaaWeatherObservationRetriever(stationQuery)
//
//      val retrieverAdapter = new ObservationRetrieverAdapter(observationRetriever)
//      
//      val observationUpdater = new ObservationUpdater("http://192.168.8.15:8080/sos/sos",
//        stationRetriever, retrieverAdapter)
//
//      observationUpdater.update()
//    })
//  }
//  
//  @Test
//  def updateStationsInDatabase(){
//    val queryBuilder = new StationQueryBuilder(
//        "jdbc:postgresql://localhost:5432/sensor", "sensoruser", "sensor")
//
//    queryBuilder.withStationQuery(stationQuery => {
//      val stationUpdater = new RawsStationUpdater(stationQuery, califorinaBoundingBox)
//      stationUpdater.update()
//    })
//  }
  
  private class StationRetriever2(
    private val stationQuery: StationQuery) extends StationRetriever {

    override def getStations(): java.util.List[SosStation] = {
      val source = stationQuery.getSource(SourceId.NOAA_WEATHER)

      for {
        station <- stationQuery.getStations(source)
        if (station.tag == "KBAN")
      } yield {
        new LocalStation(source, station, stationQuery)
      }
    }
  }
}