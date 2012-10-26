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
import com.axiomalaska.sos.source.data.LocalSource
import com.axiomalaska.sos.data.PublisherInfoImp
import com.axiomalaska.sos.source.stationupdater.NerrsStationUpdater
import webservices2.RequestsServiceLocator
import org.w3c.dom.ls.DOMImplementationLS
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Calendar
import java.util.TimeZone
import com.axiomalaska.sos.source.observationretriever.NerrsObservationRetriever
import org.apache.log4j.Logger
import org.apache.log4j.BasicConfigurator
import com.axiomalaska.sos.ObservationSubmitter
import com.axiomalaska.sos.data.PublisherInfo

@Test
class AppTest {

  /**
   * Eureka, CA to East of Yuma, AZ
   */
  def califorinaBoundingBox():BoundingBox ={
    BoundingBox(new Location(32.6666, -124.1245), 
        new Location(40.7641, -114.0830))
  }
  
  def randomBoundingBox():BoundingBox ={
    BoundingBox(new Location(39.0, -77.1), 
        new Location(39.1, -77.0))
  }

  /**
   * johnmarks
   */
  def johnmarksBoundingBox():BoundingBox ={
    BoundingBox(new Location(39.0, -80.0), 
        new Location(40.0, -74.0))
  }
  
  /**
   * All
   */
  def worldBoundingBox():BoundingBox ={
    BoundingBox(new Location(-85, -179.0), 
        new Location(85.0, 179.0))
  }
  
  @Test
  def testOK() = assertTrue(true)

//  @Test
//  def updateSos() {
//    val factory = new ObservationUpdaterFactory()
//    val queryBuilder = new StationQueryBuilder(
//      "jdbc:postgresql://localhost:5432/sensor", "sensoruser", "sensor")
//
//    val publisherInfo = new PublisherInfoImp()
//
//    publisherInfo.setCountry("")
//    publisherInfo.setEmail("")
//    publisherInfo.setName("")
//    publisherInfo.setWebAddress("")
//
//    queryBuilder.withStationQuery(stationQuery => {
//      val observationUpdater = factory.buildNerrsObservationUpdater(
//        "http://192.168.8.15:8080/sos/sos", stationQuery, publisherInfo)
//
//      observationUpdater.update()
//    })
//  }

//  @Test
//  def updateSosOneStation() {
//    val observationSubmitter = new ObservationSubmitter(
//      "http://staging1.axiom:8080/52n-sos-ioos-dev/sos");
//    val queryBuilder = new StationQueryBuilder(
//      "jdbc:postgresql://localhost:5432/sensor_metadata_database", "postgres", "postgres")
//
//    val publisherInfo = new PublisherInfoImp()
//
//    publisherInfo.setCountry("")
//    publisherInfo.setEmail("")
//    publisherInfo.setName("")
//    publisherInfo.setWebAddress("")
//    
//    queryBuilder.withStationQuery(stationQuery => {
//      val observationRetriever = new NerrsObservationRetriever(stationQuery)
//      val retrieverAdapter = new ObservationRetrieverAdapter(observationRetriever)
//      val databaseSource = stationQuery.getSource(SourceId.NERRS)
//      val sosSource = new LocalSource(databaseSource)
//      val station = stationQuery.getStation(101829)
//      val sosStation = new LocalStation(sosSource, station, stationQuery)
//      observationSubmitter.update(sosStation,
//        retrieverAdapter, publisherInfo);
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
//        "jdbc:postgresql://localhost:5432/sensor_metadata_database",
//        "postgres", "postgres")
//
//    queryBuilder.withStationQuery(stationQuery => {
//      val stationRetriever = new StationRetriever2(stationQuery)
//      val observationRetriever = new NoaaWeatherObservationRetriever(stationQuery)
//
//      val retrieverAdapter = new ObservationRetrieverAdapter(observationRetriever)
//      val publisherInfo = new PublisherInfoImp()
//      val observationUpdater = new ObservationUpdater(
//          "http://staging1.axiom:8080/52n-sos-ioos-dev/sos",
//        stationRetriever, publisherInfo, retrieverAdapter)
//
//      observationUpdater.update()
//    })
//  }
  
//  @Test
//  def testRetriever(){
//    val queryBuilder = new StationQueryBuilder(
//        "jdbc:postgresql://localhost:5432/sensor", "sensoruser", "sensor")
//
//    
//    queryBuilder.withStationQuery(stationQuery => {
//      val source = new LocalSource(stationQuery.getSource(SourceId.NERRS))
//      val databaseStation = stationQuery.getStation()
//      val station = new LocalStation(source, stationQuery)
//      val retriever = new NerrsObservationRetriever(stationQuery)
//      val observationValuesCollections = 
//        retriever.getObservationValues(station, sensor, phenomenon, startDate)
//    })
//  }
  
//  @Test
//  def updateStationsInDatabase(){
//    val queryBuilder = new StationQueryBuilder(
//        "jdbc:postgresql://localhost:5432/sensor", "sensoruser", "sensor")
//    
//    queryBuilder.withStationQuery(stationQuery => {
//      val stationUpdater = new HadsStationUpdater(stationQuery, johnmarksBoundingBox)
//      stationUpdater.update()
//    })
//  }

//  @Test
//  def pullStationsNerrs(){
//    val locator = new RequestsServiceLocator();
//
//    val requests = locator.getRequestsCfc();
//
//    val doc = requests.exportStationCodesXMLNew();
//
//    val domImplLS = doc.getImplementation().asInstanceOf[DOMImplementationLS]
//
//    val serializer = domImplLS.createLSSerializer();
//    val str = serializer.writeToString(doc);
//    
//    for (row <- (scala.xml.XML.loadString(str) \\ "data")){
//    	val paramsReported = (row \\ "Params_Reported").text.split(",").toList
//    	if(paramsReported.contains("TIDE")){
//    	  println((row \\ "Station_Code").text)
//    	}
//    }
////    println(str)
//  }
  
//  @Test
//  def formatDate(){
//    val dateParser:SimpleDateFormat = new SimpleDateFormat("MM/dd/yyyy")
//    
//    val cal:Calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT"))
//
//    cal.set(2012, Calendar.AUGUST, 1)
//    val date = cal.getTime
//    println(dateParser.format(date))
//    
//  }
//  
//  @Test
//  def pullValuesDateNerrs(){
//    val locator = new RequestsServiceLocator()
//
//    val requests = locator.getRequestsCfc()
//    
//    val stationCode = "sosbhnut"
//    val minDate = "08/01/2012"
//    val maxDate = "08/12/2012"
//    val param = "TIDE"
//      
//    val doc = requests.exportAllParamsDateRangeXMLNew(stationCode, minDate, maxDate, param)
//    
//    val domImplLS = doc.getImplementation().asInstanceOf[DOMImplementationLS]
//
//    val serializer = domImplLS.createLSSerializer()
//    val str = serializer.writeToString(doc)
//    
//    println(str)
//  }
//  
//  @Test
//  def pullValuesNerrs(){
//    val locator = new RequestsServiceLocator()
//
//    val requests = locator.getRequestsCfc()
//    
//    val stationCode = "lksblwq"
//    val recs = "4"
//    val param = "Temp"
//      
//    val doc = requests.exportSingleParamXMLNew(stationCode, recs, param)
//    
//    val domImplLS = doc.getImplementation().asInstanceOf[DOMImplementationLS]
//
//    val serializer = domImplLS.createLSSerializer()
//    val str = serializer.writeToString(doc)
//    
//    println(str)
//  }
  
  private def getString(cal:Calendar):String ={
    cal.get(Calendar.YEAR) + "-" + cal.get(Calendar.MONTH) + "-" + 
    cal.get(Calendar.DAY_OF_MONTH) + " " + cal.get(Calendar.HOUR_OF_DAY) + ":" +
    cal.get(Calendar.MINUTE)
  }
  
  private class StationRetriever2(
    private val stationQuery: StationQuery) extends StationRetriever {

    override def getStations(): java.util.List[SosStation] = {
      val source = stationQuery.getSource(SourceId.NOAA_WEATHER)
      val sosSource = new LocalSource(source)
      for {
        station <- stationQuery.getAllStations(source)
//        if (station.tag == "KBAN")
      } yield {
        new LocalStation(sosSource, station, stationQuery)
      }
    }
  }
  
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
}