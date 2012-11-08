package com.axiomalaska.sos.source.stationupdater

import com.axiomalaska.sos.source.StationQuery
import com.axiomalaska.sos.source.BoundingBox
import com.axiomalaska.sos.source.data.SourceId
import com.axiomalaska.sos.tools.HttpSender
import com.axiomalaska.sos.source.data.DatabaseStation
import com.axiomalaska.sos.source.data.DatabaseSensor
import com.axiomalaska.sos.source.data.DatabasePhenomenon
import com.axiomalaska.sos.source.Units
import com.axiomalaska.phenomena.Phenomena
import com.axiomalaska.phenomena.Phenomenon
import com.axiomalaska.sos.data.Location
import com.axiomalaska.sos.source.GeoTools
import com.axiomalaska.sos.source.data.LocalPhenomenon
import com.axiomalaska.sos.source.data.ObservedProperty
import com.axiomalaska.sos.tools.HttpPart

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.log4j.Logger

import org.jsoup.nodes.Element
import org.jsoup.nodes.Document
import org.jsoup.Jsoup

import scala.collection.mutable
import scala.collection.JavaConversions._

class HadsStationUpdater(
  private val stationQuery: StationQuery,
  private val boundingBox: BoundingBox, 
  private val logger: Logger = Logger.getRootLogger()) extends StationUpdater  {

  private val stationUpdater = new StationUpdateTool(stationQuery, logger)
  private val source = stationQuery.getSource(SourceId.HADS)
  private val httpSender = new HttpSender()
  private val foreignIdParser = """.*nesdis_id=(.*)""".r
  private val parseDate = new SimpleDateFormat("yyyy")
  private val sensorParser = """\n\s([A-Z]{2}[A-Z0-9]{0,1})\(\w+\)""".r
  private val geoTools = new GeoTools()
  
  // ---------------------------------------------------------------------------
  // Public Members
  // ---------------------------------------------------------------------------

  def update(){
    val sourceStationSensors = getSourceStations()

    val databaseStations = stationQuery.getAllStations(source)

    stationUpdater.updateStations(sourceStationSensors, databaseStations)
  }
  
  val name = "HADS"
  
  // ---------------------------------------------------------------------------
  // Private Members
  // ---------------------------------------------------------------------------

  private def getSourceStations(): 
	  List[(DatabaseStation, List[(DatabaseSensor, List[DatabasePhenomenon])])] = {

    val stations = createSourceStations()
    val size = stations.length - 1
    logger.info("Total number of unfiltered stations: " + stations.length)
    val stationSensorsCollection = for {
      (station, index) <- stations.zipWithIndex
      if (withInBoundingBox(station))
      val sourceObservedProperties = getSourceObservedProperties(station)
      val databaseObservedProperties = 
        stationUpdater.updateObservedProperties(source, sourceObservedProperties)
      val sensors = stationUpdater.getSourceSensors(station, databaseObservedProperties)
      if (sensors.nonEmpty)
    } yield {
      logger.debug("[" + index + " of " + size + "] station: " + station.name)
      (station, sensors)
    }
    
    logger.info("finished with " + stationSensorsCollection.size + " stations")

    return stationSensorsCollection
  }

  private def withInBoundingBox(station: DatabaseStation): Boolean = {
    val stationLocation = new Location(station.latitude, station.longitude)
    return geoTools.isStationWithinRegion(stationLocation, boundingBox)
  }
  
  private def getSourceObservedProperties(station: DatabaseStation): List[ObservedProperty] =
    getSensorNames(station).flatMap(
      sensorName => getObservedProperty(sensorName))
  
  private def getSensorNames(station: DatabaseStation): List[String] = {
    val httpParts = List(
      new HttpPart("state", "nil"),
      new HttpPart("hsa", "nil"),
      new HttpPart("of", "3"),
      new HttpPart("nesdis_ids", station.foreign_tag),
      new HttpPart("sinceday", "-1"))

    val results = httpSender.sendPostMessage(
      "http://amazon.nws.noaa.gov/nexhads2/servlet/DecodedData", httpParts)

    if (results != null && results.contains(parseDate.format(new Date()))) {
      val sensorNames = for (sensorMatch <- sensorParser.findAllIn(results)) yield{
        val sensorParser(sensor) = sensorMatch
        sensor
      }
      return sensorNames.toList
    }
    else{
      return Nil
    }
  }

  private def createSourceStations(): List[DatabaseStation] = {
    val set = new mutable.HashSet[String]()
    for {
      stateUrl <- getAllStateUrls()
      element <- getStationElements(stateUrl)
      station <- createStation(element)
      if (!set.contains(station.foreign_tag))
    } yield {
      set += station.foreign_tag
      station
    }
  }

  private def createStation(element: Element): Option[DatabaseStation] = {
    val label = element.parent().parent().nextElementSibling().child(0).text
    val foreignIdParser(foreignId) = element.attr("HREF")

    logger.info("Collected Metadata for station: " + label)
    
    return getLatLon(foreignId) match {
      case Some((lat, lon)) => Some(new DatabaseStation(label, foreignId, 
          foreignId, "", "FIXED MET STATION", source.id, lat, lon))
      case _ => None
    }
  }
  
  private def getLatLon(foreignId:String): Option[(Double, Double)] = {
    val latLonResults = httpSender.sendGetMessage(
      "http://amazon.nws.noaa.gov/cgi-bin/hads/interactiveDisplays/displayMetaData.pl?table=dcp&nesdis_id=" + foreignId)

    if (latLonResults != null) {
      val doc = Jsoup.parse(latLonResults)

      val latOption = getLatitude(doc)
      val lonOption = getLongitude(doc)

      (latOption, lonOption) match {
        case (Some(lat), Some(lon)) => Some((lat, lon))
        case _ => None
      }
    } else {
      None
    }
  }
  
  private def getLongitude(doc: Document): Option[Double] = {
    return doc.getElementsMatchingOwnText("Longitude").headOption match {
      case Some(element) => {
        val rawLong = element.nextElementSibling().text

        val longParser = """(\w) (\d+).(\d+)'(\d+)[\"]?""".r

        val longParser(direction, degree, minute, second) = rawLong

        val long: Double = degree.toDouble + minute.toDouble / 60 + second.toDouble / 60 / 60

        if (direction.equalsIgnoreCase("W")) {
          Some(((-1) * long))
        } else {
          Some(long)
        }
      }
      case None => None
    }
  }

  private def getLatitude(doc: Document): Option[Double] = {
    return doc.getElementsMatchingOwnText("Latitude").headOption match {
      case Some(element) => {
        val rawLat = element.nextElementSibling().text

        val latParser = """(\w) (\d+).(\d+)'(\d+)[\"]?""".r

        val latParser(direction, degree, minute, second) = rawLat

        val lat: Double = degree.toDouble + minute.toDouble / 60 + second.toDouble / 60 / 60

        if (direction.equalsIgnoreCase("S")) {
          Some(((-1) * lat))
        } else {
          Some(lat)
        }
      }
      case None => None
    }
  }
  
  private def getAllStateUrls():List[String] ={
    val results = httpSender.sendGetMessage(
        "http://amazon.nws.noaa.gov/hads/goog_earth/")

    if (results != null) {
      val doc = Jsoup.parse(results)

      val areas = doc.getElementsByTag("area")

      areas.map(_.attr("href")).toList
    } else {
      Nil
    }
  }
  
  private def getStationElements(stateUrl:String):List[Element]={
    val results = httpSender.sendGetMessage(stateUrl)

    if (results != null) {
      Jsoup.parse(results).getElementsByTag("A").filter(
        element => element.text().length > 0).toList
    } else {
      Nil
    }
  }
  
  private def getObservedProperty(id: String): 
      Option[ObservedProperty] = {
    id match {
      case "MS0" => {
        return None
      }
      case "MS1" => {
        return None
      }
      case "MS2" => {
        return None
      }
      case "HG3" => {
        return None
      }
      case "DJ" => {
        return None
      }
      case "JF" => {
        return None
      }
      case "JD" => {
        return None
      }
      case "YP" => {
        return None
      }
      case "DY" => {
        return None
      }
      case "DH" => {
        return None
      }
      case "UR" => {
        getObservedProperty(Phenomena.instance.WIND_GUST_FROM_DIRECTION, id)
      }
      case "VJA" => {
        getObservedProperty(Phenomena.instance.AIR_TEMPERATURE_MAXIMUM, id)
      }
      case "VJB" => {
        getObservedProperty(Phenomena.instance.AIR_TEMPERATURE_MINIMUM, id)
      }
      case "DJA" => {
        return None
      }
      case "DJB" => {
        return None
      }
      case "LP0" => {
        return None
      }
      case "DUP" => {
        return None
      }
      case "UE" => {
        return None
      }
      case "PC" => {
        getObservedProperty(Phenomena.instance.PRECIPITATION_ACCUMULATED, id)
      }
      case "PC2" => {
        return None
//        return new Some[ObservedProperty](
//          stationUpdater.createObservedProperty(Sensors.PRECIPITATION, id,
//            source, "Precipitation (Accumulation), 2nd Sensor", Units.INCHES, 
//            SensorPhenomenonIds.PRECIPITATION_ACCUMULATION))
      }
      case "PP" => {
        getObservedProperty(Phenomena.instance.PRECIPITATION_INCREMENT, id)
      }
      case "US" => {
        getObservedProperty(Phenomena.instance.WIND_SPEED, id)
      }
      case "UD" => {
        getObservedProperty(Phenomena.instance.WIND_FROM_DIRECTION, id)
      }
      case "UP" => {
        getObservedProperty(Phenomena.instance.WIND_SPEED_OF_GUST, id)
      }
      case "UG" => {
        getObservedProperty(Phenomena.instance.WIND_SPEED_OF_GUST, id)
      }
      case "VUP" => {
        getObservedProperty(Phenomena.instance.WIND_SPEED_OF_GUST, id)
      }
      case "TA" => {
        getObservedProperty(Phenomena.instance.AIR_TEMPERATURE, id)
      }
      case "TA2" => {
        getObservedProperty(Phenomena.instance.AIR_TEMPERATURE, id)
      }
      case "TX" => {
        getObservedProperty(Phenomena.instance.AIR_TEMPERATURE_MAXIMUM, id)
      }
      case "TN" => {
        getObservedProperty(Phenomena.instance.AIR_TEMPERATURE_MINIMUM, id)
      }
      case "MT" => {
        getObservedProperty(Phenomena.instance.FUEL_TEMPERATURE, id)
      }
      case "XR" => {
        getObservedProperty(Phenomena.instance.RELATIVE_HUMIDITY, id)
      }
      case "VB" => {
        getObservedProperty(Phenomena.instance.BATTERY_VOLTAGE, id)
      }
      case "MM" => {
        getObservedProperty(Phenomena.instance.FUEL_MOISTURE, id)
      }
      case "RW" => {
        getObservedProperty(Phenomena.instance.SOLAR_RADIATION, id)
      }
      case "RS" => {
        getObservedProperty(Phenomena.instance.PHOTOSYNTHETICALLY_ACTIVE_RADIATION, id)
      }
      case "TW" => {
        getObservedProperty(Phenomena.instance.SEA_WATER_TEMPERATURE, id)
      }
      case "TW2" => {
        return None
//        return new Some[ObservedProperty](
//          stationUpdater.createObservedProperty(Sensors.WATER_TEMPERATURE, id,
//            source, "Water Temperature, 2nd Sensor", Units.FAHRENHEIT, 
//            SensorPhenomenonIds.SEA_WATER_TEMPERATURE))
      }
      case "WT" => {
        getObservedProperty(Phenomena.instance.TURBIDITY, id)
      }
      case "WC" => {
        getObservedProperty(Phenomena.instance.SEA_WATER_ELECTRICAL_CONDUCTIVITY, id)
      }
      case "WP" => {
        getObservedProperty(Phenomena.instance.SEA_WATER_PH_REPORTED_ON_TOTAL_SCALE, id)
      }
      case "WO" => {
        getObservedProperty(Phenomena.instance.DISSOLVED_OXYGEN, id)
      }
      case "WX" => {
        getObservedProperty(Phenomena.instance.DISSOLVED_OXYGEN_SATURATION, id)
      }
      case "TD" => {
        getObservedProperty(Phenomena.instance.DEW_POINT_TEMPERATURE, id)
      }
      case "HG" => {
        getObservedProperty(Phenomena.instance.createHomelessParameter("stream_gage_height", Units.FEET), id)
      }
      case "HG2" => {
        return None
//        return new Some[ObservedProperty](
//          stationUpdater.createObservedProperty(Sensors.STREAM_GAGE_HEIGHT, id,
//            source, "Stream Gage Height, 2nd Sensor", Units.FEET, 
//            SensorPhenomenonIds.STREAM_GAGE_HEIGHT))
      }
      case "HP" => {
        getObservedProperty(Phenomena.instance.WATER_SURFACE_HEIGHT_ABOVE_REFERENCE_DATUM, id)
      }
      case "WS" => {
        getObservedProperty(Phenomena.instance.SALINITY, id)
      }
      case "HM" => {
        getObservedProperty(Phenomena.instance.createHomelessParameter("water_level", Units.FEET), id)
      }
      case "PA" => {
        getObservedProperty(Phenomena.instance.AIR_PRESSURE, id)
      }
      case "SD" => {
        getObservedProperty(Phenomena.instance.SNOW_DEPTH, id)
      }
      case "SW" => {
        getObservedProperty(Phenomena.instance.SNOW_WATER_EQUIVALENT, id)
      }
      case "TS" => {
        getObservedProperty(Phenomena.instance.createHomelessParameter("ground_temperature_observed", Units.FAHRENHEIT), id)
      }
      case _ => {
        logger.debug("[" + source.name + "] observed property: " + id +
          " is not processed correctly.")
        return None
      }
    }
  }
  
  private def getObservedProperty(phenomenon: Phenomenon, foreignTag: String) : Option[ObservedProperty] = {
    try {
      var localPhenom: LocalPhenomenon = new LocalPhenomenon(new DatabasePhenomenon(phenomenon.getId))
      var units: String = if (phenomenon.getUnit == null || phenomenon.getUnit.getSymbol == null) "none" else phenomenon.getUnit.getSymbol
      if (localPhenom.databasePhenomenon.id < 0) {
        localPhenom = new LocalPhenomenon(insertPhenomenon(localPhenom.databasePhenomenon, units, phenomenon.getId, phenomenon.getName))
      }
      return new Some[ObservedProperty](stationUpdater.createObservedProperty(foreignTag, source, localPhenom.getUnit.getSymbol, localPhenom.databasePhenomenon.id))
    } catch {
      case ex: Exception => {}
    }
    None
  }
    
  private def insertPhenomenon(dbPhenom: DatabasePhenomenon, units: String, description: String, name: String) : DatabasePhenomenon = {
    dbPhenom.units = units
    dbPhenom.description = description
    dbPhenom.name = name
    stationQuery.createPhenomenon(dbPhenom)
  }
}