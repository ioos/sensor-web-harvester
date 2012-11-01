package com.axiomalaska.sos.source.stationupdater

import com.axiomalaska.sos.source.StationQuery
import com.axiomalaska.sos.source.BoundingBox
import com.axiomalaska.sos.source.data.SourceId
import com.axiomalaska.sos.tools.HttpSender
import com.axiomalaska.sos.source.data.DatabaseStation
import com.axiomalaska.sos.source.data.DatabaseSensor
import com.axiomalaska.sos.source.data.DatabasePhenomenon
import com.axiomalaska.sos.source.Units
import com.axiomalaska.sos.source.data.SensorPhenomenonIds
import com.axiomalaska.sos.data.Location
import com.axiomalaska.sos.source.GeoTools
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

        val longParser = """(\w) (\d+).(\d+)'(\d+)\"""".r

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

        val latParser = """(\w) (\d+).(\d+)'(\d+)\"""".r

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
        return new Some(
          stationUpdater.createObservedProperty(id,
            source, Units.DEGREES,
            SensorPhenomenonIds.WIND_GUST_DIRECTION))
      }
      case "VJA" => {
        return new Some(
          stationUpdater.createObservedProperty(id,
            source, Units.FAHRENHEIT, 
            SensorPhenomenonIds.AIR_TEMPERATURE_MAXIMUM))
      }
      case "VJB" => {
        return new Some(
          stationUpdater.createObservedProperty(id,
            source, Units.FAHRENHEIT,
            SensorPhenomenonIds.AIR_TEMPERATURE_MINIMUM))
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
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(id,
            source, Units.INCHES,
            SensorPhenomenonIds.PRECIPITATION_ACCUMULATION))
      }
      case "PC2" => {
        return None
//        return new Some[ObservedProperty](
//          stationUpdater.createObservedProperty(Sensors.PRECIPITATION, id,
//            source, "Precipitation (Accumulation), 2nd Sensor", Units.INCHES, 
//            SensorPhenomenonIds.PRECIPITATION_ACCUMULATION))
      }
      case "PP" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(id,
            source, Units.INCHES, 
            SensorPhenomenonIds.PRECIPITATION_INCREMENT))
      }
      case "US" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(id,
            source, Units.MILES_PER_HOUR, SensorPhenomenonIds.WIND_SPEED))
      }
      case "UD" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(id,
            source, Units.DEGREES, 
            SensorPhenomenonIds.WIND_DIRECTION))
      }
      case "UP" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(id,
            source, Units.MILES_PER_HOUR, SensorPhenomenonIds.WIND_GUST))
      }
      case "UG" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(id,
            source, Units.MILES_PER_HOUR, SensorPhenomenonIds.WIND_GUST))
      }
      case "VUP" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(id,
            source, Units.MILES_PER_HOUR, SensorPhenomenonIds.WIND_GUST))
      }
      case "TA" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(id,
            source, Units.FAHRENHEIT, SensorPhenomenonIds.AIR_TEMPERATURE))
      }
      case "TA2" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(id,
            source, Units.FAHRENHEIT, 
            SensorPhenomenonIds.AIR_TEMPERATURE))
      }
      case "TX" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(id,
            source, Units.FAHRENHEIT, 
            SensorPhenomenonIds.AIR_TEMPERATURE_MAXIMUM))
      }
      case "TN" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(id,
            source, Units.FAHRENHEIT, 
            SensorPhenomenonIds.AIR_TEMPERATURE_MINIMUM))
      }
      case "MT" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(id,
            source, Units.FAHRENHEIT, 
            SensorPhenomenonIds.FUEL_TEMPERATURE))
      }
      case "XR" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(id,
            source, Units.PERCENT, 
            SensorPhenomenonIds.RELATIVE_HUMIDITY))
      }
      case "VB" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(id,
            source, Units.VOLTAGE, SensorPhenomenonIds.BATTERY))
      }
      case "MM" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(id,
            source, Units.PERCENT, 
            SensorPhenomenonIds.FUEL_MOISTURE))
      }
      case "RW" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(id,
            source, Units.WATT_PER_METER_SQUARED, 
            SensorPhenomenonIds.SOLAR_RADIATION))
      }
      case "RS" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(id,
            source, Units.WATT_PER_METER_SQUARED, 
            SensorPhenomenonIds.PHOTOSYNTHETICALLY_ACTIVE_RADIATION))
      }
      case "TW" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(id,
            source, Units.FAHRENHEIT, 
            SensorPhenomenonIds.SEA_WATER_TEMPERATURE))
      }
      case "TW2" => {
        return None
//        return new Some[ObservedProperty](
//          stationUpdater.createObservedProperty(Sensors.WATER_TEMPERATURE, id,
//            source, "Water Temperature, 2nd Sensor", Units.FAHRENHEIT, 
//            SensorPhenomenonIds.SEA_WATER_TEMPERATURE))
      }
      case "WT" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(id,
            source, Units.NEPHELOMETRIC_TURBIDITY_UNITS, 
            SensorPhenomenonIds.WATER_TURBIDITY))
      }
      case "WC" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(id,
            source, Units.MICRO_MHOS_PER_CENTIMETERS, 
            SensorPhenomenonIds.CONDUCTIVITY))
      }
      case "WP" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(id,
            source, Units.STD_UNITS, 
            SensorPhenomenonIds.PH_WATER))
      }
      case "WO" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(id,
            source, Units.PARTS_PER_MILLION, 
            SensorPhenomenonIds.DISSOLVED_OXYGEN))
      }
      case "WX" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(id,
            source, Units.PERCENT, 
            SensorPhenomenonIds.DISSOLVED_OXYGEN_SATURATION))
      }
      case "TD" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(id,
            source, Units.FAHRENHEIT, 
            SensorPhenomenonIds.DEW_POINT))
      }
      case "HG" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(id,
            source, Units.FEET, 
            SensorPhenomenonIds.STREAM_GAGE_HEIGHT))
      }
      case "HG2" => {
        return None
//        return new Some[ObservedProperty](
//          stationUpdater.createObservedProperty(Sensors.STREAM_GAGE_HEIGHT, id,
//            source, "Stream Gage Height, 2nd Sensor", Units.FEET, 
//            SensorPhenomenonIds.STREAM_GAGE_HEIGHT))
      }
      case "HP" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(id,
            source, Units.FEET, 
            SensorPhenomenonIds.RESERVOIR_WATER_SURFACE_ABOVE_DATUM))
      }
      case "WS" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(id,
            source, Units.PARTS_PER_TRILLION, 
            SensorPhenomenonIds.SALINITY))
      }
      case "HM" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(id,
            source, Units.FEET, 
            SensorPhenomenonIds.WATER_LEVEL))
      }
      case "PA" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(id,
            source, Units.HECTOPASCAL, 
            SensorPhenomenonIds.BAROMETRIC_PRESSURE))
      }
      case "SD" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(id,
            source, Units.INCHES, 
            SensorPhenomenonIds.SNOW_DEPTH))
      }
      case "SW" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(id,
            source, Units.METERS, 
            SensorPhenomenonIds.SNOW_WATER_EQUIVALENT))
      }
      case "TS" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(id,
            source, Units.FAHRENHEIT, 
            SensorPhenomenonIds.GROUND_TEMPERATURE_OBSERVED))
      }
      case _ => {
        logger.debug("[" + source.name + "] observed property: " + id +
          " is not processed correctly.")
        return None
      }
    }
  }
}