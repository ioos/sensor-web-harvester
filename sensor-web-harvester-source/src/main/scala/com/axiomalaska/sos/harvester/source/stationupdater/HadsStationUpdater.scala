package com.axiomalaska.sos.harvester.source.stationupdater

import com.axiomalaska.sos.harvester.BoundingBox;
import com.axiomalaska.sos.harvester.GeoTools;
import com.axiomalaska.sos.harvester.source.SourceUrls;
import com.axiomalaska.sos.harvester.StationQuery;
import com.axiomalaska.sos.harvester.Units;
import com.axiomalaska.sos.harvester.StateAbbrevations.stateAbbreviations;
import com.axiomalaska.sos.harvester.data.DatabasePhenomenon;
import com.axiomalaska.sos.harvester.data.DatabaseSensor;
import com.axiomalaska.sos.harvester.data.DatabaseStation;
import com.axiomalaska.sos.harvester.data.ObservedProperty;
import com.axiomalaska.sos.harvester.data.SourceId;
import com.axiomalaska.sos.tools.HttpSender
import com.axiomalaska.phenomena.Phenomena
import com.axiomalaska.phenomena.Phenomenon
import com.axiomalaska.sos.harvester.data.LocalPhenomenon
import com.axiomalaska.sos.tools.HttpPart
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.log4j.Logger
import org.jsoup.nodes.Element
import org.jsoup.nodes.Document
import org.jsoup.Jsoup
import scala.collection.mutable
import scala.collection.JavaConversions._
import com.axiomalaska.ioos.sos.GeomHelper
import com.axiomalaska.sos.harvester.data.PhenomenaFactory
import scala.collection.mutable.{Set => MSet}
import com.axiomalaska.ioos.sos.GeomHelper

class HadsStationUpdater(
  private val stationQuery: StationQuery,
  private val boundingBox: BoundingBox) extends StationUpdater {

  private val LOGGER = Logger.getLogger(getClass())
  private val stationUpdater = new StationUpdateTool(stationQuery)
  private val source = stationQuery.getSource(SourceId.HADS)
  private val httpSender = new HttpSender()
  private val foreignIdParser = """.*nesdis_id=(.*)""".r
  private val parseDate = new SimpleDateFormat("yyyy")
  private val sensorParser = """\n\s([A-Z]{2}[A-Z0-9]{0,1})\(\w+\)""".r
  private val geoTools = new GeoTools()
  private val phenomenaFactory = new PhenomenaFactory()
  private val nonStates = Set("PR","CN")
  // ---------------------------------------------------------------------------
  // Public Members
  // ---------------------------------------------------------------------------

  def update() {
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
    LOGGER.info("Total number of unfiltered stations: " + stations.length)
    val stationSensorsCollection = for {
      (station, index) <- stations.zipWithIndex
      val stationLocation = GeomHelper.createLatLngPoint(station.latitude, station.longitude)
      if (geoTools.isStationWithinRegion(stationLocation, boundingBox))
      val sourceObservedProperties = getSourceObservedProperties(station)
      val databaseObservedProperties = stationUpdater.updateObservedProperties(source, sourceObservedProperties)
      val sensors = stationUpdater.getSourceSensors(station, databaseObservedProperties)
      if (sensors.nonEmpty)
    } yield {
      LOGGER.debug("[" + index + " of " + size + "] station: " + station.name)
      (station, sensors)
    }

    LOGGER.info("finished with " + stationSensorsCollection.size + " stations")

    return stationSensorsCollection
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

    val results = httpSender.sendPostMessage(SourceUrls.HADS_OBSERVATION_RETRIEVAL, httpParts)

    if (results != null && results.contains(parseDate.format(new Date()))) {
      val sensorNames = for (sensorMatch <- sensorParser.findAllIn(results)) yield {
        val sensorParser(sensor) = sensorMatch
        sensor
      }
      return sensorNames.toList
    } else {
      return Nil
    }
  }

  private def createSourceStations(): List[DatabaseStation] = {
    val set = new mutable.HashSet[String]()
    for {
      stateUrl <- getStateUrls()
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

    LOGGER.info("Collected Metadata for station: " + label)

    return getLatLon(foreignId) match {
      case Some((lat, lon)) => Some(new DatabaseStation(label,
        source.tag + ":" + foreignId, foreignId, "",
        "FIXED MET STATION", source.id, lat, lon, null, null))
      case _ => None
    }
  }

  private def getLatLon(foreignId: String): Option[(Double, Double)] = {
    val latLonResults = HttpSender.sendGetMessage(SourceUrls.HADS_STATION_INFORMATION + foreignId)

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

  /**
   * This method gets all state URLs. Use getStateUrls() to get all states in the bounding box, plus PR
   */
  @Deprecated
  private def getAllStateUrls(): List[String] = {
    val results = HttpSender.sendGetMessage(SourceUrls.HADS_COLLECTION_STATE_URLS)

    if (results != null) {
      val doc = Jsoup.parse(results)

      val areas = doc.getElementsByTag("area")

      areas.map(_.attr("href")).toList
    } else {
      Nil
    }
  }

  private def getStateUrls(): List[String] = {
    val abbrs = for{ state <- MSet(GeoTools.statesInBoundingBox(boundingBox).toList:_*)} yield stateAbbreviations(state).toUpperCase
    //TODO disabling non-state harvest for now, add more robust spatial layer in the future    
//    abbrs ++= nonStates
    abbrs.map( abbr => SourceUrls.HADS_STATE_URL_TEMPLATE.format(abbr)).toList
  }
  
  private def getStationElements(stateUrl: String): List[Element] = {
    val results = HttpSender.sendGetMessage(stateUrl)

    if (results != null) {
      Jsoup.parse(results).getElementsByTag("A").filter(
        element => element.text().length > 0).toList
    } else {
      Nil
    }
  }

  private def getObservedProperty(id: String): Option[ObservedProperty] = {
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
        Some(stationUpdater.getObservedProperty(
          Phenomena.instance.WIND_GUST_FROM_DIRECTION, id, Units.DEGREES, source))
      }
      case "VJA" => {
        Some(stationUpdater.getObservedProperty(
          Phenomena.instance.AIR_TEMPERATURE_MAXIMUM, id, Units.FAHRENHEIT, source))
      }
      case "VJB" => {
        Some(stationUpdater.getObservedProperty(
          Phenomena.instance.AIR_TEMPERATURE_MINIMUM, id, Units.FAHRENHEIT, source))
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
        Some(stationUpdater.getObservedProperty(
          Phenomena.instance.PRECIPITATION_ACCUMULATED, id, Units.INCHES, source))
      }
      case "PC2" => {
        return None
        //        return new Some[ObservedProperty](
        //          stationUpdater.createObservedProperty(Sensors.PRECIPITATION, id,
        //            source, "Precipitation (Accumulation), 2nd Sensor", Units.INCHES, 
        //            SensorPhenomenonIds.PRECIPITATION_ACCUMULATION))
      }
      case "PP" => {
        Some(stationUpdater.getObservedProperty(
          Phenomena.instance.PRECIPITATION_INCREMENT, id, Units.INCHES, source))
      }
      case "US" => {
        Some(stationUpdater.getObservedProperty(
          Phenomena.instance.WIND_SPEED, id, Units.MILES_PER_HOUR, source))
      }
      case "UD" => {
        Some(stationUpdater.getObservedProperty(
          Phenomena.instance.WIND_FROM_DIRECTION, id, Units.DEGREES, source))
      }
      case "UP" => {
        Some(stationUpdater.getObservedProperty(
          Phenomena.instance.WIND_SPEED_OF_GUST, id, Units.MILES_PER_HOUR, source))
      }
      case "UG" => {
        Some(stationUpdater.getObservedProperty(
          Phenomena.instance.WIND_SPEED_OF_GUST, id, Units.MILES_PER_HOUR, source))
      }
      case "VUP" => {
        Some(stationUpdater.getObservedProperty(
          Phenomena.instance.WIND_SPEED_OF_GUST, id, Units.MILES_PER_HOUR, source))
      }
      case "TA" => {
        Some(stationUpdater.getObservedProperty(
          Phenomena.instance.AIR_TEMPERATURE, id, Units.FAHRENHEIT, source))
      }
      case "TA2" => {
        Some(stationUpdater.getObservedProperty(
          Phenomena.instance.AIR_TEMPERATURE, id, Units.FAHRENHEIT, source))
      }
      case "TX" => {
        Some(stationUpdater.getObservedProperty(
          Phenomena.instance.AIR_TEMPERATURE_MAXIMUM, id, Units.FAHRENHEIT, source))
      }
      case "TN" => {
        Some(stationUpdater.getObservedProperty(
          Phenomena.instance.AIR_TEMPERATURE_MINIMUM, id, Units.FAHRENHEIT, source))
      }
      case "MT" => {
        Some(stationUpdater.getObservedProperty(
          Phenomena.instance.FUEL_TEMPERATURE, id, Units.FAHRENHEIT, source))
      }
      case "XR" => {
        Some(stationUpdater.getObservedProperty(
          Phenomena.instance.RELATIVE_HUMIDITY, id, Units.PERCENT, source))
      }
      case "VB" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.BATTERY_VOLTAGE, id,
          Units.VOLTAGE, source))
      }
      case "MM" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.FUEL_MOISTURE, id,
          Units.PERCENT, source))
      }
      case "RW" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.SOLAR_RADIATION, id,
          Units.WATT_PER_METER_SQUARED, source))
      }
      case "RS" => {
        Some(stationUpdater.getObservedProperty(
          Phenomena.instance.PHOTOSYNTHETICALLY_ACTIVE_RADIATION, id,
          Units.WATT_PER_METER_SQUARED, source))
      }
      case "TW" => {
        Some(stationUpdater.getObservedProperty(
          Phenomena.instance.SEA_WATER_TEMPERATURE, id,
          Units.FAHRENHEIT, source))
      }
      case "TW2" => {
        return None
        //        return new Some[ObservedProperty](
        //          stationUpdater.createObservedProperty(Sensors.WATER_TEMPERATURE, id,
        //            source, "Water Temperature, 2nd Sensor", Units.FAHRENHEIT, 
        //            SensorPhenomenonIds.SEA_WATER_TEMPERATURE))
      }
      case "WT" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.TURBIDITY, id,
          Units.NEPHELOMETRIC_TURBIDITY_UNITS, source))
      }
      case "WC" => {
        Some(stationUpdater.getObservedProperty(
          Phenomena.instance.SEA_WATER_ELECTRICAL_CONDUCTIVITY, id,
          Units.MICRO_MHOS_PER_CENTIMETERS, source))
      }
      case "WP" => {
        Some(stationUpdater.getObservedProperty(
          Phenomena.instance.SEA_WATER_PH_REPORTED_ON_TOTAL_SCALE, id,
          Units.STD_UNITS, source))
      }
      case "WO" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.DISSOLVED_OXYGEN, id,
          Units.PARTS_PER_MILLION, source))
      }
      case "WX" => {
        Some(stationUpdater.getObservedProperty(
          Phenomena.instance.DISSOLVED_OXYGEN_SATURATION, id, Units.PERCENT,
          source))
      }
      case "TD" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.DEW_POINT_TEMPERATURE, id,
          Units.FAHRENHEIT, source))
      }
      case "HG" => {
        val url = Phenomena.GENERIC_FAKE_MMI_URL_PREFIX + "stream_gage_height"
        Some(stationUpdater.getObservedProperty(
          phenomenaFactory.findCustomPhenomenon(url), id, Units.FEET, source))
      }
      case "HG2" => {
        return None
        //        return new Some[ObservedProperty](
        //          stationUpdater.createObservedProperty(Sensors.STREAM_GAGE_HEIGHT, id,
        //            source, "Stream Gage Height, 2nd Sensor", Units.FEET, 
        //            SensorPhenomenonIds.STREAM_GAGE_HEIGHT))
      }
      case "HP" => {
        Some(stationUpdater.getObservedProperty(
          Phenomena.instance.WATER_SURFACE_HEIGHT_ABOVE_REFERENCE_DATUM, id,
          Units.FEET, source))
      }
      case "WS" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.SALINITY, id,
          Units.PARTS_PER_TRILLION, source))
      }
      case "HM" => {
        val url = Phenomena.GENERIC_FAKE_MMI_URL_PREFIX + "water_level"
        Some(stationUpdater.getObservedProperty(
          phenomenaFactory.findCustomPhenomenon(url), id,
          Units.FEET, source))
      }
      case "PA" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.AIR_PRESSURE, id,
          Units.HECTOPASCAL, source))
      }
      case "SD" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.SNOW_DEPTH, id,
          Units.INCHES, source))
      }
      case "SW" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.SNOW_WATER_EQUIVALENT, id,
          Units.METERS, source))
      }
      case "TS" => {
        Some(stationUpdater.getObservedProperty(
          Phenomena.instance.SOIL_TEMPERATURE, id,
          Units.FAHRENHEIT, source))
      }
      case _ => {
        LOGGER.debug("[" + source.name + "] observed property: " + id +
          " is not processed correctly.")
        return None
      }
    }
  }
}
