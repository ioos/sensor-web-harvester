package com.axiomalaska.sos.source.stationupdater

import java.util.Calendar
import java.util.TimeZone
import java.text.SimpleDateFormat

import org.apache.log4j.Logger

import org.jsoup.Jsoup
import org.jsoup.nodes.Document

import scala.collection.JavaConversions._

import com.axiomalaska.phenomena.Phenomena
import com.axiomalaska.phenomena.Phenomenon
import com.axiomalaska.sos.data.Location
import com.axiomalaska.sos.tools.HttpPart
import com.axiomalaska.sos.tools.HttpSender
import com.axiomalaska.sos.source.BoundingBox
import com.axiomalaska.sos.source.data.DatabasePhenomenon
import com.axiomalaska.sos.source.data.DatabaseSensor
import com.axiomalaska.sos.source.data.DatabaseStation
import com.axiomalaska.sos.source.GeoTools
import com.axiomalaska.sos.source.data.LocalPhenomenon
import com.axiomalaska.sos.source.data.ObservedProperty
import com.axiomalaska.sos.source.data.Source
import com.axiomalaska.sos.source.StationQuery
import com.axiomalaska.sos.source.Units
import com.axiomalaska.sos.source.data.SensorPhenomenonIds
import com.axiomalaska.sos.source.data.SourceId

class RawsStationUpdater(private val stationQuery: StationQuery,
  private val boundingBox: BoundingBox, 
  private val logger: Logger = Logger.getRootLogger()) extends StationUpdater {

  // ---------------------------------------------------------------------------
  // Private Data
  // ---------------------------------------------------------------------------

  private val stationUpdater = new StationUpdateTool(stationQuery, logger)
  private val foreignIdParser = """/cgi-bin/rawMAIN\.pl\?(.*)""".r
  private val labelParser = """.*<strong>(.*)</strong>.*""".r
//  private val latLongParser = """(-?\d+). (\d+)' (\d+).\""".r
  private val latLongParser = """(-?\d+). (\d+)' (\d+).""".r
  private val yearFormatDate = new SimpleDateFormat("yy")
  private val monthFormatDate = new SimpleDateFormat("MM")
  private val dayFormatDate = new SimpleDateFormat("dd")
  private val httpSender = new HttpSender()
  private val geoTools = new GeoTools()
  private val source = stationQuery.getSource(SourceId.RAWS)

  // ---------------------------------------------------------------------------
  // Public Members
  // ---------------------------------------------------------------------------

  def update() {
    val sourceStationSensors = getSourceStations()

    val databaseStations = stationQuery.getAllStations(source)

    stationUpdater.updateStations(sourceStationSensors, databaseStations)
  }
  
  val name = "RAWS"

  // ---------------------------------------------------------------------------
  // Private Members
  // ---------------------------------------------------------------------------

  private def getSourceStations(): 
	  List[(DatabaseStation, List[(DatabaseSensor, List[DatabasePhenomenon])])] = {

    val stations = createStations
    val size = stations.length - 1
    logger.info(stations.size + " stations unfiltered")
    val stationSensorsCollection = for {
      (station, index) <- stations.zipWithIndex
      if (withInBoundingBox(station))
      val sourceObservedProperties = getSourceObservedProperties(station, source)
      val databaseObservedProperties = 
        stationUpdater.updateObservedProperties(source, sourceObservedProperties)
      val sensors = stationUpdater.getSourceSensors(station, databaseObservedProperties)
      if (sensors.nonEmpty)
    } yield {
      logger.debug("[" + index + " of " + size + "] station: " + station.name)
      (station, sensors)
    }

    logger.info("Finished processing " + stationSensorsCollection.size + " stations")

    return stationSensorsCollection
  }
  
  private def getStateStationElements(stateUrl:String): List[String] = {
    val rawData = httpSender.sendGetMessage(stateUrl)
    if (rawData != null) {
      val mainDoc = Jsoup.parse(rawData);

      val stationElements = mainDoc.getElementsByTag("A").filter(_.hasAttr("onmouseover"))

      val foreignIds = stationElements.map(element => {
        val foreignIdParser(foreignId) = element.attr("href")
        foreignId
      })
      foreignIds.toList
    } else {
      Nil
    }
  }
  
  private def createStations():List[DatabaseStation] ={
    val stationForeignIds = getAllStationForeignIds
    logger.info("Processing " + stationForeignIds.size + " stations")

    getAllStationForeignIds.flatMap(createStation)
  }
  
  private def getAllStationForeignIds(): List[String] = 
    getRawsRegionUrls().flatMap(getStateStationElements).distinct
  
  private def getSourceObservedProperties(station: DatabaseStation,
    source: Source): List[ObservedProperty] = {
    val sensorNames = getSensorNames(station.foreign_tag)
    val sourceObservedProperties = sensorNames.flatMap(sensorName =>
      getObservedProperty(station, sensorName))

    return sourceObservedProperties
  }
  
  private def getRawsRegionUrls():List[String] =
    List(
        "http://www.raws.dri.edu/aklst.html", 
        "http://www.raws.dri.edu/azlst.html",
        "http://www.raws.dri.edu/ncalst.html",
        "http://www.raws.dri.edu/ccalst.html", 
        "http://www.raws.dri.edu/scalst.html", 
        "http://www.raws.dri.edu/colst.html", 
        "http://www.raws.dri.edu/hilst.html", 
        "http://www.raws.dri.edu/nidwmtlst.html", 
        "http://www.raws.dri.edu/sidlst.html", 
        "http://www.raws.dri.edu/emtlst.html",
        "http://www.raws.dri.edu/nidwmtlst.html", 
        "http://www.raws.dri.edu/nvlst.html", 
        "http://www.raws.dri.edu/nmlst.html", 
        "http://www.raws.dri.edu/orlst.html", 
        "http://www.raws.dri.edu/utlst.html", 
        "http://www.raws.dri.edu/walst.html", 
        "http://www.raws.dri.edu/wylst.html", 
        "http://www.raws.dri.edu/illst.html", 
        "http://www.raws.dri.edu/inlst.html",
        "http://www.raws.dri.edu/ialst.html", 
        "http://www.raws.dri.edu/kslst.html", 
        "http://www.raws.dri.edu/ky_tnlst.html", 
        "http://www.raws.dri.edu/mi_wilst.html", 
        "http://www.raws.dri.edu/mnlst.html", 
        "http://www.raws.dri.edu/molst.html", 
        "http://www.raws.dri.edu/nelst.html", 
        "http://www.raws.dri.edu/ndlst.html", 
        "http://www.raws.dri.edu/ohlst.html", 
        "http://www.raws.dri.edu/sdlst.html", 
        "http://www.raws.dri.edu/mi_wilst.html", 
        "http://www.raws.dri.edu/al_mslst.html", 
        "http://www.raws.dri.edu/arlst.html", 
        "http://www.raws.dri.edu/fllst.html", 
        "http://www.raws.dri.edu/ga_sclst.html", 
        "http://www.raws.dri.edu/lalst.html", 
        "http://www.raws.dri.edu/nclst.html", 
        "http://www.raws.dri.edu/oklst.html", 
        "http://www.raws.dri.edu/txlst.html", 
        "http://www.raws.dri.edu/prlst.html", 
        "http://www.raws.dri.edu/ct_ma_rilst.html", 
        "http://www.raws.dri.edu/de_mdlst.html", 
        "http://www.raws.dri.edu/me_nh_vtlst.html", 
        "http://www.raws.dri.edu/nj_palst.html", 
        "http://www.raws.dri.edu/nylst.html", 
        "http://www.raws.dri.edu/va_wvlst.html")

  private def createStation(foreignId: String): Option[DatabaseStation] = {
    val response = httpSender.sendGetMessage(
      "http://www.raws.dri.edu/cgi-bin/wea_info.pl?" + foreignId)

    if (response != null) {
      try {
        val siteDoc = Jsoup.parse(response)

        val label = getStationName(siteDoc)
        val lat = getLatitude(siteDoc)
        val lon = getLongitude(siteDoc)

        logger.info("Processed station: " + label)
        return Some(new DatabaseStation(label, foreignId, foreignId, "", 
            "FIXED MET STATION", source.id, lat, lon))
      } catch {
        case ex: Exception => {
            logger.error("Could not read in station id " + foreignId)
            None
        }
      }
    } else {
      logger.info("response not found ------------------------")
      None
    }
  }
  
  private def getStationName(siteDoc: Document): String = {
    val rawLabel = 
      siteDoc.getElementsContainingOwnText("Location").head.parent().nextElementSibling().text

    val label = rawLabel.replace("(RAWS)", "").trim

    return label
  }

  private def getLatitude(siteDoc: Document): Double = {
    val rawLat = 
      siteDoc.getElementsContainingOwnText("Latitude").head.parent().nextElementSibling().text

    val latLongParser(degree, minute, second) = rawLat

    val lat: Double = degree.toDouble + minute.toDouble / 60 + second.toDouble / 60 / 60

    return lat
  }

  private def getLongitude(siteDoc: Document): Double = {
    val rawLong = 
      siteDoc.getElementsContainingOwnText("Longitude").head.parent().nextElementSibling().text

    val latLongParser(degree, minute, second) = rawLong

    val long: Double = (-1) * (degree.toDouble + minute.toDouble / 60 + second.toDouble / 60 / 60)

    return long
  }

  private def withInBoundingBox(station: DatabaseStation): Boolean = {
    val stationLocation = new Location(station.latitude, station.longitude)
    return geoTools.isStationWithinRegion(stationLocation, boundingBox)
  }

  private def getSensorNames(foreignId: String): List[String] = {
    val endDate = Calendar.getInstance(TimeZone.getTimeZone("GMT-9:00"))
    val startDate = Calendar.getInstance(TimeZone.getTimeZone("GMT-9:00"))
    startDate.add(Calendar.DAY_OF_MONTH, -1)

    val parts = List(
      new HttpPart("stn", foreignId.substring(foreignId.length() - 4, foreignId.length())),
      new HttpPart("smon", monthFormatDate.format(startDate.getTime)),
      new HttpPart("sday", dayFormatDate.format(startDate.getTime)),
      new HttpPart("syea", yearFormatDate.format(startDate.getTime)),
      new HttpPart("emon", monthFormatDate.format(endDate.getTime)),
      new HttpPart("eday", dayFormatDate.format(endDate.getTime)),
      new HttpPart("eyea", yearFormatDate.format(endDate.getTime)),
      new HttpPart("dfor", "02"),
      new HttpPart("srce", "W"),
      new HttpPart("miss", "03"),
      new HttpPart("flag", "N"),
      new HttpPart("Dfmt", "02"),
      new HttpPart("Tfmt", "01"),
      new HttpPart("Head", "02"),
      new HttpPart("Deli", "01"),
      new HttpPart("unit", "M"),
      new HttpPart("WsMon", "01"),
      new HttpPart("WsDay", "01"),
      new HttpPart("WeMon", "12"),
      new HttpPart("WeDay", "31"),
      new HttpPart("WsHou", "00"),
      new HttpPart("WeHou", "24"))

    val results = httpSender.sendPostMessage(
      "http://www.raws.dri.edu/cgi-bin/wea_list2.pl", parts);

    if (results != null) {
      val doc = Jsoup.parse(results);

      val body = doc.getElementsByTag("PRE").text();

      val sensorParser = """:(.*)\(""".r

      val sensorNames = for (parsedData <- sensorParser.findAllIn(body)) yield {
        val sensorParser(rawSensor) = parsedData
        val sensor: String = rawSensor.replaceAll("\\(.*\\)", "").replaceAll(" ", "").replaceAll(":", "").replaceAll("-", "").replaceAll("#", "").trim
        sensor
      }

      sensorNames.toList
    } else {
      Nil
    }
  }

  private def getObservedProperty(station: DatabaseStation, id: String): Option[ObservedProperty] = {
    id match {
      case "Precipitation" => {
        getObservedProperty(Phenomena.instance.PRECIPITATION_ACCUMULATED, id)
      }
      case "AverageRelativeHumidity" => {
        getObservedProperty(Phenomena.instance.RELATIVE_HUMIDITY_AVERAGE, id)
      }
      case "MaximumRelativeHumidity" => {
        getObservedProperty(Phenomena.instance.RELATIVE_HUMIDITY_MAXIMUM, id)
      }
      case "MinimumRelativeHumidity" => {
        getObservedProperty(Phenomena.instance.RELATIVE_HUMIDITY_MINIMUM, id)
      }
      case "BarometricPressure" => {
        getObservedProperty(Phenomena.instance.AIR_PRESSURE, id)
      }
      case "DewPointTemp" => {
        getObservedProperty(Phenomena.instance.DEW_POINT_TEMPERATURE, id)
      }
      case "MeanWindSpeed" => {
        getObservedProperty(Phenomena.instance.WIND_SPEED, id)
      }
      case "MaximumWindGust" => {
        getObservedProperty(Phenomena.instance.WIND_SPEED_OF_GUST, id)
      }
      case "MeanWindDirection" => {
        getObservedProperty(Phenomena.instance.WIND_FROM_DIRECTION, id)
      }
      case "AverageAirTemperature" => {
        getObservedProperty(Phenomena.instance.AIR_TEMPERATURE_AVERAGE, id)
      }
      case "AirTemperature" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(id,
            source, Units.CELSIUS,
            SensorPhenomenonIds.AIR_TEMPERATURE))
        getObservedProperty(Phenomena.instance.AIR_TEMPERATURE, id)
      }
      case "AIRTEMP.1FOOT" => {
        getObservedProperty(Phenomena.instance.AIR_TEMPERATURE, id, 0.3048)
      }
      case "AIRTEMP.3FOOT" => {
        getObservedProperty(Phenomena.instance.AIR_TEMPERATURE, id, 0.9144)
      }
      case "AIRTEMP.8FOOT" => {
        getObservedProperty(Phenomena.instance.AIR_TEMPERATURE, id, 2.4384)
      }
      case "AIRTEMP.15FOOT" => {
        getObservedProperty(Phenomena.instance.AIR_TEMPERATURE, id, 4.572)
      }
      case "MaximumAirTemperature" => {
        getObservedProperty(Phenomena.instance.AIR_TEMPERATURE_MAXIMUM, id)
      }
      case "MinimumAirTemperature" => {
        getObservedProperty(Phenomena.instance.AIR_TEMPERATURE_MINIMUM, id)
      }
      case "AveFuelTemp" => {
        getObservedProperty(Phenomena.instance.FUEL_TEMPERATURE, id)
      }
      case "BatteryVoltage" => {
        getObservedProperty(Phenomena.instance.BATTERY_VOLTAGE, id)
      }
      case "FuelMoistureAverage" => {
        getObservedProperty(Phenomena.instance.FUEL_MOISTURE, id)
      }
      case "AverageSoilTemperature4Inches" => {
        getObservedProperty(Phenomena.instance.createHomelessParameter("ground_temperature_observed", Units.CELSIUS), id, 0.1016)
      }
      case "AverageSoilTemperature20Inches" => {
        getObservedProperty(Phenomena.instance.createHomelessParameter("ground_temperature_observed", Units.CELSIUS), id, 0.508)
      }
      // The name is correct with the Y
      case "AverageSoilYemperature40Inches" => {
        getObservedProperty(Phenomena.instance.createHomelessParameter("ground_temperature_observed", Units.CELSIUS), id, 1.016)
      }
      case "SoilTemperatureSensor1" => {
        getObservedProperty(Phenomena.instance.createHomelessParameter("ground_temperature_observed", Units.CELSIUS), id)
      }
      case "SoilTemperauter2ndSensor" => {
        return None
      }
      case "SolarRadiation2NDS" => {
        return None
      }
      case "SolarRadiation" => {
        getObservedProperty(Phenomena.instance.SOLAR_RADIATION, id)
      }
      case "SnowDepth" => {
        getObservedProperty(Phenomena.instance.SNOW_DEPTH, id)
      }
      case "SnowPillow" => {
        getObservedProperty(Phenomena.instance.SNOW_PILLOW, id)
      }
      case "SOILMOISTURE%" => {
        getObservedProperty(Phenomena.instance.SOIL_MOISTURE_PERCENT, id)
      }
      case "TimeofDay" => {
        return None
      }
      case "DayofYear" => {
        return None
      }
      case "DirectionofMaxGust" => {
        return None
      }
      case "ShaftEncoderAve." => {
        return None
      }
      case "10HrFuelMoisture" => {
        return None
      }
      case "RelativeHumidity24HrHigh" => {
        return None
      }
      case "RelativeHumidity24HrLow" => {
        return None
      }
      case "AirTemperture24HourLow" => {
        return None
      }
      case "AirTemperture24HourHigh" => {
        return None
      }
      case "RelativeHumidity12HrLow" => {
        return None
      }
      case "RelativeHumidity12HrHigh" => {
        return None
      }
      case "AirTemperture12HourHigh" => {
        return None
      }
      case "AirTemperture12HourLow" => {
        return None
      }
      case "Visibility" => {
        return None
      }
      case "RaingaugeNumber2" => {
        return None
      }
      case "AvePAR" => {
        return None
      }
      case "MiscellaneousNumber1" => {
        return None
      }
      case "MiscellaneousNumber3" => {
        return None
      }
      case "MiscellaneousNumber4" => {
        return None
      }
      case "SOILM.TENS.cBARS" => {
        return None
      }
      case "WindDirection2" => {
        return None
      }
      case "WindSpeed2" => {
        return None
      }
      case "AverageRelativeHumidity2" => {
        return None
      }
      case "Ave.AirTemperature2" => {
        return None
      }
      case _ => {
        logger.debug("[" + source.name + "] observed property: " + id +
          " is not processed correctly.")
        return None
      }
    }
  }
  
  private def getObservedProperty(phenomenon: Phenomenon, foreignTag: String) : Option[ObservedProperty] = {
    getObservedProperty(phenomenon, foreignTag, 0)
  }
  
  private def getObservedProperty(phenomenon: Phenomenon, foreignTag: String, depth: Double) : Option[ObservedProperty] = {
    try {
      var localPhenom: LocalPhenomenon = new LocalPhenomenon(new DatabasePhenomenon(phenomenon.getId))
      var units: String = if (phenomenon.getUnit == null || phenomenon.getUnit.getSymbol == null) "none" else phenomenon.getUnit.getSymbol
      if (localPhenom.databasePhenomenon.id < 0) {
        localPhenom = new LocalPhenomenon(insertPhenomenon(localPhenom.databasePhenomenon, units, phenomenon.getId, phenomenon.getName))
      }
      return new Some[ObservedProperty](stationUpdater.createObservedProperty(foreignTag, source, localPhenom.getUnit.getSymbol, localPhenom.databasePhenomenon.id, depth))
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