package com.axiomalaska.sos.harvester.source.stationupdater

import java.util.Calendar
import java.util.TimeZone
import com.axiomalaska.sos.tools.HttpPart
import com.axiomalaska.sos.tools.HttpSender
import java.text.SimpleDateFormat
import org.apache.log4j.Logger
import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import scala.collection.JavaConversions._
import com.axiomalaska.phenomena.Phenomena
import com.axiomalaska.phenomena.Phenomenon
import com.axiomalaska.sos.harvester.BoundingBox;
import com.axiomalaska.sos.harvester.GeoTools;
import com.axiomalaska.sos.harvester.source.SourceUrls;
import com.axiomalaska.sos.harvester.StationQuery;
import com.axiomalaska.sos.harvester.Units;
import com.axiomalaska.sos.harvester.data.DatabasePhenomenon;
import com.axiomalaska.sos.harvester.data.DatabaseSensor;
import com.axiomalaska.sos.harvester.data.DatabaseStation;
import com.axiomalaska.sos.harvester.data.ObservedProperty;
import com.axiomalaska.sos.harvester.data.SensorPhenomenonIds;
import com.axiomalaska.sos.harvester.data.Source;
import com.axiomalaska.sos.harvester.data.SourceId;
import com.axiomalaska.sos.harvester.data.LocalPhenomenon
import com.axiomalaska.ioos.sos.GeomHelper
import scala.collection.mutable.{Set => MSet}
import com.axiomalaska.ioos.sos.GeomHelper

class RawsStationUpdater(private val stationQuery: StationQuery,
  private val boundingBox: BoundingBox) extends StationUpdater {

  // ---------------------------------------------------------------------------
  // Private Data
  // ---------------------------------------------------------------------------
  
  private val LOGGER = Logger.getLogger(getClass())
  private val stationUpdater = new StationUpdateTool(stationQuery)
  private val foreignIdParser = """/cgi-bin/rawMAIN\.pl\?(.*)""".r
  private val labelParser = """.*<strong>(.*)</strong>.*""".r
  private val latLongParser = """(-?\d+). (\d+)' (\d+).""".r
  private val yearFormatDate = new SimpleDateFormat("yy")
  private val monthFormatDate = new SimpleDateFormat("MM")
  private val dayFormatDate = new SimpleDateFormat("dd")
  private val httpSender = new HttpSender()
  private val geoTools = new GeoTools()
  private val source = stationQuery.getSource(SourceId.RAWS)
  private val nonStates = Set("pr")
  private val stateCodeMap = Map(
    "Alabama" -> Set("al_ms"),
    "Alaska" -> Set("ak"),
    "Arizona" -> Set("az"),
    "Arkansas" -> Set("ar"),
    "California" -> Set("nca","cca","sca"),
    "Colorado" -> Set("co"),
    "Connecticut" -> Set("ct_ma_ri"),
    "Delaware" -> Set("de_md"),
    "District of Columbia" -> Set("de_md"),
    "Florida" -> Set("fl"),
    "Georgia" -> Set("ga_sc"),
    "Hawaii" -> Set("hi"),
    "Idaho" -> Set("nidwmt","sid"),
    "Illinois" -> Set("il"),
    "Indiana" -> Set("in"),
    "Iowa" -> Set("ia"),
    "Kansas" -> Set("ks"),
    "Kentucky" -> Set("ky_tn"),
    "Louisiana" -> Set("la"),
    "Maine" -> Set("me_nh_vt"),
    "Maryland" -> Set("de_md"),
    "Massachusetts" -> Set("ct_ma_ri"),
    "Michigan" -> Set("mi_wi"),
    "Minnesota" -> Set("mn"),
    "Mississippi" -> Set("al_ms"),
    "Missouri" -> Set("mo"),
    "Montana" -> Set("emt","nidwmt"),
    "Nebraska" -> Set("ne"),
    "Nevada" -> Set("nv"),
    "New Hampshire" -> Set("me_nh_vt"),
    "New Jersey" -> Set("nj_pa"),
    "New Mexico" -> Set("nm"),
    "New York" -> Set("ny"),
    "North Carolina" -> Set("nc"),
    "North Dakota" -> Set("nd"),
    "Ohio" -> Set("oh"),
    "Oklahoma" -> Set("ok"),
    "Oregon" -> Set("or"),
    "Pennsylvania" -> Set("nj_pa"),
    "Rhode Island" -> Set("ct_ma_ri"),
    "South Carolina" -> Set("ga_sc"),
    "South Dakota" -> Set("sd"),
    "Tennessee" -> Set("ky_tn"),
    "Texas" -> Set("tx"),
    "Utah" -> Set("ut"),
    "Vermont" -> Set("me_nh_vt"), 
    "Virginia" -> Set("va_wv"),
    "Washigton" -> Set("wa"),
    "West Virginia" -> Set("va_wv"),
    "Wisconsin" -> Set("mi_wi"),
    "Wyoming" -> Set("wy")
  )
  
  
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
    LOGGER.info(stations.size + " stations unfiltered")
    val stationSensorsCollection = for {
      (station, index) <- stations.zipWithIndex
      val location = GeomHelper.createLatLngPoint(station.latitude, station.longitude)
      if (geoTools.isStationWithinRegion(location, boundingBox))
      val sourceObservedProperties = getSourceObservedProperties(station, source)
      val databaseObservedProperties = 
        stationUpdater.updateObservedProperties(source, sourceObservedProperties)
      val sensors = stationUpdater.getSourceSensors(station, databaseObservedProperties)
      if (sensors.nonEmpty)
    } yield {
      LOGGER.debug("[" + index + " of " + size + "] station: " + station.name)
      (station, sensors)
    }

    LOGGER.info("Finished processing " + stationSensorsCollection.size + " stations")

    return stationSensorsCollection
  }
  
  private def getStateStationElements(stateUrl:String): List[String] = {
    val rawData = HttpSender.sendGetMessage(stateUrl)
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
    LOGGER.info("Processing " + stationForeignIds.size + " stations")

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
    
  private def getRawsRegionUrls():List[String] = {
    val stateCodes = MSet(GeoTools.statesInBoundingBox(boundingBox).map(state => stateCodeMap(state)).flatten).flatten
    //TODO disabling non-state harvest for now, add more robust spatial layer in the future    
//    stateCodes ++= nonStates
    stateCodes.map(stateCode => SourceUrls.RAWS_STATE_URL_TEMPLATE.format(stateCode)).toList
  }

  private def createStation(foreignId: String): Option[DatabaseStation] = {
    val response = HttpSender.sendGetMessage(
        SourceUrls.RAWS_STATION_INFORMATION + foreignId)

    if (response != null) {
      val siteDoc = Jsoup.parse(response)

      val label = getStationName(siteDoc)
      val lat = getLatitude(siteDoc)
      val lon = getLongitude(siteDoc)

      LOGGER.info("Processed station: " + label)
      return Some(new DatabaseStation(label, source.tag + ":" + foreignId, foreignId, "", 
          "FIXED MET STATION", source.id, lat, lon, null, null))
    } else {
      LOGGER.info("response not found ------------------------")
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
        SourceUrls.RAWS_OBSERVATION_RETRIEVAL, parts);

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
        Some(stationUpdater.getObservedProperty(Phenomena.instance.PRECIPITATION_ACCUMULATED, id, 
            Units.MILLIMETERS, source))
      }
      case "AverageRelativeHumidity" => {
        Some(stationUpdater.getObservedProperty(
            Phenomena.instance.RELATIVE_HUMIDITY_AVERAGE, id, Units.PERCENT, source))
      }
      case "MaximumRelativeHumidity" => {
        Some(stationUpdater.getObservedProperty(
            Phenomena.instance.RELATIVE_HUMIDITY_MAXIMUM, id, Units.PERCENT, source))
      }
      case "MinimumRelativeHumidity" => {
        Some(stationUpdater.getObservedProperty(
            Phenomena.instance.RELATIVE_HUMIDITY_MINIMUM, id, Units.PERCENT, source))
      }
      case "BarometricPressure" => {
        Some(stationUpdater.getObservedProperty(
            Phenomena.instance.AIR_PRESSURE, id, Units.MILLI_BAR, source))
      }
      case "DewPointTemp" => {
        Some(stationUpdater.getObservedProperty(
            Phenomena.instance.DEW_POINT_TEMPERATURE, id, Units.CELSIUS, source))
      }
      case "MeanWindSpeed" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.WIND_SPEED, 
            id, Units.METER_PER_SECONDS, source))
      }
      case "MaximumWindGust" => {
        Some(stationUpdater.getObservedProperty(
            Phenomena.instance.WIND_SPEED_OF_GUST, id, Units.METER_PER_SECONDS, source))
      }
      case "MeanWindDirection" => {
        Some(stationUpdater.getObservedProperty(
            Phenomena.instance.WIND_FROM_DIRECTION, id, Units.DEGREES, source))
      }
      case "AverageAirTemperature" => {
        Some(stationUpdater.getObservedProperty(
            Phenomena.instance.AIR_TEMPERATURE_AVERAGE, id, Units.CELSIUS, source))
      }
      case "AirTemperature" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.AIR_TEMPERATURE, 
            id, Units.CELSIUS, source))
      }
      case "AIRTEMP.1FOOT" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.AIR_TEMPERATURE, 
            id, Units.CELSIUS, 0.3048, source))
      }
      case "AIRTEMP.3FOOT" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.AIR_TEMPERATURE, 
            id, Units.CELSIUS, 0.9144, source))
      }
      case "AIRTEMP.8FOOT" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.AIR_TEMPERATURE, 
            id, Units.CELSIUS, 2.4384, source))
      }
      case "AIRTEMP.15FOOT" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.AIR_TEMPERATURE, 
            id, Units.CELSIUS, 4.572, source))
      }
      case "MaximumAirTemperature" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.AIR_TEMPERATURE_MAXIMUM, 
            id, Units.CELSIUS, source))
      }
      case "MinimumAirTemperature" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.AIR_TEMPERATURE_MINIMUM, 
            id, Units.CELSIUS, source))
      }
      case "AveFuelTemp" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.FUEL_TEMPERATURE, 
            id, Units.CELSIUS, source))
      }
      case "BatteryVoltage" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.BATTERY_VOLTAGE, 
            id, Units.VOLTAGE, source))
      }
      case "FuelMoistureAverage" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.FUEL_MOISTURE, 
            id, Units.PERCENT, source))
      }
      case "AverageSoilTemperature4Inches" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.SOIL_TEMPERATURE, 
            id, Units.CELSIUS, 0.1016, source))
      }
      case "AverageSoilTemperature20Inches" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.SOIL_TEMPERATURE, 
            id, Units.CELSIUS, 0.508, source))
      }
      // The name is correct with the Y
      case "AverageSoilYemperature40Inches" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.SOIL_TEMPERATURE, 
            id, Units.CELSIUS, 1.016, source))
      }
      case "SoilTemperatureSensor1" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.SOIL_TEMPERATURE, 
            id, Units.CELSIUS, source))
      }
      case "SoilTemperauter2ndSensor" => {
        return None
      }
      case "SolarRadiation2NDS" => {
        return None
      }
      case "SolarRadiation" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.SOLAR_RADIATION, 
            id, Units.WATT_PER_METER_SQUARED, source))
      }
      case "SnowDepth" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.SNOW_DEPTH, 
            id, Units.MILLIMETERS, source))
      }
      case "SnowPillow" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.SNOW_PILLOW, 
            id, Units.MILLIMETERS, source))
      }
      case "SOILMOISTURE%" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.SOIL_MOISTURE_PERCENT, 
            id, Units.PERCENT, source))
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
        LOGGER.debug("[" + source.name + "] observed property: " + id +
          " is not processed correctly.")
        return None
      }
    }
  }
}
