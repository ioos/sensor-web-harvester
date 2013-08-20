package com.axiomalaska.sos.harvester.source.stationupdater

import com.axiomalaska.sos.tools.HttpSender
import com.axiomalaska.phenomena.Phenomena
import com.axiomalaska.phenomena.Phenomenon
import com.axiomalaska.sos.harvester.BoundingBox;
import com.axiomalaska.sos.harvester.GeoTools;
import com.axiomalaska.sos.harvester.StationQuery;
import com.axiomalaska.sos.harvester.Units;
import com.axiomalaska.sos.harvester.data.DatabasePhenomenon;
import com.axiomalaska.sos.harvester.data.DatabaseSensor;
import com.axiomalaska.sos.harvester.data.DatabaseStation;
import com.axiomalaska.sos.harvester.data.ObservedProperty;
import com.axiomalaska.sos.harvester.data.SourceId;
import com.axiomalaska.sos.harvester.data.LocalPhenomenon
import scala.collection.mutable
import scala.collection.JavaConversions._
import org.apache.log4j.Logger
import org.jsoup.Jsoup
import com.axiomalaska.ioos.sos.GeomHelper
import com.axiomalaska.sos.harvester.data.PhenomenaFactory

/**
 * Currently not being used. It was replaced by the NdbcSosStationUpdater
 */
class NdbcStationUpdater(private val stationQuery: StationQuery,
  private val boundingBox: BoundingBox) extends StationUpdater {

  // ---------------------------------------------------------------------------
  // Private Data
  // ---------------------------------------------------------------------------
  
  private val LOGGER = Logger.getLogger(getClass())
  private val stationUpdater = new StationUpdateTool(stationQuery)
  private val source = stationQuery.getSource(SourceId.NDBC)
  private val httpSender = new HttpSender()
  private val locationParser = """.*<strong>Location:</strong> (\d*\.\d*)N (\d*\.\d*)W<br />.*""".r
  private val textParser = """(\d{4})\s+(\d{2})\s+(\d{2})\s+(\d{2})\s+(\d{2})\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+?)""".r
  private val specParser = """(\d{4})\s+(\d{2})\s+(\d{2})\s+(\d{2})\s+(\d{2})\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)""".r
  private val geoTools = new GeoTools()
  private val latParser = """var stnlat = (.*?);""".r
  private val lonParser = """var stnlon = (.*?);""".r
  private val nameParser = """Station.*?[<a.*?<//a>.*?]?-\s*(.*)""".r
  private val phenomenaFactory = new PhenomenaFactory()
  
  // ---------------------------------------------------------------------------
  // Public Members
  // ---------------------------------------------------------------------------

  def update() {
    val sourceStationSensors = getSourceStations()

    val databaseStations = stationQuery.getAllStations(source)

    stationUpdater.updateStations(sourceStationSensors, databaseStations)
  }
  
  val name = "NDBC"
    
  // ---------------------------------------------------------------------------
  // Private Members
  // ---------------------------------------------------------------------------

  private def getSourceStations(): List[(DatabaseStation, List[(DatabaseSensor, List[DatabasePhenomenon])])] = {
    
    val foreignIds = getAllForeignIds()

    val size = foreignIds.length - 1
    LOGGER.info("Total number of staitons: " + foreignIds.length)
    val stationSensorsCollection = for {(foreignId, index) <- foreignIds.zipWithIndex
      station <- createSourceStation(foreignId)
      if (withInBoundingBox(station))
      val sourceObservedProperties = getSourceObservedProperties(station)
      val databaseObservedProperties =
        stationUpdater.updateObservedProperties(source, sourceObservedProperties)
      val sensors = stationUpdater.getSourceSensors(station, databaseObservedProperties)
      if(sensors.nonEmpty)
    } yield{
      LOGGER.debug("[" + index + " of " + size + "] station: " + station.name)
      (station, sensors)
    }
    LOGGER.info("finished with stations")
    
    return stationSensorsCollection
  }
  
  private def getSourceObservedProperties(station: DatabaseStation): List[ObservedProperty] = {
    val sensorNames = getSensorNames(station.foreign_tag)
    val sourceObservedProperties = sensorNames.flatMap(sensorName =>
      getObservedProperty(sensorName))
      
    return sourceObservedProperties
  }
  
  private def getSensorNames(foreignId: String): List[String] = {
    val textResult = HttpSender.sendGetMessage(
        "http://www.ndbc.noaa.gov/data/5day2/" + foreignId + "_5day.txt")
        
    val sensorNames = mutable.Set.empty[String]
    if (textResult != null) {
      for (patternMatch <- textParser.findAllIn(textResult)) {
        val textParser(year, month, day, hour, min, wdir, wspd, gst, wvht, dpd, apd,
          mwd, pres, atmp, wtmp, dewp, vis, ptdy, tide) = patternMatch
          
        if (wdir != "MM") sensorNames.add("WDIR")
        if (wspd != "MM") sensorNames.add("WSPD")
        if (gst != "MM") sensorNames.add("GST")
        // if(wvht != "MM") sensorNames.add("WVHT")
        if (dpd != "MM") sensorNames.add("DPD")
        // if(apd != "MM") sensorNames.add("APD")
        // if(mwd != "MM") sensorNames.add("MWD")
        if (pres != "MM") sensorNames.add("PRES")
        if (atmp != "MM") sensorNames.add("ATMP")
        if (wtmp != "MM") sensorNames.add("WTMP")
        if (dewp != "MM") sensorNames.add("DEWP")
        if (vis != "MM") sensorNames.add("VIS")
        if (ptdy != "MM") sensorNames.add("PTDY")
        if (tide != "MM") sensorNames.add("TIDE")
      }
    }

    if (HttpSender.doesUrlExist("http://www.ndbc.noaa.gov/data/realtime2/" + foreignId + ".spec")) {

      val specResult = HttpSender.sendGetMessage(
        "http://www.ndbc.noaa.gov/data/5day2/" + foreignId + "_5day.spec")
      if (specResult != null) {
        for (patternMatch <- specParser.findAllIn(specResult)) {
          val specParser(year, month, day, hour, min, wvht, swh, swp, wwh, wwp, swd, wwd, steepness, apd, mwd) = patternMatch
          if (validValue(wvht)) sensorNames.add("WVHT")
          if (validValue(swh)) sensorNames.add("SwH")
          if (validValue(swp)) sensorNames.add("SwP")
          if (validValue(swd)) sensorNames.add("SwD")
          if (validValue(wwh)) sensorNames.add("WWH")
          if (validValue(wwp)) sensorNames.add("WWP")
          if (validValue(wwd)) sensorNames.add("WWD")
          if (validValue(steepness)) sensorNames.add("STEEPNESS")
          if (validValue(apd)) sensorNames.add("APD")
          if (validValue(mwd)) sensorNames.add("MWD")
        }
      }
    }
    
    return sensorNames.toList
  }
  
  private def validValue(value:String):Boolean={
    value != "MM" && value != "-99" && value != "N/A"
  }

  private def withInBoundingBox(station: DatabaseStation): Boolean = {
    val stationLocation = GeomHelper.createLatLngPoint(station.latitude, station.longitude)
    geoTools.isStationWithinRegion(stationLocation, boundingBox)
  }

  private def createSourceStation(foreignId: String): Option[DatabaseStation] = {
    val result = HttpSender.sendGetMessage(
      "http://www.ndbc.noaa.gov/station_page.php?station=" + foreignId)

    if (result != null) {
      val doc = Jsoup.parse(result)

      val latParser(lat) = latParser.findFirstIn(result).get
      val lonParser(lon) = lonParser.findFirstIn(result).get
      val header = doc.getElementsByTag("h1")(0).text()

      val name = nameParser.findFirstMatchIn(header) match {
        case Some(nameMatch) => {
          nameMatch.group(1)
        }
        case None => {
          foreignId
        }
      }

      LOGGER.info("Processing station: " + name)
      Some(new DatabaseStation(name, source.tag + ":" + foreignId, foreignId, "", 
          "BUOY", source.id, lat.toDouble, lon.toDouble, null, null))
    } else {
      None
    }
  }

  private def getAllForeignIds(): List[String] = {
    val result = HttpSender.sendGetMessage("http://www.ndbc.noaa.gov/to_station.shtml")

    if (result != null) {
      val doc = Jsoup.parse(result)

      val ndbcHeader = doc.getElementsMatchingOwnText("National Data Buoy Center Stations").head

      val preElement = ndbcHeader.nextElementSibling

      val foreignIds = preElement.children.map(_.text).toList

      LOGGER.info("Processing foreign IDs")
      val filterForeignIds = for {
        foreignId <- foreignIds
        if (HttpSender.doesUrlExist("http://www.ndbc.noaa.gov/data/5day2/" + foreignId + "_5day.txt"))
      } yield {
        LOGGER.info("Processing foreign ID: " + foreignId)
        foreignId
      }
      filterForeignIds
    } else {
      Nil
    }
  }
  
  private def getObservedProperty(id: String): 
      Option[ObservedProperty] = {
    id match {
      //The direction from which the wind waves at the wind wave period (WWPD) are coming. The units are degrees from true North, increasing clockwise, with North as 0 (zero) degrees and East as 90 degrees.
      case "WWD" => {
        Some(stationUpdater.getObservedProperty(
            Phenomena.instance.SEA_SURFACE_WIND_WAVE_TO_DIRECTION, id, Units.DEGREES, source))
      }
      //The direction from which the swell waves at the swell wave period (SWPD) are coming. The units are degrees from true North, increasing clockwise, with North as 0 (zero) degrees and East as 90 degrees.
      case "SwD" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.SEA_SURFACE_SWELL_WAVE_TO_DIRECTION, id, Units.DEGREES, source))
      }
      //Wind Wave Period is the time (in seconds) that it takes successive wind wave crests or troughs to pass a fixed point.
     case "WWP" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.SEA_SURFACE_WIND_WAVE_PERIOD, id, Units.SECONDS, source))
      }
     //Wind Wave Height is the vertical distance (meters) between any wind wave crest and the succeeding wind wave trough (independent of swell waves).
     case "WWH" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.SEA_SURFACE_WIND_WAVE_SIGNIFICANT_HEIGHT, id, Units.METERS, source))
      }
     //Swell Period is the time (usually measured in seconds) that it takes successive swell wave crests or troughs pass a fixed point.
     case "SwP" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.SEA_SURFACE_SWELL_WAVE_PERIOD, id, Units.SECONDS, source))
     }
     //Swell height is the vertical distance (meters) between any swell crest and the succeeding swell wave trough.
     case "SwH" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.SEA_SURFACE_SWELL_WAVE_SIGNIFICANT_HEIGHT, id, Units.METERS, source))
     }
      //Wind direction (the direction the wind is coming from in degrees clockwise from true N) during the same period used for WSPD.
      case "WDIR" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.WIND_FROM_DIRECTION, id, Units.DEGREES, source))
      }
      //Wind speed (m/s) averaged over an eight-minute period for buoys and a two-minute period for land stations. Reported Hourly. 
      case "WSPD" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.WIND_SPEED, id, Units.METER_PER_SECONDS, source))
      }
      //Peak 5 or 8 second gust speed (m/s) measured during the eight-minute or two-minute period. The 5 or 8 second period can be determined by payload,
      case "GST" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.WIND_SPEED_OF_GUST, id, Units.METER_PER_SECONDS, source))
      }
      //Significant wave height (meters) is calculated as the average of the highest one-third of all of the wave heights during the 20-minute sampling period. 
      case "WVHT" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.SEA_SURFACE_WAVE_SIGNIFICANT_HEIGHT, id, Units.METERS, source))
      }
      //Dominant wave period (seconds) is the period with the maximum wave energy.
      case "DPD" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.DOMINANT_WAVE_PERIOD, id, Units.SECONDS, source))
      }
      //Average wave period (seconds) of all waves during the 20-minute period.
      case "APD" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.SEA_SURFACE_WAVE_MEAN_PERIOD, id, Units.SECONDS, source))
      }
      //The direction from which the waves at the dominant period (DPD) are coming. The units are degrees from true North, increasing clockwise, with North as 0 (zero) degrees and East as 90 degrees. 
      case "MWD" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.SEA_SURFACE_DOMINANT_WAVE_TO_DIRECTION, id, Units.DEGREES, source))
      }
      //Sea level pressure (hPa). 
      case "PRES" => {
        val url = Phenomena.GENERIC_FAKE_MMI_URL_PREFIX + "sea_level_pressure"
        Some(stationUpdater.getObservedProperty(
            phenomenaFactory.findCustomPhenomenon(url), id, Units.HECTOPASCAL, source))
      }
      //Air temperature (Celsius). 
      case "ATMP" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.AIR_TEMPERATURE, id, Units.CELSIUS, source))
      }
      //Sea surface temperature (Celsius).
      case "WTMP" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.SEA_WATER_TEMPERATURE, id, Units.CELSIUS, source))
      }
      //Dewpoint temperature taken at the same height as the air temperature measurement.
      case "DEWP" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.DEW_POINT_TEMPERATURE, id, Units.CELSIUS, source))
      }
      //Station visibility (nautica miles). Note that buoy stations are limited to reports from 0 to 1.6 nmi.
      case "VIS" => {
        None
      }
      //Pressure Tendency is the direction (plus or minus) and the amount of pressure change (hPa)for a three hour period ending at the time of observation. (not in Historical files)
      case "PTDY" => {
        None
      }
      case "STEEPNESS" => {
        None
      }
      //The water level in feet above or below Mean Lower Low Water (MLLW).
      case "TIDE" => {
        Some(stationUpdater.getObservedProperty(
            Phenomena.instance.WATER_SURFACE_HEIGHT_ABOVE_REFERENCE_DATUM, id, Units.FEET, source))
      }
      case _ => {
        LOGGER.debug("[" + source.name + "] observed property: " + id +
          " is not processed correctly.")
        return None
      }
    }
  }
}
