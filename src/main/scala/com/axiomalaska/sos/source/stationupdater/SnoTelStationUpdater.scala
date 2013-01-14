package com.axiomalaska.sos.source.stationupdater

import com.axiomalaska.sos.source.BoundingBox
import com.axiomalaska.sos.source.StationQuery
import com.axiomalaska.sos.source.data.SourceId
import com.axiomalaska.sos.source.data.DatabaseStation
import com.axiomalaska.sos.source.data.DatabaseSensor
import com.axiomalaska.sos.source.data.DatabasePhenomenon
import com.axiomalaska.sos.source.Units
import com.axiomalaska.sos.tools.HttpSender
import com.axiomalaska.sos.tools.HttpPart
import com.axiomalaska.phenomena.Phenomena
import com.axiomalaska.phenomena.Phenomenon
import com.axiomalaska.sos.data.Location
import com.axiomalaska.sos.source.GeoTools
import com.axiomalaska.sos.source.data.LocalPhenomenon
import com.axiomalaska.sos.source.data.ObservedProperty
import org.jsoup.Jsoup
import org.jsoup.select.Elements
import org.apache.log4j.Logger
import java.util.zip.ZipFile
import scala.collection.JavaConversions._
import scala.xml.Node
import scala.collection.mutable
import scala.xml.XML
import javax.measure.Measure
import javax.measure.unit.NonSI
import javax.measure.unit.SI
import com.axiomalaska.sos.source.SourceUrls

case class SnotelSensor(observedpropertylabel: String,
  observedpropertylongcode: String, observedpropertyshortcode: String,
  unit: String, instrument: String, interval: String,
  ordinal: Int, sensorheight: Double)

class SnoTelStationUpdater(private val stationQuery: StationQuery,
  private val boundingBox: BoundingBox, 
  private val logger: Logger = Logger.getRootLogger()) extends StationUpdater {

  // ---------------------------------------------------------------------------
  // Private Data
  // ---------------------------------------------------------------------------

  private val httpSender = new HttpSender()
  private val stationUpdater = new StationUpdateTool(stationQuery, logger)
  private val source = stationQuery.getSource(SourceId.SNOTEL)
  private val foreignIdParser = """.*<a href="http://www\.wcc\.nrcs\.usda\.gov/nwcc/site\?sitenum=(\d+)">Site Info</a>.*""".r
  private val labelParser = """.*<font size="\+2">.*: (.*)</font>.*""".r
  private val geoTools = new GeoTools()
    
  // ---------------------------------------------------------------------------
  // Public Members
  // ---------------------------------------------------------------------------

  def update() {
    val sourceStationSensors = getSourceStations()

    val databaseStations = stationQuery.getAllStations(source)

    stationUpdater.updateStations(sourceStationSensors, databaseStations)
  }

  val name = "SnoTel"
    
  // ---------------------------------------------------------------------------
  // Private Members
  // ---------------------------------------------------------------------------

  private def getSourceStations(): List[(DatabaseStation, List[(DatabaseSensor, List[DatabasePhenomenon])])] = {

    val stations = createStations()
    val size = stations.length - 1
    logger.info("Total number of stations not filtered: " + size)
    val stationSensorsCollection = for {
      (station, index) <- stations.zipWithIndex
      if (withInBoundingBox(station))
      val sourceObservedProperties = getSourceObservedProperties(station)
      val databaseObservedProperties = 
        stationUpdater.updateObservedProperties(source, sourceObservedProperties)
      val sensors = 
        stationUpdater.getSourceSensors(station, databaseObservedProperties)
      if (sensors.nonEmpty)
    } yield {
      logger.debug("[" + index + " of " + size + "] done processing station: " + station.name)
      (station, sensors)
    }
    logger.info("finished with stations")

    return stationSensorsCollection
  }

  private def getSourceObservedProperties(station: DatabaseStation): List[ObservedProperty] = {
    val snotelSensors = createSnotelSensors(station.foreign_tag)
    
    logger.info("Processing station: " + station.name)

    return snotelSensors.flatMap(snotelSensor => getObservedProperty(snotelSensor))
  }

  private def createSnotelSensors(foreignTag: String): List[SnotelSensor] = {
    try {
      val results = httpSender.sendPostMessage(
        SourceUrls.SNOTEL_COLLECTION_SENSOR_INFO_FOR_STATION,
        List[HttpPart](new HttpPart("sitenum", foreignTag)))

      if (results == null) {
        return Nil
      }
      val doc = Jsoup.parse(results)

      val databaseSnotelSensors = for {
        element <- doc.getElementsByTag("a")
        if element.attr("name") == "results"
        trElement <- element.nextElementSibling().getElementsByTag("tr")
        tdElements = trElement.getElementsByTag("td")
        if tdElements.size > 0
      } yield { createSnotelSensor(tdElements) }

      val map = new mutable.HashSet[String]

      val filteredSnotelSensors = for {
        snotelSensor <- databaseSnotelSensors
        val tag = snotelSensor.observedpropertyshortcode + snotelSensor.sensorheight
        if (!map.contains(tag))
      } yield {
        map += tag
        snotelSensor
      }

      return filteredSnotelSensors.toList
    } catch {
      case e: Exception => logger.error("getSnotelSensor: " + e.getMessage())
    }

    return Nil
  }
  
  private def createSnotelSensor(elements: Elements): SnotelSensor = {
    val rawLongCode = elements.get(0).text
    val label = elements.get(1).text
    val unit = elements.get(2).text match {
      case "Degc" => "C"
      case s: String => s
    }
    val instrument = elements.get(3).text
    val shortCode = elements.get(4).text().trim
    val interval = elements.get(5).text
    val ordinal = elements.get(6).text().toInt
    val sensorHeight = elements.get(7).text
    
    val valueInches = sensorHeight match{
      case "unknown" => {
        0.0
      }
      case s:String =>{
        s.replace("\"", "").toDouble
      }
    }

    val valueMeters = Measure.valueOf(valueInches, 
        NonSI.INCH).doubleValue(SI.METER).abs
        
    return SnotelSensor(label, rawLongCode, shortCode, unit,
      instrument, interval, ordinal, valueMeters)
  }

  private def withInBoundingBox(station: DatabaseStation): Boolean = {
    val stationLocation = new Location(station.latitude, station.longitude)
    return geoTools.isStationWithinRegion(stationLocation, boundingBox)
  }

  private def createStation(placemark: Node): DatabaseStation = {
    val lat = (placemark \ "LookAt" \ "latitude").text.toDouble
    val lon = (placemark \ "LookAt" \ "longitude").text.toDouble
    val descriptionText = (placemark \ "description").text.replace("\n", "")
    val foreignIdParser(foreignId) = descriptionText
    val labelParser(label) = descriptionText

    return new DatabaseStation(label, source.tag + ":" + foreignId, foreignId, 
        "", "FIXED MET STATION", source.id, lat, lon)
  }
  
  private def createStations(): List[DatabaseStation] = {
    val filename =
      httpSender.downloadFile(SourceUrls.SNOTEL_COLLECTION_OF_STATIONS)

    if (filename != null) {
      val rootzip = new ZipFile(filename);

      val zipEntryOption =
        rootzip.entries().find(_.getName == "snotelwithoutlabels.kml")

      zipEntryOption match {
        case Some(zipEntry) => {
          val snotelKmlRootElem = XML.load(rootzip.getInputStream(zipEntry))
          (snotelKmlRootElem \\ "Placemark").map(createStation).toList
        }
        case None => {
          logger.error("snotelwithoutlabels.kml file not found")
          Nil
        }
      }
    } else {
      logger.error("snotelwithoutlabels.kml could not be downloaded")
      Nil
    }
  }
  
  private def getObservedProperty(snotelSensor: SnotelSensor): Option[ObservedProperty] ={
    snotelSensor.observedpropertyshortcode match {
      case "WTEQ" => {
        getObservedProperty(Phenomena.instance.SNOW_WATER_EQUIVALENT, snotelSensor.observedpropertyshortcode)
      }
      case "SAL" => {
        getObservedProperty(Phenomena.instance.SALINITY, snotelSensor.observedpropertyshortcode)
      }
      case "TMAX" => {
        getObservedProperty(Phenomena.instance.AIR_TEMPERATURE_MAXIMUM, snotelSensor.observedpropertyshortcode)
      }
      case "TOBS" => {
        getObservedProperty(Phenomena.instance.AIR_TEMPERATURE, snotelSensor.observedpropertyshortcode)
      }
      case "PRCP" => {
        getObservedProperty(Phenomena.instance.PRECIPITATION_INCREMENT, snotelSensor.observedpropertyshortcode)
      }
      case "SMS" => {
        getObservedProperty(Phenomena.instance.SOIL_MOISTURE_PERCENT, snotelSensor.observedpropertyshortcode)
      }
      case "RHUM" => {
        getObservedProperty(Phenomena.instance.RELATIVE_HUMIDITY, snotelSensor.observedpropertyshortcode)
      }
      case "RHUMN" => {
        getObservedProperty(Phenomena.instance.RELATIVE_HUMIDITY_MINIMUM, snotelSensor.observedpropertyshortcode)
      }
      case "RHUMV" => {
        getObservedProperty(Phenomena.instance.RELATIVE_HUMIDITY_AVERAGE, snotelSensor.observedpropertyshortcode)
      }
      case "RHUMX" => {
        getObservedProperty(Phenomena.instance.RELATIVE_HUMIDITY_MAXIMUM, snotelSensor.observedpropertyshortcode)
      }
      case "RDC" => {
        getObservedProperty(Phenomena.instance.createHomelessParameter("real_dielectiric_constant", snotelSensor.unit), snotelSensor.observedpropertyshortcode)
      }
      case "PREC" => {
        getObservedProperty(Phenomena.instance.PRECIPITATION_ACCUMULATED, snotelSensor.observedpropertyshortcode)
      }
      case "STO" => {
        getObservedProperty(Phenomena.instance.createHomelessParameter("ground_temperature_observed", snotelSensor.unit), snotelSensor.observedpropertyshortcode)
      }
      case "TAVG" => {
        getObservedProperty(Phenomena.instance.AIR_TEMPERATURE_AVERAGE, snotelSensor.observedpropertyshortcode)
      }
      case "BATT" => {
        getObservedProperty(Phenomena.instance.BATTERY_VOLTAGE, snotelSensor.observedpropertyshortcode)
      }
      case "BATX" => {
        getObservedProperty(Phenomena.instance.BATTERY_VOLTAGE_MAXIMUM, snotelSensor.observedpropertyshortcode)
      }
      case "BATN" => {
        getObservedProperty(Phenomena.instance.BATTERY_VOLTAGE_MINIMUM, snotelSensor.observedpropertyshortcode)
      }
      case "WSPDX" => {
        getObservedProperty(Phenomena.instance.WIND_SPEED_OF_GUST, snotelSensor.observedpropertyshortcode)
      }
      case "WSPDV" => {
        getObservedProperty(Phenomena.instance.WIND_SPEED, snotelSensor.observedpropertyshortcode)
      }
      case "SNWD" => {
        getObservedProperty(Phenomena.instance.SNOW_DEPTH, snotelSensor.observedpropertyshortcode)
      }
      case "TMIN" => {
        getObservedProperty(Phenomena.instance.AIR_TEMPERATURE_MINIMUM, snotelSensor.observedpropertyshortcode)
      }
      case "PRES" => {
        getObservedProperty(Phenomena.instance.AIR_PRESSURE, snotelSensor.observedpropertyshortcode)
      }
      case "WDIRV" => {
        getObservedProperty(Phenomena.instance.WIND_FROM_DIRECTION, snotelSensor.observedpropertyshortcode)
      }
      case "SRADV" => {
        getObservedProperty(Phenomena.instance.SOLAR_RADIATION_AVERAGE, snotelSensor.observedpropertyshortcode)
      }
      case "SRADN" => {
        getObservedProperty(Phenomena.instance.SOLAR_RADIATION_MINIMUM, snotelSensor.observedpropertyshortcode)
      }
      case "SRADX" => {
        getObservedProperty(Phenomena.instance.SOLAR_RADIATION_MAXIMUM, snotelSensor.observedpropertyshortcode)
      }
      case "SRAD" => {
        getObservedProperty(Phenomena.instance.SOLAR_RADIATION, snotelSensor.observedpropertyshortcode)
      }
      case "COND" => {
        getObservedProperty(Phenomena.instance.createHomelessParameter("conductivity", Units.MICRO_MHOS_PER_CENTIMETERS), snotelSensor.observedpropertyshortcode)
      }
      case "WTEMP" => {
        getObservedProperty(Phenomena.instance.SEA_WATER_TEMPERATURE, snotelSensor.observedpropertyshortcode)
      }
      case "SRMO" => {
        getObservedProperty(Phenomena.instance.createHomelessParameter("stream_gage_height", snotelSensor.unit), snotelSensor.observedpropertyshortcode)
      }
      case "SNOW" => None  // Snow Fall
      case "LRADT" => None // Solar Radiation/langley Total
      case "DIAG" => None
      case "RHENC" => None // Relative Humidity Enclosure
      case _ => {
        logger.debug("[" + source.name + "] observed property: " + snotelSensor.observedpropertylabel +
          " == " + snotelSensor.observedpropertylongcode + " is not processed correctly.")
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