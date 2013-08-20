package com.axiomalaska.sos.harvester.source.stationupdater

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
import com.axiomalaska.sos.harvester.data.SourceId;
import com.axiomalaska.sos.harvester.data.LocalPhenomenon
import com.axiomalaska.ioos.sos.GeomHelper
import org.jsoup.Jsoup
import org.jsoup.select.Elements
import org.apache.log4j.Logger
import com.axiomalaska.sos.tools.HttpPart
import com.axiomalaska.sos.tools.HttpSender
import java.util.zip.ZipFile
import scala.collection.JavaConversions._
import scala.xml.Node
import scala.collection.mutable
import scala.xml.XML
import javax.measure.Measure
import javax.measure.unit.NonSI
import javax.measure.unit.SI
import com.axiomalaska.phenomena.Phenomena
import com.axiomalaska.sos.harvester.data.PhenomenaFactory

class SnoTelStationUpdater(private val stationQuery: StationQuery,
  private val boundingBox: BoundingBox) extends StationUpdater {

private case class SnotelSensor(observedpropertylabel: String,
  observedpropertylongcode: String, observedpropertyshortcode: String,
  unit: String, instrument: String, interval: String,
  ordinal: Int, sensorDepth: Double)
  
  // ---------------------------------------------------------------------------
  // Private Data
  // ---------------------------------------------------------------------------
  private val LOGGER = Logger.getLogger(getClass())
  private val httpSender = new HttpSender()
  private val stationUpdater = new StationUpdateTool(stationQuery)
  private val source = stationQuery.getSource(SourceId.SNOTEL)
  private val foreignIdParser = """.*<a href="http://www\.wcc\.nrcs\.usda\.gov/nwcc/site\?sitenum=(\d+)">Site Info</a>.*""".r
  private val labelParser = """.*<font size="\+2">.*: (.*)</font>.*""".r
  private val geoTools = new GeoTools()
  private val phenomenaFactory = new PhenomenaFactory()
    
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

  private def getSourceStations(): List[(DatabaseStation, 
      List[(DatabaseSensor, List[DatabasePhenomenon])])] = {

    val stations = createStations()
    val size = stations.length - 1
    LOGGER.info("Total number of stations not filtered: " + size)
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
      LOGGER.debug("[" + index + " of " + size + "] done processing station: " + station.name)
      (station, sensors)
    }
    LOGGER.info("finished with stations")

    return stationSensorsCollection
  }

  private def getSourceObservedProperties(station: DatabaseStation): List[ObservedProperty] = {
    val snotelSensors = createSnotelSensors(station.foreign_tag)
    
    LOGGER.info("Processing station: " + station.name)

    return snotelSensors.flatMap(getObservedProperty)
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
        val tag = snotelSensor.observedpropertyshortcode + snotelSensor.sensorDepth
        if (!map.contains(tag))
      } yield {
        map += tag
        snotelSensor
      }

      return filteredSnotelSensors.toList
    } catch {
      case e: Exception => LOGGER.error("getSnotelSensor: " + e.getMessage())
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
    val stationLocation = GeomHelper.createLatLngPoint(station.latitude, station.longitude)
    return geoTools.isStationWithinRegion(stationLocation, boundingBox)
  }

  private def createStation(placemark: Node): DatabaseStation = {
    val lat = (placemark \ "LookAt" \ "latitude").text.toDouble
    val lon = (placemark \ "LookAt" \ "longitude").text.toDouble
    val descriptionText = (placemark \ "description").text.replace("\n", "")
    val foreignIdParser(foreignId) = descriptionText
    val labelParser(label) = descriptionText

    return new DatabaseStation(label, source.tag + ":" + foreignId, foreignId, 
        "", "FIXED MET STATION", source.id, lat, lon, null, null)
  }
  
  private def createStations(): List[DatabaseStation] = {
    val filename =
      HttpSender.downloadFile(SourceUrls.SNOTEL_COLLECTION_OF_STATIONS)

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
          LOGGER.error("snotelwithoutlabels.kml file not found")
          Nil
        }
      }
    } else {
      LOGGER.error("snotelwithoutlabels.kml could not be downloaded")
      Nil
    }
  }
  
  private def getObservedProperty(snotelSensor: SnotelSensor): Option[ObservedProperty] ={
    snotelSensor.observedpropertyshortcode match {
      case "WTEQ" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.SNOW_WATER_EQUIVALENT,
          snotelSensor.observedpropertyshortcode, snotelSensor.unit, 
          snotelSensor.sensorDepth, source))
      }
      case "SAL" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.SALINITY,
          snotelSensor.observedpropertyshortcode, snotelSensor.unit, 
          snotelSensor.sensorDepth, source))
      }
      case "TMAX" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.AIR_TEMPERATURE_MAXIMUM,
          snotelSensor.observedpropertyshortcode, snotelSensor.unit, 
          snotelSensor.sensorDepth, source))
      }
      case "TOBS" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.AIR_TEMPERATURE,
          snotelSensor.observedpropertyshortcode, snotelSensor.unit, 
          snotelSensor.sensorDepth, source))
      }
      case "PRCP" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.PRECIPITATION_INCREMENT,
          snotelSensor.observedpropertyshortcode, snotelSensor.unit, 
          snotelSensor.sensorDepth, source))
      }
      case "SMS" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.SOIL_MOISTURE_PERCENT,
          snotelSensor.observedpropertyshortcode, snotelSensor.unit, 
          snotelSensor.sensorDepth, source))
      }
      case "RHUM" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.RELATIVE_HUMIDITY,
          snotelSensor.observedpropertyshortcode, snotelSensor.unit, 
          snotelSensor.sensorDepth, source))
      }
      case "RHUMN" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.RELATIVE_HUMIDITY_MINIMUM,
          snotelSensor.observedpropertyshortcode, snotelSensor.unit, 
          snotelSensor.sensorDepth, source))
      }
      case "RHUMV" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.RELATIVE_HUMIDITY_AVERAGE,
          snotelSensor.observedpropertyshortcode, snotelSensor.unit, 
          snotelSensor.sensorDepth, source))
      }
      case "RHUMX" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.RELATIVE_HUMIDITY_MAXIMUM,
          snotelSensor.observedpropertyshortcode, snotelSensor.unit, 
          snotelSensor.sensorDepth, source))
      }
      case "RDC" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.RELATIVE_PERMITTIVITY,
          snotelSensor.observedpropertyshortcode, snotelSensor.unit, 
          snotelSensor.sensorDepth, source))
      }
      case "PREC" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.PRECIPITATION_ACCUMULATED,
          snotelSensor.observedpropertyshortcode, snotelSensor.unit, 
          snotelSensor.sensorDepth, source))
      }
      case "STO" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.SOIL_TEMPERATURE,
          snotelSensor.observedpropertyshortcode, snotelSensor.unit, 
          snotelSensor.sensorDepth, source))
      }
      case "TAVG" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.AIR_TEMPERATURE_AVERAGE,
          snotelSensor.observedpropertyshortcode, snotelSensor.unit, 
          snotelSensor.sensorDepth, source))
      }
      case "BATT" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.BATTERY_VOLTAGE,
          snotelSensor.observedpropertyshortcode, snotelSensor.unit, 
          snotelSensor.sensorDepth, source))
      }
      case "BATX" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.BATTERY_VOLTAGE_MAXIMUM,
          snotelSensor.observedpropertyshortcode, snotelSensor.unit, 
          snotelSensor.sensorDepth, source))
      }
      case "BATN" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.BATTERY_VOLTAGE_MINIMUM,
          snotelSensor.observedpropertyshortcode, snotelSensor.unit, 
          snotelSensor.sensorDepth, source))
      }
      case "WSPDX" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.WIND_SPEED_OF_GUST,
          snotelSensor.observedpropertyshortcode, snotelSensor.unit, 
          snotelSensor.sensorDepth, source))
      }
      case "WSPDV" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.WIND_SPEED,
          snotelSensor.observedpropertyshortcode, snotelSensor.unit, 
          snotelSensor.sensorDepth, source))
      }
      case "SNWD" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.SNOW_DEPTH,
          snotelSensor.observedpropertyshortcode, snotelSensor.unit, 
          snotelSensor.sensorDepth, source))
      }
      case "TMIN" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.AIR_TEMPERATURE_MINIMUM,
          snotelSensor.observedpropertyshortcode, snotelSensor.unit, 
          snotelSensor.sensorDepth, source))
      }
      case "PRES" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.AIR_PRESSURE,
          snotelSensor.observedpropertyshortcode, snotelSensor.unit, 
          snotelSensor.sensorDepth, source))
      }
      case "WDIRV" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.WIND_FROM_DIRECTION,
          snotelSensor.observedpropertyshortcode, snotelSensor.unit, 
          snotelSensor.sensorDepth, source))
      }
      case "SRADV" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.SOLAR_RADIATION_AVERAGE,
          snotelSensor.observedpropertyshortcode, snotelSensor.unit, 
          snotelSensor.sensorDepth, source))
      }
      case "SRADN" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.SOLAR_RADIATION_MINIMUM,
          snotelSensor.observedpropertyshortcode, snotelSensor.unit, 
          snotelSensor.sensorDepth, source))
      }
      case "SRADX" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.SOLAR_RADIATION_MAXIMUM,
          snotelSensor.observedpropertyshortcode, snotelSensor.unit, 
          snotelSensor.sensorDepth, source))
      }
      case "SRAD" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.SOLAR_RADIATION,
          snotelSensor.observedpropertyshortcode, snotelSensor.unit, 
          snotelSensor.sensorDepth, source))
      }
      case "COND" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.SEA_WATER_ELECTRICAL_CONDUCTIVITY,
          snotelSensor.observedpropertyshortcode, snotelSensor.unit, 
          snotelSensor.sensorDepth, source))
      }
      case "WTEMP" => {
        Some(stationUpdater.getObservedProperty(Phenomena.instance.SEA_WATER_TEMPERATURE,
          snotelSensor.observedpropertyshortcode, snotelSensor.unit, 
          snotelSensor.sensorDepth, source))
      }
      case "SRMO" => {
        val url = Phenomena.GENERIC_FAKE_MMI_URL_PREFIX + "stream_gage_height"
        Some(stationUpdater.getObservedProperty(phenomenaFactory.findCustomPhenomenon(url),
          snotelSensor.observedpropertyshortcode, snotelSensor.unit, 
          snotelSensor.sensorDepth, source))
      }
      case "SNOW" => None  // Snow Fall
      case "LRADT" => None // Solar Radiation/langley Total
      case "DIAG" => None
      case "RHENC" => None // Relative Humidity Enclosure
      case _ => {
        LOGGER.debug("[" + source.name + "] observed property: " + 
            snotelSensor.observedpropertylabel +
          " == " + snotelSensor.observedpropertylongcode + 
          " is not processed correctly.")
        return None
      }
    }
  }
}
