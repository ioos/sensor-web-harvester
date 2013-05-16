package com.axiomalaska.sos.source.stationupdater

import com.axiomalaska.phenomena.Phenomenon
import com.axiomalaska.sos.data.Location
import com.axiomalaska.sos.source.BoundingBox
import com.axiomalaska.sos.source.StationQuery
import com.axiomalaska.sos.source.data.SensorPhenomenonIds
import com.axiomalaska.sos.source.data.SourceId
import com.axiomalaska.sos.source.data.DatabaseStation
import com.axiomalaska.sos.source.data.DatabaseSensor
import com.axiomalaska.sos.source.Units
import com.axiomalaska.sos.source.data.DatabasePhenomenon
import com.axiomalaska.sos.source.GeoTools
import com.axiomalaska.sos.source.data.LocalPhenomenon
import com.axiomalaska.sos.source.data.ObservedProperty
import com.axiomalaska.sos.source.SourceUrls
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
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(snotelSensor.observedpropertyshortcode,
            source, snotelSensor.unit,
            SensorPhenomenonIds.SNOW_WATER_EQUIVALENT, snotelSensor.sensorheight * (-1)))
      }
      case "SAL" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(snotelSensor.observedpropertyshortcode,
            source, snotelSensor.unit,
            SensorPhenomenonIds.SALINITY, snotelSensor.sensorheight * (-1)))
      }
      case "TMAX" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(snotelSensor.observedpropertyshortcode,
            source, snotelSensor.unit,
            SensorPhenomenonIds.AIR_TEMPERATURE_MAXIMUM, snotelSensor.sensorheight * (-1)))
      }
      case "TOBS" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(snotelSensor.observedpropertyshortcode,
            source, snotelSensor.unit,
            SensorPhenomenonIds.AIR_TEMPERATURE, snotelSensor.sensorheight * (-1)))
      }
      case "PRCP" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(snotelSensor.observedpropertyshortcode,
            source, snotelSensor.unit,
            SensorPhenomenonIds.PRECIPITATION_INCREMENT, snotelSensor.sensorheight * (-1)))
      }
      case "SMS" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(snotelSensor.observedpropertyshortcode,
            source, snotelSensor.unit,
            SensorPhenomenonIds.SOIL_MOISTURE_PERCENT, snotelSensor.sensorheight * (-1)))
      }
      case "RHUM" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(snotelSensor.observedpropertyshortcode,
            source, snotelSensor.unit,
            SensorPhenomenonIds.RELATIVE_HUMIDITY, snotelSensor.sensorheight * (-1)))
      }
      case "RHUMN" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(snotelSensor.observedpropertyshortcode,
            source, snotelSensor.unit,
            SensorPhenomenonIds.RELATIVE_HUMIDITY_MINIMUM, snotelSensor.sensorheight * (-1)))
      }
      case "RHUMV" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(snotelSensor.observedpropertyshortcode,
            source, snotelSensor.unit,
            SensorPhenomenonIds.RELATIVE_HUMIDITY_AVERAGE, snotelSensor.sensorheight * (-1)))
      }
      case "RHUMX" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(snotelSensor.observedpropertyshortcode,
            source, snotelSensor.unit,
            SensorPhenomenonIds.RELATIVE_HUMIDITY_MAXIMUM, snotelSensor.sensorheight * (-1)))
      }
      case "RDC" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(snotelSensor.observedpropertyshortcode,
            source, snotelSensor.unit,
            SensorPhenomenonIds.REAL_DIELECTRIC_CONSTANT, snotelSensor.sensorheight * (-1)))
      }
      case "PREC" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(snotelSensor.observedpropertyshortcode,
            source, snotelSensor.unit,
            SensorPhenomenonIds.PRECIPITATION_ACCUMULATION, snotelSensor.sensorheight * (-1)))
      }
      case "STO" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(snotelSensor.observedpropertyshortcode,
            source, snotelSensor.unit,
            SensorPhenomenonIds.GROUND_TEMPERATURE_OBSERVED, snotelSensor.sensorheight * (-1)))
      }
      case "TAVG" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(snotelSensor.observedpropertyshortcode,
            source, snotelSensor.unit,
            SensorPhenomenonIds.AIR_TEMPERATURE_AVERAGE, snotelSensor.sensorheight * (-1)))
      }
      case "BATT" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(snotelSensor.observedpropertyshortcode,
            source, snotelSensor.unit,
            SensorPhenomenonIds.BATTERY, snotelSensor.sensorheight * (-1)))
      }
      case "BATX" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(snotelSensor.observedpropertyshortcode,
            source, "V", SensorPhenomenonIds.BATTERY_MAXIMUM, snotelSensor.sensorheight * (-1)))
      }
      case "BATN" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(snotelSensor.observedpropertyshortcode,
            source, snotelSensor.unit,
            SensorPhenomenonIds.BATTERY_MINIMUM, snotelSensor.sensorheight * (-1)))
      }
      case "WSPDX" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(snotelSensor.observedpropertyshortcode,
            source, snotelSensor.unit,
            SensorPhenomenonIds.WIND_GUST, snotelSensor.sensorheight * (-1)))
      }
      case "WSPDV" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(snotelSensor.observedpropertyshortcode,
            source, snotelSensor.unit,
            SensorPhenomenonIds.WIND_SPEED, snotelSensor.sensorheight * (-1)))
      }
      case "SNWD" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(snotelSensor.observedpropertyshortcode,
            source, snotelSensor.unit,
            SensorPhenomenonIds.SNOW_DEPTH, snotelSensor.sensorheight * (-1)))
      }
      case "TMIN" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(snotelSensor.observedpropertyshortcode,
            source, snotelSensor.unit,
            SensorPhenomenonIds.AIR_TEMPERATURE_MINIMUM, snotelSensor.sensorheight * (-1)))
      }
      case "PRES" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(snotelSensor.observedpropertyshortcode,
            source, snotelSensor.unit,
            SensorPhenomenonIds.BAROMETRIC_PRESSURE, snotelSensor.sensorheight * (-1)))
      }
      case "WDIRV" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(snotelSensor.observedpropertyshortcode,
            source, snotelSensor.unit,
            SensorPhenomenonIds.WIND_DIRECTION, snotelSensor.sensorheight * (-1)))
      }
      case "SRADV" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(snotelSensor.observedpropertyshortcode,
            source, snotelSensor.unit,
            SensorPhenomenonIds.SOLAR_RADIATION_AVERAGE, snotelSensor.sensorheight * (-1)))
      }
      case "SRADN" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(snotelSensor.observedpropertyshortcode,
            source, snotelSensor.unit,
            SensorPhenomenonIds.SOLAR_RADIATION_MINIMUM, snotelSensor.sensorheight * (-1)))
      }
      case "SRADX" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(snotelSensor.observedpropertyshortcode,
            source, snotelSensor.unit,
            SensorPhenomenonIds.SOLAR_RADIATION_MAXIMUM, snotelSensor.sensorheight * (-1)))
      }
      case "SRAD" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(snotelSensor.observedpropertyshortcode,
            source, snotelSensor.unit,
            SensorPhenomenonIds.SOLAR_RADIATION, snotelSensor.sensorheight * (-1)))
      }
      case "COND" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(snotelSensor.observedpropertyshortcode,
            source, Units.MICRO_MHOS_PER_CENTIMETERS, 
            SensorPhenomenonIds.CONDUCTIVITY, snotelSensor.sensorheight * (-1)))
      }
      case "WTEMP" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(snotelSensor.observedpropertyshortcode,
            source, snotelSensor.unit, 
            SensorPhenomenonIds.SEA_WATER_TEMPERATURE, snotelSensor.sensorheight * (-1)))
      }
      case "SRMO" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(snotelSensor.observedpropertyshortcode,
            source, snotelSensor.unit, 
            SensorPhenomenonIds.STREAM_GAGE_HEIGHT, snotelSensor.sensorheight * (-1)))
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
    stationQuery.createPhenomenon(dbPhenom)
  }
}