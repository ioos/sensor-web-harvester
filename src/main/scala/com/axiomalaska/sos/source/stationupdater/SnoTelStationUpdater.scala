package com.axiomalaska.sos.source.stationupdater

import com.axiomalaska.sos.source.BoundingBox
import com.axiomalaska.sos.source.StationQuery
import com.axiomalaska.sos.source.data.SourceId
import com.axiomalaska.sos.source.data.DatabaseStation
import com.axiomalaska.sos.source.data.DatabaseSensor
import com.axiomalaska.sos.source.data.DatabasePhenomenon
import com.axiomalaska.sos.source.data.SensorPhenomenonIds
import com.axiomalaska.sos.source.Units

import java.util.zip.ZipFile

import com.axiomalaska.sos.tools.HttpSender
import com.axiomalaska.sos.tools.HttpPart
import com.axiomalaska.sos.data.Location
import com.axiomalaska.sos.source.GeoTools
import com.axiomalaska.sos.source.data.ObservedProperty

import org.jsoup.Jsoup
import org.jsoup.select.Elements

import org.apache.log4j.Logger

import scala.collection.JavaConversions._
import scala.xml.Node
import scala.xml.Elem

case class SnotelSensor(observedpropertylabel: String,
  observedpropertylongcode: String, observedpropertyshortcode: String,
  unit: String, instrument: String, interval: String,
  ordinal: Int, sensorheight: String)

class SnoTelStationUpdater(private val stationQuery: StationQuery,
  private val boundingBoxOption: Option[BoundingBox]) extends StationUpdater {

  // ---------------------------------------------------------------------------
  // Private Data
  // ---------------------------------------------------------------------------

  private val httpSender = new HttpSender()
  private val stationUpdater = new StationUpdateTool(stationQuery)
  private val log = Logger.getRootLogger()
  private val source = stationQuery.getSource(SourceId.SNOTEL)
  private val foreignIdParser = """.*<a href="http://www\.wcc\.nrcs\.usda\.gov/nwcc/site\?sitenum=(\d+)">Site Info</a>.*""".r
  private val labelParser = """.*<font size="\+2">.*: (.*)</font>.*""".r
  private val geoTools = new GeoTools()

  // ---------------------------------------------------------------------------
  // Public Members
  // ---------------------------------------------------------------------------

  def update() {
    val sourceStationSensors = getSourceStations()

    val databaseStations = stationQuery.getStations(source)

    stationUpdater.updateStations(sourceStationSensors, databaseStations)
  }

  // ---------------------------------------------------------------------------
  // Private Members
  // ---------------------------------------------------------------------------

  private def getSourceStations(): List[(DatabaseStation, List[(DatabaseSensor, List[DatabasePhenomenon])])] = {

    val placemarks = (snotelKmlRootElem \\ "Placemark").toList
    val size = placemarks.length - 1
    val stationSensorsCollection = for {
      (placemark, index) <- placemarks.zipWithIndex
      val station = createStation(placemark)
      if (withInBoundingBox(station))
      val sourceObservedProperties = getSourceObservedProperties(station)
      val databaseObservedProperties = stationUpdater.updateObservedProperties(source, sourceObservedProperties)
      val sensors = stationUpdater.getSourceSensors(station, databaseObservedProperties)
      if (sensors.nonEmpty)
    } yield {
      log.info("[" + index + " of " + size + "] station: " + station.name)
      (station, sensors)
    }
    log.info("finished with stations")

    return stationSensorsCollection
  }

  private def getSourceObservedProperties(station: DatabaseStation): List[ObservedProperty] = {
    val snotelSensors = createSnotelSensors(station)

    return snotelSensors.flatMap(
      snotelSensor => getObservedProperty(snotelSensor))
  }

  private def createSnotelSensors(station: DatabaseStation): List[SnotelSensor] = {
    try {
      val results = httpSender.sendPostMessage(
        "http://www.wcc.nrcs.usda.gov/nwcc/sensors",
        List[HttpPart](new HttpPart("sitenum", station.foreign_tag)))

      val doc = Jsoup.parse(results)

      val databaseSnotelSensors = for {
        element <- doc.getElementsByTag("a")
        if element.attr("name") == "results"
        trElement <- element.nextElementSibling().getElementsByTag("tr")
        tdElements = trElement.getElementsByTag("td")
        if tdElements.size > 0
      } yield { createSnotelSensor(tdElements) }

      return databaseSnotelSensors.toList
    } catch {
      case e: Exception => log.error("getSnotelSensor: " + e.getMessage())
    }

    return Nil
  }

  private def getObservedProperty(snotelSensor: SnotelSensor): Option[ObservedProperty] = {
    snotelSensor.observedpropertylabel match {
      case "Diagnostics" => None
      case "Stream Stage (gauge Height) Observed" => None
      case "Snow Water Equivalent" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(snotelSensor.observedpropertylongcode,
            source, snotelSensor.unit,
            SensorPhenomenonIds.SNOW_WATER_EQUIVALENT))
      }
      case "Precipitation Accumulation" => {
        snotelSensor.observedpropertylongcode match {
          case "PRECI1" => {
            return new Some[ObservedProperty](
              stationUpdater.createObservedProperty(snotelSensor.observedpropertylongcode,
                source, snotelSensor.unit,
                SensorPhenomenonIds.PRECIPITATION_ACCUMULATION))
          }
          case "PRECI2" => return None // Tipping Bucket Usvi
        }
      }
      case "Precipitation Increment" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(snotelSensor.observedpropertylongcode,
            source, snotelSensor.unit,
            SensorPhenomenonIds.PRECIPITATION_INCREMENT))
      }
      case "Air Temperature Observed" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(snotelSensor.observedpropertylongcode,
            source, snotelSensor.unit,
            SensorPhenomenonIds.AIR_TEMPERATURE))
      }
      case "Air Temperature Maximum" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(snotelSensor.observedpropertylongcode,
            source, snotelSensor.unit,
            SensorPhenomenonIds.AIR_TEMPERATURE_MAXIMUM))
      }
      case "Air Temperature Minimum" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(snotelSensor.observedpropertylongcode,
            source, snotelSensor.unit,
            SensorPhenomenonIds.AIR_TEMPERATURE_MINIMUM))
      }
      case "Air Temperature Average" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(snotelSensor.observedpropertylongcode,
            source, snotelSensor.unit,
            SensorPhenomenonIds.AIR_TEMPERATURE_AVERAGE))
      }
      case "Snow Depth" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(snotelSensor.observedpropertylongcode,
            source, snotelSensor.unit,
            SensorPhenomenonIds.SNOW_DEPTH))
      }
      case "Soil Moisture Percent" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(snotelSensor.observedpropertylongcode,
            source, Units.PERCENT, SensorPhenomenonIds.SOIL_MOISTURE_PERCENT))
      }
      case "Soil Temperature Observed" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(snotelSensor.observedpropertylongcode,
            source, snotelSensor.unit,
            SensorPhenomenonIds.GROUND_TEMPERATURE_OBSERVED))
      }
      case "Battery" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(snotelSensor.observedpropertylongcode,
            source, Units.VOLTAGE, SensorPhenomenonIds.BATTERY))
      }
      case "Battery Maximum" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(snotelSensor.observedpropertylongcode,
            source, Units.VOLTAGE, SensorPhenomenonIds.BATTERY_MAXIMUM))
      }
      case "Battery Minimum" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(snotelSensor.observedpropertylongcode,
            source, Units.VOLTAGE,
            SensorPhenomenonIds.BATTERY_MINIMUM))
      }
      case "Solar Radiation/langley Total" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(snotelSensor.observedpropertylongcode,
            source, Units.WATT_PER_METER_SQUARED,
            SensorPhenomenonIds.SOLAR_RADIATION))
      }
      case "Solar Radiation" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(snotelSensor.observedpropertylongcode,
            source, Units.WATT_PER_METER_SQUARED,
            SensorPhenomenonIds.SOLAR_RADIATION))
      }
      case "Solar Radiation Average" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(snotelSensor.observedpropertylongcode,
            source, Units.WATT_PER_METER_SQUARED,
            SensorPhenomenonIds.SOLAR_RADIATION_AVERAGE))
      }
      case "Solar Radiation Maximum" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(snotelSensor.observedpropertylongcode,
            source, Units.WATT_PER_METER_SQUARED,
            SensorPhenomenonIds.SOLAR_RADIATION_MAXIMUM))
      }
      case "Solar Radiation Minimum" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(snotelSensor.observedpropertylongcode,
            source, Units.WATT_PER_METER_SQUARED,
            SensorPhenomenonIds.SOLAR_RADIATION_MINIMUM))
      }
      case "Relative Humidity Enclosure" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(snotelSensor.observedpropertylongcode,
            source, snotelSensor.unit,
            SensorPhenomenonIds.RELATIVE_HUMIDITY))
      }
      case "Relative Humidity" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(snotelSensor.observedpropertylongcode,
            source, Units.PERCENT,
            SensorPhenomenonIds.RELATIVE_HUMIDITY))
      }
      case "Relative Humidity Minimum" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(snotelSensor.observedpropertylongcode,
            source, Units.PERCENT,
            SensorPhenomenonIds.RELATIVE_HUMIDITY_MINIMUM))
      }
      case "Relative Humidity Average" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(snotelSensor.observedpropertylongcode,
            source, snotelSensor.unit,
            SensorPhenomenonIds.RELATIVE_HUMIDITY_AVERAGE))
      }
      case "Relative Humidity Maximum" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(snotelSensor.observedpropertylongcode,
            source, Units.PERCENT,
            SensorPhenomenonIds.RELATIVE_HUMIDITY_MAXIMUM))
      }
      case "Wind Direction Average" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(snotelSensor.observedpropertylongcode,
            source, snotelSensor.unit,
            SensorPhenomenonIds.WIND_DIRECTION))
      }
      case "Wind Speed Maximum" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(snotelSensor.observedpropertylongcode,
            source, snotelSensor.unit,
            SensorPhenomenonIds.WIND_GUST))
      }
      case "Wind Speed Average" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(snotelSensor.observedpropertylongcode,
            source, snotelSensor.unit,
            SensorPhenomenonIds.WIND_SPEED))
      }
      case "Barometric Pressure" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(snotelSensor.observedpropertylongcode,
            source, snotelSensor.unit,
            SensorPhenomenonIds.BAROMETRIC_PRESSURE))
      }
      case "Water Temperature" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(snotelSensor.observedpropertylongcode,
            source, snotelSensor.unit,
            SensorPhenomenonIds.SEA_WATER_TEMPERATURE))
      }
      case "Conductivity" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(snotelSensor.observedpropertylongcode,
            source, Units.MICRO_MHOS_PER_CENTIMETERS,
            SensorPhenomenonIds.CONDUCTIVITY))
      }
      case "Salinity" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(snotelSensor.observedpropertylongcode,
            source, snotelSensor.unit,
            SensorPhenomenonIds.SALINITY))
      }
      case "Real Dielectric Constant" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(snotelSensor.observedpropertylongcode,
            source, snotelSensor.unit,
            SensorPhenomenonIds.REAL_DIELECTRIC_CONSTANT))
      }
      case _ => {
        log.error("[" + source.name + "] observed propery: " + snotelSensor.observedpropertylabel +
          " is not processed correctly.")
        return None
      }
    }
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

    val longCode = rawLongCode.map(c => c match {
      case '.' => ""
      case '-' => ""
      case ':' => ""
      case ' ' => ""
      case c: Char => c
    }).mkString

    return new SnotelSensor(label, longCode, shortCode, unit,
      instrument, interval, ordinal, sensorHeight)
  }

  private def withInBoundingBox(station: DatabaseStation): Boolean = {
    boundingBoxOption match {
      case Some(boundingBox) => {
        val stationLocation = new Location(station.latitude, station.longitude)
        return geoTools.isStationWithinRegion(stationLocation, boundingBox)
      }
      case None => true
    }
  }

  private def createStation(placemark: Node): DatabaseStation = {
    val lat = (placemark \ "LookAt" \ "latitude").text.toDouble
    val lon = (placemark \ "LookAt" \ "longitude").text.toDouble
    val descriptionText = (placemark \ "description").text.replace("\n", "")
    val foreignIdParser(foreignId) = descriptionText
    val labelParser(label) = descriptionText

    return new DatabaseStation(label, foreignId, foreignId, source.id, lat, lon)
  }

  private def snotelKmlRootElem(): Elem = {
    val filename =
      httpSender.downloadFile(
        "http://www.wcc.nrcs.usda.gov/ftpref/data/water/wcs/earth/snotelwithoutlabels.kmz")

    val rootzip = new ZipFile(filename);

    val zipEntryOption = rootzip.entries().find(_.getName == "snotelwithoutlabels.kml")

    zipEntryOption match {
      case Some(zipEntry) => {
        val x = scala.xml.XML.load(rootzip.getInputStream(zipEntry))
        return x
      }
      case None => throw new Exception("snotelwithoutlabels.kml file not found")
    }
  }
}