package com.axiomalaska.sos.harvester.source.stationupdater

import org.apache.log4j.Logger
import com.axiomalaska.phenomena.Phenomena
import com.axiomalaska.phenomena.Phenomenon
import com.axiomalaska.sos.harvester.BoundingBox;
import com.axiomalaska.sos.harvester.GeoTools;
import com.axiomalaska.sos.harvester.StationQuery;
import com.axiomalaska.sos.harvester.data.DatabasePhenomenon;
import com.axiomalaska.sos.harvester.data.DatabaseSensor;
import com.axiomalaska.sos.harvester.data.DatabaseStation;
import com.axiomalaska.sos.harvester.data.ObservedProperty;
import com.axiomalaska.sos.harvester.data.Source;
import com.axiomalaska.sos.harvester.source.observationretriever.SosRawDataRetriever;
import com.axiomalaska.sos.harvester.data.LocalPhenomenon
import net.opengis.sos.x10.ObservationOfferingType
import net.opengis.sos.x10.CapabilitiesDocument
import net.opengis.sos.x10.GetCapabilitiesDocument
import com.axiomalaska.sos.tools.HttpSender
import scala.xml.Node
import org.joda.time.format.ISODateTimeFormat
import java.sql.Timestamp
import com.axiomalaska.ioos.sos.GeomHelper

abstract class SosStationUpdater(private val stationQuery: StationQuery,
  private val boundingBox: BoundingBox) extends StationUpdater {

  // ---------------------------------------------------------------------------
  // Private Data
  // ---------------------------------------------------------------------------

  private val stationUpdater = new StationUpdateTool(stationQuery)
  private val httpSender = new HttpSender()
  private val geoTools = new GeoTools()
  private val rawDataRetriever = new SosRawDataRetriever()
  protected val serviceUrl: String
  protected val source: Source
  private val LOGGER = Logger.getLogger(getClass())
  private val dateParserFull = ISODateTimeFormat.dateTime()
  private val dateParserNoMillis = ISODateTimeFormat.dateTimeNoMillis()

  // ---------------------------------------------------------------------------
  // StationUpdater Members
  // ---------------------------------------------------------------------------

  def update() {
    val sourceStationSensors = getSourceStations(source)

    val databaseStations = stationQuery.getAllStations(source)

    stationUpdater.updateStations(sourceStationSensors, databaseStations)
  }

  protected def sensorForeignNotUsed = List(
    "http://mmisw.org/ont/cf/parameter/harmonic_constituents",
    "http://mmisw.org/ont/cf/parameter/datums",
    "http://mmisw.org/ont/cf/parameter/currents")

  // ---------------------------------------------------------------------------
  // Private Members
  // ---------------------------------------------------------------------------

  private def getSourceStations(source: Source): List[(DatabaseStation, List[(DatabaseSensor, List[DatabasePhenomenon])])] = {
    val observationOfferings = getObservationOfferingTypes()
    val size = observationOfferings.length - 1
    LOGGER.info("Total number of stations not filtered: " + (size + 1))

    val stationSensorsCollection = for {
      (observationOfferingType, index) <- observationOfferings.zipWithIndex
      val station = createSouceStation(observationOfferingType, source)
      if (withInBoundingBox(station))
      val sourceObservedProperties = getSourceObservedProperties(
        observationOfferingType, station, source)
      val databaseObservedProperties = stationUpdater.updateObservedProperties(
        source, sourceObservedProperties)
      val sensors = stationUpdater.getSourceSensors(station, databaseObservedProperties)
      if (sensors.nonEmpty)
    } yield {
      LOGGER.debug("[" + index + " of " + size + "] station: " + station.name)
      (station, sensors)
    }
    LOGGER.info("finished with stations")

    return stationSensorsCollection
  }

  private def withInBoundingBox(station: DatabaseStation): Boolean = {
    val stationLocation = GeomHelper.createLatLngPoint(station.latitude, station.longitude)
    return geoTools.isStationWithinRegion(stationLocation, boundingBox)
  }

  private def getObservationOfferingTypes(): List[ObservationOfferingType] = {
    getCapabilitiesDocument() match {
      case Some(capabilitiesDocument) => {
        capabilitiesDocument.getCapabilities().getContents().
          getObservationOfferingList().getObservationOfferingArray().filter(
            observationOffering => isStation(observationOffering)).toList
      }
      case None => {
        Nil
      }
    }
  }

  private def isStation(observationOffering: ObservationOfferingType): Boolean = {
    val procedures = observationOffering.getProcedureArray()
    if (procedures.nonEmpty) {
      val procedure = procedures(0)
      procedure.getHref().startsWith("urn:ioos:station:")
    } else {
      false
    }
  }

  protected def getCapabilitiesDocument(): Option[CapabilitiesDocument] = {
    val getCapabilitiesDocument = GetCapabilitiesDocument.Factory.newInstance

    val getCapabilities = getCapabilitiesDocument.addNewGetCapabilities
    getCapabilities.setService("SOS")

    val results = HttpSender.sendPostMessage(
      serviceUrl, getCapabilitiesDocument.xmlText())

    if (results != null) {
      Some(CapabilitiesDocument.Factory.parse(results))
    } else {
      None
    }
  }

  case class ForeignObservatedProperty(name: String, units: String, phenomenonUrl: String)

  private def getSourceObservedProperties(observationOfferingType: ObservationOfferingType,
    station: DatabaseStation, source: Source): List[ObservedProperty] = {
    val foreignObservatedProperties = getNamedQuantityTypes(observationOfferingType, station)

    val sourceObservedProperty = foreignObservatedProperties.flatMap(foreignObservatedProperty =>
      getObservedProperty(foreignObservatedProperty, source))

    LOGGER.info("returning " + sourceObservedProperty.size + " properties")
    return sourceObservedProperty
  }

  private def getObservedProperty(foreignObservatedProperty: ForeignObservatedProperty,
    source: Source): Option[ObservedProperty] = {
    LOGGER.debug("getting observed property: " + foreignObservatedProperty.phenomenonUrl +
      " - " + foreignObservatedProperty.name)
    foreignObservatedProperty match {
      case ForeignObservatedProperty(name @ "WaterTemperature", units, _) => {
        Some(stationUpdater.getObservedProperty(
          Phenomena.instance.SEA_WATER_TEMPERATURE, name, units, source))
      }
      case ForeignObservatedProperty(name @ "SignificantWaveHeight", units, _) => {
        Some(stationUpdater.getObservedProperty(
          Phenomena.instance.SIGNIFICANT_WAVE_HEIGHT, name, units, source))
      }
      case ForeignObservatedProperty(name @ "DominantWavePeriod", units, _) => {
        Some(stationUpdater.getObservedProperty(
          Phenomena.instance.DOMINANT_WAVE_PERIOD, name, units, source))
      }
      case ForeignObservatedProperty(name @ "AverageWavePeriod", units, _) => {
        Some(stationUpdater.getObservedProperty(
          Phenomena.instance.MEAN_WAVE_PERIOD, name, units, source))
      }
      case ForeignObservatedProperty(name @ "SwellHeight", units, _) => {
        Some(stationUpdater.getObservedProperty(
          Phenomena.instance.SEA_SURFACE_SWELL_WAVE_SIGNIFICANT_HEIGHT, name, units, source))
      }
      case ForeignObservatedProperty(name @ "SwellPeriod", units, _) => {
        Some(stationUpdater.getObservedProperty(
          Phenomena.instance.SEA_SURFACE_SWELL_WAVE_PERIOD, name, units, source))
      }
      case ForeignObservatedProperty(name @ "WindWaveHeight", units, _) => {
        Some(stationUpdater.getObservedProperty(
          Phenomena.instance.SEA_SURFACE_WIND_WAVE_SIGNIFICANT_HEIGHT, name, units, source))
      }
      case ForeignObservatedProperty(name @ "WindWavePeriod", units, _) => {
        Some(stationUpdater.getObservedProperty(
          Phenomena.instance.SEA_SURFACE_WIND_WAVE_PERIOD, name, units, source))
      }
      case ForeignObservatedProperty(name @ "SwellWaveDirection", units, _) => {
        Some(stationUpdater.getObservedProperty(
          Phenomena.instance.SEA_SURFACE_SWELL_WAVE_TO_DIRECTION, name, units, source))
      }
      case ForeignObservatedProperty(name @ "WindWaveDirection", units, _) => {
        Some(stationUpdater.getObservedProperty(
          Phenomena.instance.SEA_SURFACE_WIND_WAVE_TO_DIRECTION, name, units, source))
      }
      case ForeignObservatedProperty(name @ "WindSpeed", units, _) => {
        Some(stationUpdater.getObservedProperty(
          Phenomena.instance.WIND_SPEED, name, units, source))
      }
      case ForeignObservatedProperty(name @ "WindDirection", units, _) => {
        Some(stationUpdater.getObservedProperty(
          Phenomena.instance.WIND_FROM_DIRECTION, name, units, source))
      }
      case ForeignObservatedProperty(name @ "WindVerticalVelocity", units, _) => {
        Some(stationUpdater.getObservedProperty(
          Phenomena.instance.WIND_VERTICAL_VELOCITY, name, units, source))
      }
      case ForeignObservatedProperty(name @ "WindGust", units, _) => {
        Some(stationUpdater.getObservedProperty(
          Phenomena.instance.WIND_GUST_FROM_DIRECTION, name, units, source))
      }
      case ForeignObservatedProperty(name @ "WaterLevel", units, "http://mmisw.org/ont/cf/parameter/sea_surface_height_amplitude_due_to_equilibrium_ocean_tide") => {
        Some(stationUpdater.getObservedProperty(
          Phenomena.instance.SEA_SURFACE_HEIGHT_AMPLITUDE_DUE_TO_GEOCENTRIC_OCEAN_TIDE, name, units, source))
      }
      case ForeignObservatedProperty(name @ "WaterLevel", units, "http://mmisw.org/ont/cf/parameter/water_surface_height_above_reference_datum") => {
        Some(stationUpdater.getObservedProperty(
          Phenomena.instance.SEA_SURFACE_HEIGHT_ABOVE_SEA_LEVEL, name, units, source))
      }
      case ForeignObservatedProperty(name @ "Salinity", units, _) => {
        Some(stationUpdater.getObservedProperty(
          Phenomena.instance.SALINITY, name, units, source))
      }
      case ForeignObservatedProperty(name @ "Depth", units, _) => {
        Some(stationUpdater.getObservedProperty(
          Phenomena.instance.SEA_FLOOR_DEPTH_BELOW_SEA_SURFACE, name, units, source))
      }
      case ForeignObservatedProperty(name @ "CurrentDirection", units, _) => {
        Some(stationUpdater.getObservedProperty(
          Phenomena.instance.CURRENT_DIRECTION, name, units, source))
      }
      case ForeignObservatedProperty(name @ "CurrentSpeed", units, _) => {
        Some(stationUpdater.getObservedProperty(
          Phenomena.instance.CURRENT_SPEED, name, units, source))
      }
      case ForeignObservatedProperty(name @ "AirTemperature", units, _) => {
        Some(stationUpdater.getObservedProperty(
          Phenomena.instance.AIR_TEMPERATURE, name, units, source))
      }
      case ForeignObservatedProperty(name @ "BarometricPressure", units, _) => {
        Some(stationUpdater.getObservedProperty(
          Phenomena.instance.AIR_PRESSURE, name, units, source))
      }
      case ForeignObservatedProperty(name @ "SamplingRate", units, _) => None
      case ForeignObservatedProperty(name @ "WaveDuration", units, _) => None
      case ForeignObservatedProperty(name @ "MeanWaveDirectionPeakPeriod", units, _) => None
      case ForeignObservatedProperty(name, units, phenomenonUrl) => {
        LOGGER.debug(" observed property: " + name + " phenomenonUrl: " + phenomenonUrl +
          " is not processed correctly.")
        return None
      }
    }
  }

  private def convertUnits(originalUnits: String) =
    originalUnits match {
      case "deg" => "degrees"
      case x => x
    }

  private def createSouceStation(observationOffering: ObservationOfferingType,
    source: Source): DatabaseStation = {

    val procedure = observationOffering.getProcedureArray(0)
    val stationPostfixName = procedure.getHref().replace("urn:ioos:station:", "")
    val label = observationOffering.getDescription().getStringValue
    val (lat, lon) = getLatLon(observationOffering)

    val (timeBegin, timeEnd) = getTimeExtents(observationOffering)

    LOGGER.info("Processing station: " + label)
    return new DatabaseStation(label, stationPostfixName, stationPostfixName,
      "", "BUOY", source.id, lat, lon, timeBegin.getOrElse(null), timeEnd.getOrElse(null))
  }

  private def getLatLon(observationOfferingType: ObservationOfferingType): (Double, Double) = {
    val latLon = observationOfferingType.getBoundedBy().getEnvelope().getLowerCorner().getListValue
    val lat = latLon.get(0).toString().toDouble
    val lon = latLon.get(1).toString().toDouble

    return (lat, lon)
  }

  private def getTimeExtents(observationOffering: ObservationOfferingType): (Option[Timestamp], Option[Timestamp]) = {
    val tpelem = scala.xml.XML.loadString(observationOffering.getTime.toString)
    val rFull = "\\.\\d+Z$".r
    val beginval = (tpelem \ "beginPosition").text.trim
    var timeBegin : Option[Timestamp] = None
    try {
      timeBegin = beginval match {
        case "" => None
        case _ if rFull.findFirstIn(beginval) != None => Some(new Timestamp(dateParserFull.parseDateTime(beginval).getMillis))
        case _ => Some(new Timestamp(dateParserNoMillis.parseDateTime(beginval).getMillis))
      }
    } catch {
      case ex: IllegalArgumentException => {
        // pass
      }
    }

    val endval = (tpelem \ "endPosition").text.trim
    var timeEnd : Option[Timestamp] = None
    try {
      timeEnd = endval match {
        case "" => None
        case _ if rFull.findFirstIn(endval) != None => Some(new Timestamp(dateParserFull.parseDateTime(endval).getMillis))
        case _ => Some(new Timestamp(dateParserNoMillis.parseDateTime(endval).getMillis))
      }
    } catch {
      case ex: IllegalArgumentException => {
        // pass
      }
    }

    return (timeBegin, timeEnd)
  }

  private def getNamedQuantityTypes(observationOfferingType: ObservationOfferingType,
    station: DatabaseStation): List[ForeignObservatedProperty] = {
    (for {
      phenomenonUrl <- getSensorForeignIds(observationOfferingType)
      val rawData = rawDataRetriever.getRawDataLatest(serviceUrl,
        station.foreign_tag, phenomenonUrl)
      val xmlResult = scala.xml.XML.loadString(rawData)
    } yield {
      xmlResult.find(node => node.label == "CompositeObservation") match {
        case Some(compositeObNode) => {
          getPropertyNames(compositeObNode, phenomenonUrl)
        }
        case None => {
          LOGGER.warn(" station ID: " + station.foreign_tag +
            " phenomenon: " + phenomenonUrl + " error: parsing data ")
          Nil
        }
      }
    }).flatten
  }

  private def getPropertyNames(document: Node, phenomenonUrl: String): List[ForeignObservatedProperty] = {
    val resultNode = document \ "result"
    val composite = resultNode \ "Composite"

    (for {
      compositeValue <- composite \\ "CompositeValue"
      valueComponent <- compositeValue \ "valueComponents"
      quantity <- valueComponent \ "Quantity"
      names <- quantity.attribute("name")
      name <- names.headOption
      unitsList <- quantity.attribute("uom")
      units <- unitsList.headOption
    } yield {
      ForeignObservatedProperty(name.text, units.text, phenomenonUrl)
    }).toList
  }

  private def getSensorForeignIds(observationOfferingType: ObservationOfferingType): List[String] = {
    val sensorForeignIds = observationOfferingType.getObservedPropertyArray().map(_.getHref)

    val filteredSensorForeignIds = sensorForeignIds.filter(sensorForeignId =>
      !sensorForeignNotUsed.contains(sensorForeignId))

    return filteredSensorForeignIds.toList
  }
}
