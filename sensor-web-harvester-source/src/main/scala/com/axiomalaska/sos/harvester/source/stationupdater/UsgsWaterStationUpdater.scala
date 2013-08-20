package com.axiomalaska.sos.harvester.source.stationupdater

import com.axiomalaska.phenomena.Phenomena
import com.axiomalaska.phenomena.Phenomenon
import com.axiomalaska.sos.tools.HttpSender
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
import com.axiomalaska.sos.harvester.data.Source;
import com.axiomalaska.sos.harvester.data.SourceId;
import com.axiomalaska.sos.harvester.data.LocalPhenomenon
import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.{Set => MSet}
import org.apache.log4j.Logger
import org.cuahsi.waterML.x11.TimeSeriesType
import org.cuahsi.waterML.x11.TimeSeriesResponseDocument
import org.cuahsi.waterML.x11.SiteInfoType
import org.cuahsi.waterML.x11.LatLonPointType
import com.axiomalaska.ioos.sos.GeomHelper
import com.axiomalaska.sos.harvester.data.PhenomenaFactory

class UsgsWaterStationUpdater(private val stationQuery: StationQuery,
  private val boundingBox: BoundingBox) extends StationUpdater {

  // ---------------------------------------------------------------------------
  // Private Data
  // ---------------------------------------------------------------------------
  private val LOGGER = Logger.getLogger(getClass())
  private val stationUpdater = new StationUpdateTool(stationQuery)
  private val httpSender = new HttpSender()
  private val geoTools = new GeoTools()
  private val source = stationQuery.getSource(SourceId.USGSWATER)
  private val phenomenaFactory = new PhenomenaFactory()
  
  //always request these since we don't currently have spatial data for them.
  //could be improved if we had a shapefile that included them (but still should be small)
  private val nonStates = Set("aq", "gu", "mp", "pr", "vi")
  
  // ---------------------------------------------------------------------------
  // Public Members
  // ---------------------------------------------------------------------------

  def update() {
    val sourceStationSensors = getSourceStations()

    val databaseStations = stationQuery.getAllStations(source)

    stationUpdater.updateStations(sourceStationSensors, databaseStations)
  }

  val name = "USGS Water"

  // ---------------------------------------------------------------------------
  // Private Members
  // ---------------------------------------------------------------------------

  private def getSourceStations(): List[(DatabaseStation, List[(DatabaseSensor, List[DatabasePhenomenon])])] = {
    val timeSerieses = getStatesTimeSeriesTypes
    val stations = createSourceStations(timeSerieses)
    val size = stations.length - 1
    LOGGER.info("Number of unfiltered stations= " + size)
    val stationSensorsCollection = for {
      (station, index) <- stations.zipWithIndex;
      if (withInBoundingBox(station))
      val sourceObservedProperties = createObservedProperties(timeSerieses, station)
      val databaseObservedProperties = stationUpdater.updateObservedProperties(source, sourceObservedProperties)
      val sensors = stationUpdater.getSourceSensors(station, databaseObservedProperties)
      if (sensors.nonEmpty)
    } yield {
      LOGGER.debug("[" + index + " of " + size + "] station: " + station.name)
      (station, sensors)
    }

    LOGGER.info("Finished with processing " + stationSensorsCollection.size + " stations")

    return stationSensorsCollection
  }

  private def withInBoundingBox(station: DatabaseStation): Boolean = {
    val stationLocation = GeomHelper.createLatLngPoint(station.latitude, station.longitude)
    return geoTools.isStationWithinRegion(stationLocation, boundingBox)
  }

  private def getStatesTimeSeriesTypes(): List[TimeSeriesType] = {
    val abbrs = for{ state <- MSet(GeoTools.statesInBoundingBox(boundingBox).toList:_*)} yield stateAbbreviations(state)
    //TODO disabling non-state harvest for now, add more robust spatial layer in the future
//    abbrs ++= nonStates
    abbrs.toList.flatMap(getTimeSeriesTypes)
  }    

  private def getTimeSeriesTypes(stateTag: String): List[TimeSeriesType] = {

    LOGGER.info("Processing state: " + stateTag)
    val rawServerData = HttpSender.sendGetMessage(
      SourceUrls.USGS_WATER_COLLECTION_OF_STATE_STATIONS
        + stateTag + "&period=PT4H")

    if (rawServerData != null) {
      val documentOption = try {
        Some(TimeSeriesResponseDocument.Factory.parse(rawServerData))
      } catch {
        case e: Exception => {
          None
        }
      }
      documentOption match {
        case Some(document) => {
          document.getTimeSeriesResponse().getTimeSeriesArray().filter(timeSeriesType =>
            timeSeriesType.getValuesArray().length > 0 &&
              timeSeriesType.getValuesArray(0).getValueArray().length > 0 &&
              !timeSeriesType.getValuesArray(0).getValueArray(0).getStringValue.equals("-999999")).toList
        }
        case None => Nil
      }
    } else {
      Nil
    }
  }

  private def createSourceStations(
    timeSerieses: List[TimeSeriesType]): List[DatabaseStation] = {

    val stationMap = mutable.Map[String, DatabaseStation]()

    timeSerieses.foreach(timeSeriesType => {
      createStation(timeSeriesType, source) match {
        case Some(station) => {
          stationMap.put(station.foreign_tag, station)
        }
        case None => //do nothing
      }
    })

    return stationMap.values.toList
  }

  private def createObservedProperties(timeSerieses: List[TimeSeriesType],
    station: DatabaseStation): List[ObservedProperty] = {

    val matchingTimeSeriesTypes = timeSerieses.filter(timeSeries => timeSeries.getSourceInfo() match {
      case s: SiteInfoType => {
        s.getSiteCodeArray(0).getStringValue == station.foreign_tag
      }
      case _ => false
    })

    val sourceObservedProperies = for {
      timeSeriesType <- matchingTimeSeriesTypes
      vc <- timeSeriesType.getVariable().getVariableCodeArray()
      val observedPropertyOption = getObservedProperty(vc.getStringValue)
      if (!observedPropertyOption.isEmpty)
    } yield {
      observedPropertyOption.get
    }

    return sourceObservedProperies
  }

  private def getObservedProperty(id: String): Option[ObservedProperty] = {
    id match {
      case "70969" => {
        return None
      }
      case "72106" => {
        return None
      }
      case "00025" => {
        Some(stationUpdater.getObservedProperty(
            Phenomena.instance.AIR_PRESSURE, id, Units.MILLIMETER_PER_MERCURY, source))
      }
      case "72019" => {
        Some(stationUpdater.getObservedProperty(
            Phenomena.instance.DEPTH_TO_WATER_LEVEL, id, Units.FEET, source))
      }
      case "00020" => {
        Some(stationUpdater.getObservedProperty(
            Phenomena.instance.AIR_TEMPERATURE, id, Units.CELSIUS, source))
      }
      case "00021" => {
        Some(stationUpdater.getObservedProperty(
            Phenomena.instance.AIR_TEMPERATURE, id, Units.FAHRENHEIT, source))
      }
      case "00300" => {
        Some(stationUpdater.getObservedProperty(
            Phenomena.instance.DISSOLVED_OXYGEN, id, Units.MILLIGRAMS_PER_LITER, source))
      }
      case "00301" => {
        Some(stationUpdater.getObservedProperty(
            Phenomena.instance.DISSOLVED_OXYGEN_SATURATION, id, Units.PERCENT, source))
      }
      case "00400" => {
        Some(stationUpdater.getObservedProperty(
            Phenomena.instance.SEA_WATER_PH_REPORTED_ON_TOTAL_SCALE, id, Units.STD_UNITS, source))
      }
      case "00045" => {
        Some(stationUpdater.getObservedProperty(
            Phenomena.instance.PRECIPITATION_ACCUMULATED, id, Units.INCHES, source))
      }
      case "00062" => {
        Some(stationUpdater.getObservedProperty(
            Phenomena.instance.WATER_SURFACE_HEIGHT_ABOVE_REFERENCE_DATUM, id, Units.FEET, source))
      }
      case "00095" => {
        return None
        //        return new Some[ObservedProperty](
        //          stationUpdater.createObservedProperty(Sensors.Specific Conductance of Water", id,
        //            source, "Specific Conductance of Water", "uS/cm @25C", 
        //            SensorPhenomenonIds.SpecificConductanceofWater"))
      }
      case "00060" => {
        val url = Phenomena.GENERIC_FAKE_MMI_URL_PREFIX + "stream_flow"
        Some(stationUpdater.getObservedProperty(
            phenomenaFactory.findCustomPhenomenon(url), id, Units.CUBIC_FOOT_PER_SECOUND, source))
      }
      case "00065" => {
        val url = Phenomena.GENERIC_FAKE_MMI_URL_PREFIX + "stream_gage_height"
        Some(stationUpdater.getObservedProperty(
          phenomenaFactory.findCustomPhenomenon(url), id, Units.FEET, source))
      }
      case "99065" => {
        val url = Phenomena.GENERIC_FAKE_MMI_URL_PREFIX + "stream_gage_height"
        Some(stationUpdater.getObservedProperty(
          phenomenaFactory.findCustomPhenomenon(url), id, Units.METERS, source))
      }
      case "00010" => {
        Some(stationUpdater.getObservedProperty(
            Phenomena.instance.SEA_WATER_TEMPERATURE, id, Units.CELSIUS, source))
      }
      case "85583" => {
        Some(stationUpdater.getObservedProperty(
            Phenomena.instance.WATER_TEMPERATURE_INTRAGRAVEL, id, Units.CELSIUS, source))
      }
      case "00035" => {
        Some(stationUpdater.getObservedProperty(
            Phenomena.instance.WIND_SPEED, id, Units.MILES_PER_HOUR, source))
      }
      case "00036" => {
        Some(stationUpdater.getObservedProperty(
            Phenomena.instance.WIND_FROM_DIRECTION, id, Units.DEGREES, source))
      }
      case "00036avg" => {
        return None
        //        return new Some[ObservedProperty](
        //          stationUpdater.createObservedProperty(Sensors.WINDS, id,
        //            source, "Wind Direction", Units.DEGREES, 
        //            SensorPhenomenonIds.WIND_DIRECTION, ValueDataType.Avg, CompositeType.Azimuth))
      }
      case "61728" => {
        Some(stationUpdater.getObservedProperty(
            Phenomena.instance.WIND_SPEED_OF_GUST, id, Units.MILES_PER_HOUR, source))
      }
      case "62628" => {
        Some(stationUpdater.getObservedProperty(
            Phenomena.instance.WIND_SPEED, id, Units.METER_PER_SECONDS, source))
      }
      case _ => {
        LOGGER.debug("[" + source.name + "] observed property: " + id +
          " is not processed correctly.")
        return None
      }
    }
  }

  private def createStation(timeSeriesType: TimeSeriesType, source: Source): Option[DatabaseStation] = {
    timeSeriesType.getSourceInfo() match {
      case s: SiteInfoType => {
        s.getGeoLocation().getGeogLocation() match {
          case p: LatLonPointType => {
            val foreignId = s.getSiteCodeArray(0).getStringValue
            val station = new DatabaseStation(s.getSiteName,
              source.tag + ":" + foreignId, foreignId, "", "FIXED MET STATION", source.id,
              p.getLatitude, p.getLongitude, null, null)

            return Some(station)
          }
        }
      }
    }

    return None
  }
}
