package com.axiomalaska.sos.source.stationupdater

import com.axiomalaska.sos.data.Location
import com.axiomalaska.sos.tools.HttpPart
import com.axiomalaska.sos.tools.HttpSender
import com.axiomalaska.sos.source.BoundingBox
import com.axiomalaska.sos.source.data.DatabasePhenomenon
import com.axiomalaska.sos.source.data.DatabaseSensor
import com.axiomalaska.sos.source.data.DatabaseStation
import com.axiomalaska.sos.source.GeoTools
import com.axiomalaska.sos.source.data.ObservedProperty
import com.axiomalaska.sos.source.data.Source
import com.axiomalaska.sos.source.StationQuery
import com.axiomalaska.sos.source.Units
import com.axiomalaska.sos.source.data.SensorPhenomenonIds
import com.axiomalaska.sos.source.data.SourceId
import scala.collection.JavaConversions._
import scala.collection.mutable
import org.apache.log4j.Logger
import java.util.Calendar
import java.util.TimeZone
import org.cuahsi.waterML.x11.TimeSeriesType
import org.cuahsi.waterML.x11.TimeSeriesResponseDocument
import org.cuahsi.waterML.x11.SiteInfoType
import org.cuahsi.waterML.x11.LatLonPointType

class UsgsWaterStationUpdater(private val stationQuery: StationQuery,
  private val boundingBox: BoundingBox, 
  private val logger: Logger = Logger.getRootLogger()) extends StationUpdater {

  // ---------------------------------------------------------------------------
  // Private Data
  // ---------------------------------------------------------------------------

  private val stationUpdater = new StationUpdateTool(stationQuery, logger)
  private val httpSender = new HttpSender()
  private val geoTools = new GeoTools()
  private val source = stationQuery.getSource(SourceId.USGSWATER)
  
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

  private def getSourceStations(): 
	  List[(DatabaseStation, List[(DatabaseSensor, List[DatabasePhenomenon])])] = {
    val timeSerieses = getAllStatesTimeSeriesTypes
    val stations = createSourceStations(timeSerieses)
    val size = stations.length - 1
    logger.info("Number of unfiltered stations= " + size)
    val stationSensorsCollection = for {
      (station, index) <- stations.zipWithIndex;
      if (withInBoundingBox(station))
      val sourceObservedProperties = createObservedProperties(timeSerieses, station)
      val databaseObservedProperties =
        stationUpdater.updateObservedProperties(source, sourceObservedProperties)
      val sensors = stationUpdater.getSourceSensors(station, databaseObservedProperties)
      if(sensors.nonEmpty)
    } yield {
      logger.debug("[" + index + " of " + size + "] station: " + station.name)
      (station, sensors)
    }
    
    logger.info("Finished with processing " + stationSensorsCollection.size + " stations")

    return stationSensorsCollection
  }

  private def withInBoundingBox(station: DatabaseStation): Boolean = {
    val stationLocation = new Location(station.latitude, station.longitude)
    return geoTools.isStationWithinRegion(stationLocation, boundingBox)
  }
  
  private def getAllStatesTimeSeriesTypes():List[TimeSeriesType] = 
    getStateTags().flatMap(getTimeSeriesTypes)
  
  private def getStateTags() = List("al", "ak", "aq", "az", "ar", "ca", "co", 
      "ct", "de", "dc", "fl", "ga", "gu", "hi", "id", "il", "in", "ia", "ks", 
      "ky", "la", "me", "md", "ma", "mi", "mn", "ms", "mo", "mt", "ne", "nv", 
      "nh", "nj", "nm", "ny", "nc", "nd", "mp", "oh", "ok", "or", "pa", "pr", 
      "ri", "sc", "sd", "tn", "tx", "ut", "vt", "vi", "va", "wa", "wv", "wi", 
      "wy")
  
  private def getTimeSeriesTypes(stateTag:String):List[TimeSeriesType] ={
    logger.info("Processing state: " + stateTag)
    val rawServerData = httpSender.sendGetMessage(
        "http://waterservices.usgs.gov/nwis/iv?stateCd=" + stateTag + "&period=PT4H")

    if (rawServerData != null) {
      val documentOption = try{
        Some(TimeSeriesResponseDocument.Factory.parse(rawServerData))
      }
      catch{
        case e:Exception =>{
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
      timeSerieses:List[TimeSeriesType]):List[DatabaseStation] ={
    
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
  
  private def createObservedProperties(timeSerieses:List[TimeSeriesType], 
      station: DatabaseStation): List[ObservedProperty] = {
    
    val matchingTimeSeriesTypes = timeSerieses.filter(timeSeries => timeSeries.getSourceInfo() match{
      case s: SiteInfoType => {
        s.getSiteCodeArray(0).getStringValue == station.foreign_tag
      }
      case _ => false
    })

    val sourceObservedProperies = for {
      timeSeriesType <- matchingTimeSeriesTypes
      vc <- timeSeriesType.getVariable().getVariableCodeArray()
      val observedPropertyOption = getObservedProperty(vc.getStringValue)
      if(!observedPropertyOption.isEmpty)
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
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(id,
            source, Units.MILLIMETER_PER_MERCURY, 
            SensorPhenomenonIds.BAROMETRIC_PRESSURE))
      }
      case "72019" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(id,
            source, Units.FEET, 
            SensorPhenomenonIds.DEPTH_TO_WATER_LEVEL))
      }
      case "00020" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(id,
            source, Units.CELSIUS, 
            SensorPhenomenonIds.AIR_TEMPERATURE))
      }
      case "00021" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(id,
            source, Units.FAHRENHEIT, 
            SensorPhenomenonIds.AIR_TEMPERATURE))
      }
      case "00300" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(id,
            source, Units.MILLIGRAMS_PER_LITER, 
            SensorPhenomenonIds.DISSOLVED_OXYGEN))
      }
      case "00301" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(id,
            source, Units.PERCENT, 
            SensorPhenomenonIds.DISSOLVED_OXYGEN_SATURATION))
      }
      case "00400" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(id,
            source, Units.STD_UNITS, 
            SensorPhenomenonIds.PH_WATER))
      }
      case "00045" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(id,
            source, Units.INCHES, 
            SensorPhenomenonIds.PRECIPITATION_ACCUMULATION))
      }
      case "00062" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(id,
            source, Units.FEET, 
            SensorPhenomenonIds.RESERVOIR_WATER_SURFACE_ABOVE_DATUM))
      }
      case "00095" => {
        return None
//        return new Some[ObservedProperty](
//          stationUpdater.createObservedProperty(Sensors.Specific Conductance of Water", id,
//            source, "Specific Conductance of Water", "uS/cm @25C", 
//            SensorPhenomenonIds.SpecificConductanceofWater"))
      }
      case "00060" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(id,
            source, Units.CUBIC_FOOT_PER_SECOUND, 
            SensorPhenomenonIds.STREAM_FLOW))
      }
      case "00065" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(id,
            source, Units.FEET, 
            SensorPhenomenonIds.STREAM_GAGE_HEIGHT))
      }
      case "99065" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(id,
            source, Units.METERS, 
            SensorPhenomenonIds.STREAM_GAGE_HEIGHT))
      }
      case "00010" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(id,
            source, Units.CELSIUS, 
            SensorPhenomenonIds.SEA_WATER_TEMPERATURE))
      }
      case "85583" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(id,
            source, Units.CELSIUS, 
            SensorPhenomenonIds.WATER_TEMPERATURE_INTRAGRAVEL))
      }
      case "00035" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(id,
            source, Units.MILES_PER_HOUR, 
            SensorPhenomenonIds.WIND_SPEED))
      }
      case "00036" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(id,
            source, Units.DEGREES, 
            SensorPhenomenonIds.WIND_DIRECTION))
      }
      case "00036avg" => {
        return None
//        return new Some[ObservedProperty](
//          stationUpdater.createObservedProperty(Sensors.WINDS, id,
//            source, "Wind Direction", Units.DEGREES, 
//            SensorPhenomenonIds.WIND_DIRECTION, ValueDataType.Avg, CompositeType.Azimuth))
      }
      case "61728" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(id,
            source, Units.MILES_PER_HOUR, 
            SensorPhenomenonIds.WIND_GUST))
      }
      case "62628" => {
        return new Some[ObservedProperty](
          stationUpdater.createObservedProperty(id,
            source, Units.METER_PER_SECONDS, 
            SensorPhenomenonIds.WIND_SPEED))
      }
      case _ => {
        logger.debug("[" + source.name + "] observed property: " + id +
          " is not processed correctly.")
        return None
      }
    }
  }
    
  private def createStation(timeSeriesType: TimeSeriesType, source:Source): Option[DatabaseStation] = {
    timeSeriesType.getSourceInfo() match {
      case s: SiteInfoType => {
        s.getGeoLocation().getGeogLocation() match {
          case p: LatLonPointType => {
            val foreignId = s.getSiteCodeArray(0).getStringValue
            val station = new DatabaseStation(s.getSiteName, foreignId, foreignId, "", "FIXED MET STATION", source.id,
              p.getLatitude, p.getLongitude)

            return Some(station)
          }
        }
      }
    }

    return None
  }
}