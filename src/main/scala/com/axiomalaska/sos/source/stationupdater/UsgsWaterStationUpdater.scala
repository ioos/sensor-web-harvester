package com.axiomalaska.sos.source.stationupdater

import com.axiomalaska.phenomena.Phenomena
import com.axiomalaska.phenomena.Phenomenon
import com.axiomalaska.sos.data.Location
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
import scala.collection.JavaConversions._
import scala.collection.mutable
import org.apache.log4j.Logger
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
            getObservedProperty(Phenomena.instance.AIR_PRESSURE, id)
      }
      case "72019" => {
            getObservedProperty(Phenomena.instance.DEPTH_TO_WATER_LEVEL, id)
      }
      case "00020" => {
            getObservedProperty(Phenomena.instance.AIR_TEMPERATURE, id)
      }
      case "00021" => {
            getObservedProperty(Phenomena.instance.createHomelessParameter("air_temperature", Units.FAHRENHEIT), id)
      }
      case "00300" => {
            getObservedProperty(Phenomena.instance.DISSOLVED_OXYGEN, id)
      }
      case "00301" => {
            getObservedProperty(Phenomena.instance.DISSOLVED_OXYGEN_SATURATION, id)
      }
      case "00400" => {
            getObservedProperty(Phenomena.instance.SEA_WATER_PH_REPORTED_ON_TOTAL_SCALE, id)
      }
      case "00045" => {
            getObservedProperty(Phenomena.instance.PRECIPITATION_ACCUMULATED, id)
      }
      case "00062" => {
            getObservedProperty(Phenomena.instance.WATER_SURFACE_HEIGHT_ABOVE_REFERENCE_DATUM, id)
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
            getObservedProperty(Phenomena.instance.createHomelessParameter("stream_flow", Units.CUBIC_FOOT_PER_SECOUND), id)
      }
      case "00065" => {
            getObservedProperty(Phenomena.instance.createHomelessParameter("stream_gage_height", Units.FEET), id)
      }
      case "99065" => {
            getObservedProperty(Phenomena.instance.createHomelessParameter("stream_gage_height", Units.METERS), id)
      }
      case "00010" => {
            getObservedProperty(Phenomena.instance.SEA_WATER_TEMPERATURE, id)
      }
      case "85583" => {
            getObservedProperty(Phenomena.instance.WATER_TEMPERATURE_INTRAGRAVEL, id)
      }
      case "00035" => {
            getObservedProperty(Phenomena.instance.WIND_SPEED, id)
      }
      case "00036" => {
            getObservedProperty(Phenomena.instance.WIND_FROM_DIRECTION, id)
      }
      case "00036avg" => {
        return None
//        return new Some[ObservedProperty](
//          stationUpdater.createObservedProperty(Sensors.WINDS, id,
//            source, "Wind Direction", Units.DEGREES, 
//            SensorPhenomenonIds.WIND_DIRECTION, ValueDataType.Avg, CompositeType.Azimuth))
      }
      case "61728" => {
            getObservedProperty(Phenomena.instance.WIND_SPEED_OF_GUST, id)
      }
      case "62628" => {
            getObservedProperty(Phenomena.instance.createHomelessParameter("wind_speed", Units.METER_PER_SECONDS), id)
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