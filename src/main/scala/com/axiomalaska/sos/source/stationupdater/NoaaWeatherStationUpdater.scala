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
import java.util.Calendar
import java.util.TimeZone
import scala.collection.JavaConversions._
import org.apache.log4j.Logger

class NoaaWeatherStationUpdater(private val stationQuery: StationQuery,
  private val boundingBox: BoundingBox, 
  private val logger: Logger = Logger.getRootLogger()) extends StationUpdater {

  // ---------------------------------------------------------------------------
  // Private Data
  // ---------------------------------------------------------------------------

  private val stationUpdater = new StationUpdateTool(stationQuery, logger)
  private val httpSender = new HttpSender()
  private val geoTools = new GeoTools()
  private val source = stationQuery.getSource(SourceId.NOAA_WEATHER)
  
  // ---------------------------------------------------------------------------
  // Public Members
  // ---------------------------------------------------------------------------

  def update() {
    val sourceObservedProperies = getSourceObservedProperties()
    
    val observedProperties = 
      stationUpdater.updateObservedProperties(source, sourceObservedProperies)

    val sourceStationSensors = getSourceStations(observedProperties)

    val databaseStations = stationQuery.getAllStations(source)

    stationUpdater.updateStations(sourceStationSensors, databaseStations)
  }
  
  val name = "NOAA Weather"
  
  // ---------------------------------------------------------------------------
  // Private Members
  // ---------------------------------------------------------------------------

  private def getSourceStations(observedProperties:  List[ObservedProperty]):
	  List[(DatabaseStation, List[(DatabaseSensor, List[DatabasePhenomenon])])] = {

    val stationSensorsCollection = for {station <- getStations()
      val sensors = stationUpdater.getSourceSensors(station, observedProperties)} yield {
      (station, sensors)
    }

    logger.info("Finished with processing " + stationSensorsCollection.size + " stations")

    stationSensorsCollection
  }
  
  private def getStations():List[DatabaseStation] ={
    val data = httpSender.sendGetMessage(
      "http://weather.noaa.gov/data/nsd_cccc.txt")

    if (data != null) {
      val stations = for {
        line <- data.split("\n")
        val rows = line.split(";")
        if (rows.size > 8)
        val station = createStation(rows)
        if (withInBoundingBox(station))
        if (httpSender.doesUrlExists("http://www.nws.noaa.gov/data/obhistory/" + station.foreign_tag + ".html"))
      } yield { station }

      stations.toList
    } else {
      Nil
    }
  }

  private def createStation(rows: Array[String]): DatabaseStation = {
    val foreignId = rows(0)
    val label = rows(3)
    val latitudeRaw = rows(7)
    val longitudeRaw = rows(8)
    val latitude = parseLatitude(latitudeRaw)
    val longitude = parseLongitude(longitudeRaw)
    
    logger.info("Processing station: " + label)
    new DatabaseStation(label, foreignId, foreignId, "",
      "FIXED MET STATION", source.id, latitude, longitude)
  }

  private def withInBoundingBox(station: DatabaseStation): Boolean = {
    val stationLocation = new Location(station.latitude, station.longitude)
    return geoTools.isStationWithinRegion(stationLocation, boundingBox)
  }
  
  private def parseLatitude(rawLatitude:String):Double = {
    val latitudeHem = rawLatitude.last
    val latSplit = rawLatitude.dropRight(1).split("-")
    
    val lat = latSplit.size match{
      case 2 => {
        latSplit(0).toDouble + latSplit(1).toDouble/60
      }
      case 3 => {
        latSplit(0).toDouble + latSplit(1).toDouble/60 + latSplit(2).toDouble/60/60
      }
    }
    
    latitudeHem match{
      case 'N' => lat
      case 'S' => (-1)*lat
      case _ => lat
    }
  }
  
  private def parseLongitude(rawLongitude:String):Double = {
    val longitudeHem = rawLongitude.last
    val lonSplit = rawLongitude.dropRight(1).split("-")
    
    val lonValue = lonSplit.size match{
      case 2 => {
        (lonSplit(0).toDouble + lonSplit(1).toDouble/60)
      }
      case 3 => {
        (lonSplit(0).toDouble + lonSplit(1).toDouble/60 +  + lonSplit(2).toDouble/60/60)
      }
    }
    
    longitudeHem match{
      case 'E' => lonValue
      case 'W' => (-1)*lonValue
      case _ => (-1)*lonValue
    }
  }
  
  private def getSourceObservedProperties() = List(
    stationUpdater.createObservedProperty("Temperature", source,
       Units.FAHRENHEIT, SensorPhenomenonIds.AIR_TEMPERATURE),
    stationUpdater.createObservedProperty("Dew Point", source,
      Units.FAHRENHEIT, SensorPhenomenonIds.DEW_POINT),
    stationUpdater.createObservedProperty("Wind Speed", source, 
        Units.MILES_PER_HOUR, SensorPhenomenonIds.WIND_SPEED),
    stationUpdater.createObservedProperty("Wind Direction",
      source, Units.DEGREES, SensorPhenomenonIds.WIND_DIRECTION),
    stationUpdater.createObservedProperty("Pressure",
      source, Units.INCHES_OF_MERCURY, SensorPhenomenonIds.BAROMETRIC_PRESSURE))
}