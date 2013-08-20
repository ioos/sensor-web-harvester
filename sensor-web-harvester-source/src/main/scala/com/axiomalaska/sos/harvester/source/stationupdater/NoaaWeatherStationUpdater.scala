package com.axiomalaska.sos.harvester.source.stationupdater

import com.axiomalaska.phenomena.Phenomena
import com.axiomalaska.phenomena.Phenomenon
import com.axiomalaska.sos.tools.HttpSender
import com.axiomalaska.sos.harvester.BoundingBox;
import com.axiomalaska.sos.harvester.GeoTools;
import com.axiomalaska.sos.harvester.source.SourceUrls;
import com.axiomalaska.sos.harvester.StationQuery;
import com.axiomalaska.sos.harvester.Units;
import com.axiomalaska.sos.harvester.data.DatabasePhenomenon;
import com.axiomalaska.sos.harvester.data.DatabaseSensor;
import com.axiomalaska.sos.harvester.data.DatabaseStation;
import com.axiomalaska.sos.harvester.data.ObservedProperty;
import com.axiomalaska.sos.harvester.data.SourceId;
import com.axiomalaska.sos.harvester.data.LocalPhenomenon
import scala.collection.JavaConversions._
import org.apache.log4j.Logger
import com.axiomalaska.ioos.sos.GeomHelper

class NoaaWeatherStationUpdater(private val stationQuery: StationQuery,
  private val boundingBox: BoundingBox) extends StationUpdater {

  // ---------------------------------------------------------------------------
  // Private Data
  // ---------------------------------------------------------------------------

  private val stationUpdater = new StationUpdateTool(stationQuery)
  private val httpSender = new HttpSender()
  private val geoTools = new GeoTools()
  private val source = stationQuery.getSource(SourceId.NOAA_WEATHER)
  private val LOGGER = Logger.getLogger(getClass())

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

  private def getSourceStations(observedProperties: List[ObservedProperty]): List[(DatabaseStation, List[(DatabaseSensor, List[DatabasePhenomenon])])] = {

    val stationSensorsCollection = for {
      station <- getStations()
      val sensors = stationUpdater.getSourceSensors(station, observedProperties)
    } yield {
      (station, sensors)
    }

    LOGGER.info("Finished with processing " + stationSensorsCollection.size + " stations")

    stationSensorsCollection
  }

  private def getStations(): List[DatabaseStation] = {
    val data = HttpSender.sendGetMessage(
      SourceUrls.NOAA_WEATHER_COLLECTION_OF_STATIONS)

    if (data != null) {
      val stations = for {
        line <- data.split("\n")
        val rows = line.split(";")
        if (rows.size > 8)
        val station = createStation(rows)
        if (withInBoundingBox(station))
        if (HttpSender.doesUrlExist(
          SourceUrls.NOAA_WEATHER_OBSERVATION_RETRIEVAL +
            station.foreign_tag + ".html"))
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

    LOGGER.info("Processing station: " + label)
    new DatabaseStation(label, source.tag + ":" + foreignId, foreignId, "",
      "FIXED MET STATION", source.id, latitude, longitude, null, null)
  }

  private def withInBoundingBox(station: DatabaseStation): Boolean = {
    val stationLocation = GeomHelper.createLatLngPoint(station.latitude, station.longitude)
    return geoTools.isStationWithinRegion(stationLocation, boundingBox)
  }

  private def parseLatitude(rawLatitude: String): Double = {
    val latitudeHem = rawLatitude.last
    val latSplit = rawLatitude.dropRight(1).split("-")

    val lat = latSplit.size match {
      case 2 => {
        latSplit(0).toDouble + latSplit(1).toDouble / 60
      }
      case 3 => {
        latSplit(0).toDouble + latSplit(1).toDouble / 60 + latSplit(2).toDouble / 60 / 60
      }
    }

    latitudeHem match {
      case 'N' => lat
      case 'S' => (-1) * lat
      case _ => lat
    }
  }

  private def parseLongitude(rawLongitude: String): Double = {
    val longitudeHem = rawLongitude.last
    val lonSplit = rawLongitude.dropRight(1).split("-")

    val lonValue = lonSplit.size match {
      case 2 => {
        (lonSplit(0).toDouble + lonSplit(1).toDouble / 60)
      }
      case 3 => {
        (lonSplit(0).toDouble + lonSplit(1).toDouble / 60 + +lonSplit(2).toDouble / 60 / 60)
      }
    }

    longitudeHem match {
      case 'E' => lonValue
      case 'W' => (-1) * lonValue
      case _ => (-1) * lonValue
    }
  }
    
  private def getSourceObservedProperties() = List(
    stationUpdater.getObservedProperty(
      Phenomena.instance.AIR_TEMPERATURE, "Temperature", Units.FAHRENHEIT, source),
    stationUpdater.getObservedProperty(
      Phenomena.instance.DEW_POINT_TEMPERATURE, "Dew Point", Units.FAHRENHEIT, source),
    stationUpdater.getObservedProperty(
      Phenomena.instance.WIND_SPEED, "Wind Speed", Units.MILES_PER_HOUR, source),
    stationUpdater.getObservedProperty(
      Phenomena.instance.WIND_FROM_DIRECTION, "Wind Direction", Units.DEGREES, source),
    stationUpdater.getObservedProperty(
      Phenomena.instance.AIR_PRESSURE, "Pressure", Units.INCHES_OF_MERCURY, source))
}
