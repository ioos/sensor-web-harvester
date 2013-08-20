package com.axiomalaska.sos.harvester.source.stationupdater

import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.mutable

import org.apache.log4j.Logger

import com.axiomalaska.phenomena.Phenomena
import com.axiomalaska.phenomena.Phenomenon
import com.axiomalaska.sos.harvester.StationQuery
import com.axiomalaska.sos.harvester.data.DatabasePhenomenon
import com.axiomalaska.sos.harvester.data.DatabaseSensor
import com.axiomalaska.sos.harvester.data.DatabaseStation
import com.axiomalaska.sos.harvester.data.ObservedProperty
import com.axiomalaska.sos.harvester.data.Source

class StationUpdateTool(private val stationQuery: StationQuery) {

  private val LOGGER = Logger.getLogger(getClass())

  // ---------------------------------------------------------------------------
  // Public Members
  // ---------------------------------------------------------------------------

  def updateStations(sourceStationSensors: List[(DatabaseStation, List[(DatabaseSensor, List[DatabasePhenomenon])])],
    databaseStations: List[DatabaseStation]) {

    for (sourceStationSensor <- sourceStationSensors) {
      val sourceStation = sourceStationSensor._1
      val sourceSensors = sourceStationSensor._2

      databaseStations.filter(databaseStation =>
        databaseStation.foreign_tag == sourceStation.foreign_tag).headOption match {
        case Some(databaseStation: DatabaseStation) => {
          if (!areDoublesEquals(databaseStation.latitude, sourceStation.latitude) ||
            !areDoublesEquals(databaseStation.longitude, sourceStation.longitude) ||
            databaseStation.timeBegin != sourceStation.timeBegin ||
            databaseStation.timeEnd != sourceStation.timeEnd ) {
            stationQuery.updateStation(databaseStation, sourceStation)
            LOGGER.info("Updating Station " + databaseStation.name)
          }

          val databaseSenors = stationQuery.getAllSensors(databaseStation)

          val createdSensors = for {
            (sourceSensor, phenomena) <- sourceSensors
            if (!isThereAnEqualSensor(databaseSenors, sourceSensor, phenomena))
          } yield {
            val createdSensor = stationQuery.createSensor(databaseStation, sourceSensor)
            phenomena.foreach(phenomenon =>
              stationQuery.associatePhenomonenToSensor(createdSensor, phenomenon))

            createdSensor
          }

          if (createdSensors.nonEmpty) {
            LOGGER.info("Association Sensors " + createdSensors.map(s => s.tag).mkString(", ") +
              " to Station: " + sourceStation.name)
          }
        }
        case None => {
          val databaseStation = stationQuery.createStation(sourceStation)
          LOGGER.info("Created new station: " + databaseStation.name)

          for ((sourceSensor, phenomena) <- sourceSensors) {
            val createdSensor = stationQuery.createSensor(databaseStation, sourceSensor)
            phenomena.foreach(phenomenon =>
              stationQuery.associatePhenomonenToSensor(createdSensor, phenomenon))
          }
          LOGGER.info("Created new sensors: " + sourceSensors.map(s => s._1.tag).mkString(", "))
        }
      }
    }
  }

  def getSourceSensors(station: DatabaseStation,
    observedProperties: List[ObservedProperty]): List[(DatabaseSensor, List[DatabasePhenomenon])] = {
    val set = new mutable.HashSet[Long]
    val sensors = for {
      observedProperty <- observedProperties
      if (!set.contains(observedProperty.phenomenon_id))
    } yield {
      set += observedProperty.phenomenon_id
      val databasePhenomenon = stationQuery.getPhenomenon(observedProperty.phenomenon_id)
      val phenomenon = findPhenomenon(databasePhenomenon)
      val sensorName = getSensorName(databasePhenomenon)
      (new DatabaseSensor(sensorName, phenomenon.getName(), station.id),
        List(databasePhenomenon))
    }

    return sensors
  }

  private def getSensorName(databasePhenomenon: DatabasePhenomenon): String = {
    val index = databasePhenomenon.tag.lastIndexOf("/") + 1
    databasePhenomenon.tag.substring(index)
  }

  /*
     * This is a very slow process for large number of inserts, needs to be rewritten
     * for instances of large number of insertions. Probably would be best to write
     * output to a file and then use postgres's COPY method to write file into db.
     * Also, might want to consider an index on foreign_tag or something.
     */
  def updateObservedProperties(source: Source,
    sourceObservedProperies: List[ObservedProperty]): List[ObservedProperty] = {

    val observedProperties = for (observedProperty <- sourceObservedProperies) yield {
      stationQuery.getObservedProperty(observedProperty.foreign_tag, observedProperty.depth,
        observedProperty.phenomenon_id, source) match {
          case Some(databaseObservedProperty) => {
            databaseObservedProperty
          }
          case None => {
            val newObservedProperties = stationQuery.createObservedProperty(observedProperty)
            LOGGER.info("creating new observedProperties " + observedProperty.foreign_tag)
            newObservedProperties
          }
        }
    }

    return observedProperties
  }

  def createObservedProperty(foreignTag: String, source: Source,
    foreignUnits: String, phenomenonId: Long, depth: Double = 0): ObservedProperty = {
    new ObservedProperty(foreignTag, source.id, foreignUnits,
      phenomenonId, depth)
  }

  def getObservedProperty(phenomenon: Phenomenon,
    foreignTag: String, units: String, source: Source): ObservedProperty = {
    getObservedProperty(phenomenon, foreignTag, units, 0, source)
  }

  def getObservedProperty(phenomenon: Phenomenon,
    foreignTag: String, units: String, depth: Double, source: Source): ObservedProperty = {
    val databasePhenomenon = findDatabasePhenomenon(phenomenon)

    createObservedProperty(foreignTag, source, units, databasePhenomenon.id, depth)
  }

  // ---------------------------------------------------------------------------
  // Private Members
  // ---------------------------------------------------------------------------

  private def findDatabasePhenomenon(phenomenon: Phenomenon): DatabasePhenomenon = {
    stationQuery.getPhenomenon(phenomenon.getId) match {
      case Some(databasePhenomenon) => {
        databasePhenomenon
      }
      case None => {
        insertPhenomenon(phenomenon)
      }
    }
  }

  private def insertPhenomenon(phenomenon: Phenomenon): DatabasePhenomenon = {
    stationQuery.createPhenomenon(
      new DatabasePhenomenon(phenomenon.getId, phenomenon.getUnit().toString()))
  }

  private def findPhenomenon(databasePhenomenon: DatabasePhenomenon): Phenomenon = {
    val option = Phenomena.instance.getAllPhenomena().find(phenomenon => {
      phenomenon.getId() == databasePhenomenon.tag
    })

    if (option.isEmpty) {
      throw new Exception("Did not find Phenomenon: " + databasePhenomenon.tag);
    }

    option.get
  }

  private def isThereAnEqualSensor(originalSensors: List[DatabaseSensor],
    newSensor: DatabaseSensor, newPhenomena: List[DatabasePhenomenon]): Boolean = {
    originalSensors.exists(originalSensor => {
      val originalPhenomena = stationQuery.getPhenomena(originalSensor)

      (newPhenomena.forall(newPhenomenon =>
        originalPhenomena.exists(_.tag == newPhenomenon.tag)) &&
        originalPhenomena.forall(originalPhenomenon =>
          newPhenomena.exists(_.tag == originalPhenomenon.tag)))
    })
  }

  private def areClose(value1: Double, value2: Double): Boolean = {
    val roundedValue1 = value1.round
    val roundedValue2 = value2.round
    return roundedValue1 == roundedValue2
  }

  private def areDoublesEquals(value1: Double, value2: Double): Boolean = {
    val roundedValue1 = (value1 * 1000).round
    val roundedValue2 = (value2 * 1000).round
    return roundedValue1 == roundedValue2
  }
}
