package com.axiomalaska.sos.source.data

import com.axiomalaska.sos.data.SosSensor
import com.axiomalaska.sos.data.Location
import com.axiomalaska.sos.data.SosStation
import com.axiomalaska.sos.data.SosPhenomenon
import com.axiomalaska.sos.source.StationQuery
import scala.collection.JavaConversions._
import com.axiomalaska.sos.data.SosNetwork

class LocalStation(val source:Source, 
    val databaseStation: DatabaseStation,
  private val stationQuery: StationQuery) extends SosStation {

  /**
   * A list of phenomena that this station has readings for
   */
  def getSensors(): java.util.List[SosSensor] = {
    val sensors = stationQuery.getSensors(databaseStation)
    sensors.map(sensor => new LocalSensor(sensor, stationQuery))
  }

  /**
   * This ID should be unique for each station. For example '11111'
   */
  def getId() = databaseStation.tag

  /**
   * The default name of the location with which the station takes its
   * reading from.
   *
   * Maximum characters 80
   *
   * If characters are over 100 they will be truncated to 80
   */
  def getFeatureOfInterestName(): String = {
    "station: " + databaseStation.name + " of source: " + source.name
  }

  /**
   * The location of the station
   */
  def getLocation(): Location = {
    new Location(databaseStation.latitude, databaseStation.longitude)
  }

  /**
   * Name of the source for the station.
   *
   * @return
   */
  def getSourceId() = source.tag

  /**
   * A list of networks this station is associated to
   * @return
   */
  def getNetworks(): java.util.List[SosNetwork] = {
    Nil
  }

  def isMoving = false
}
