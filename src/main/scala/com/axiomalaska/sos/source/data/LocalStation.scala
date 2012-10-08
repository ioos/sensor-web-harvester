package com.axiomalaska.sos.source.data

import com.axiomalaska.sos.data.SosSensor
import com.axiomalaska.sos.data.Location
import com.axiomalaska.sos.data.SosStation
import com.axiomalaska.sos.source.StationQuery
import scala.collection.JavaConversions._
import com.axiomalaska.sos.data.SosNetwork
import com.axiomalaska.sos.data.SosSource

class LocalStation(val source:SosSource, 
    val databaseStation: DatabaseStation,
  private val stationQuery: StationQuery) extends SosStation {

  /**
   * A list of phenomena that this station has readings for
   */
  def getSensors(): java.util.List[SosSensor] = {
    val sensors = stationQuery.getActiveSensors(databaseStation)
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
  def getFeatureOfInterestName() =
    "station: " + databaseStation.name + " of source: " + source.getName

  /**
   * The location of the station
   */
  def getLocation() = 
    new Location(databaseStation.latitude, databaseStation.longitude)

  /**
   * A list of networks this station is associated to
   * @return
   */
  def getNetworks(): java.util.List[SosNetwork] = {
    Nil
  }

  def isMoving = false
  
  def getSource() = source
  
  def getName() = databaseStation.name
  
  def getDescription() = databaseStation.description
    
  def getPlatformType() = databaseStation.platformType
}
