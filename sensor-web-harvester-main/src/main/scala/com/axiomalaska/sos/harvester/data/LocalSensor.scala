package com.axiomalaska.sos.harvester.data

import com.axiomalaska.phenomena.Phenomenon
import com.axiomalaska.sos.data.SosNetwork
import com.axiomalaska.sos.data.SosSensor
import scala.collection.JavaConversions._
import org.n52.sos.ioos.asset.SensorAsset
import com.axiomalaska.sos.data.SosStation
import com.axiomalaska.sos.harvester.StationQuery

class LocalSensor(
  val databaseSensor: DatabaseSensor,
  private val station:SosStation,
  private val stationQuery: StationQuery) extends SosSensor {

  setStation(station)
  
  setPhenomena(getLocalPhenomena)
      
  setAsset(new SensorAsset(station.getAsset(), databaseSensor.tag))
  
  private def getLocalPhenomena():List[LocalPhenomenon] = {
    val phenomena = stationQuery.getPhenomena(databaseSensor).map(
      phenomenon => new LocalPhenomenon(phenomenon)).toList
    phenomena
  }
  
}