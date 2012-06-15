package com.axiomalaska.sos.source.data

import com.axiomalaska.sos.source.StationQuery
import com.axiomalaska.sos.data.SosSensor
import com.axiomalaska.sos.data.SosPhenomenon

import scala.collection.JavaConversions._

class LocalSensor(val databaseSensor:DatabaseSensor, 
    private val stationQuery:StationQuery) extends SosSensor{
  
  def getId():String ={
    databaseSensor.tag
  }
  
  def getDescription():String ={
    databaseSensor.description
  }
  
  def getPhenomena():java.util.List[SosPhenomenon] = {
    val phenomena = stationQuery.getPhenomena(databaseSensor)
    phenomena.map(phenomenon => new LocalPhenomenon(phenomenon)).toList
  }
}