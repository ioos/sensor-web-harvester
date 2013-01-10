package com.axiomalaska.sos.source.data

import com.axiomalaska.phenomena.Phenomenon
import com.axiomalaska.phenomena.Phenomena
import com.axiomalaska.sos.source.StationQuery
import scala.collection.JavaConversions._

/**
 * A SosPhenomenon built from the DatabasePhenomenon
 */
class LocalPhenomenon(val databasePhenomenon:DatabasePhenomenon, private val stationQuery:StationQuery = null) extends Phenomenon {
  
  private val phenomenon = findPhenomenon()
  
  val getDatabasePhenomenon = findDatabasePhenomenon()
  
  def getName() = phenomenon.getName()
  def getId() = phenomenon.getId()
  def getUnit() = phenomenon.getUnit()
  
  private def findPhenomenon():Phenomenon = {
    val option = Phenomena.instance.getAllPhenomena().find(phenomenon =>{
        phenomenon.getId() == databasePhenomenon.tag
    })
  
    if(option.isEmpty){
      throw new Exception("Did not find Phenomenon - " + databasePhenomenon.id + " - " + databasePhenomenon.tag);
    }
    
    option.get
  }
  
  private def findDatabasePhenomenon():DatabasePhenomenon = {
    if (stationQuery == null || phenomenon == null)
      null
    else
      try {
        return stationQuery.getPhenomenon(phenomenon.getId)
      } catch {
        case ex: Exception => {
            null
        }
      }
  }
}