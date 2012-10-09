package com.axiomalaska.sos.source.data

import com.axiomalaska.phenomena.Phenomenon
import com.axiomalaska.phenomena.Phenomena
import scala.collection.JavaConversions._

/**
 * A SosPhenomenon built from the DatabasePhenomenon
 */
class LocalPhenomenon(val databasePhenomenon:DatabasePhenomenon) extends Phenomenon {
  
  private val phenomenon = findPhenomenon()
  
  def getName() = phenomenon.getName()
  def getId() = phenomenon.getId()
  def getUnits() = phenomenon.getUnits()
  
  private def findPhenomenon():Phenomenon = {
    val option = Phenomena.getAllPhenomena().find(phenomenon =>{
      val index = phenomenon.getId().lastIndexOf("/") + 1
      val tag = phenomenon.getId().substring(index)
      tag == databasePhenomenon.tag
    })
    
    if(option.isEmpty){
      throw new Exception("Did not find Phenomenon");
    }
    
    option.get
  }
}