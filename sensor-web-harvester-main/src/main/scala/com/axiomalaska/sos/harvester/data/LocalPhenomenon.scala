package com.axiomalaska.sos.harvester.data

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
  def getUnit() = phenomenon.getUnit()
  def getTag() = phenomenon.getTag()
  
  private def findPhenomenon():Phenomenon = {
    Phenomena.instance.getAllPhenomena().find(phenomenon =>{
        phenomenon.getId() == databasePhenomenon.tag
    }) match{
      case Some(phenomenon) =>{
        phenomenon
      }
      case None =>{
        databasePhenomenon.tag match{
          case CustomPhenomenon(url, name) =>{
             Phenomena.instance().createHomelessParameter(name, url, 
                 databasePhenomenon.units)
          }
          case _ =>{
            throw new Exception("Phenomenon: " + databasePhenomenon.tag + 
                " is not build correctly" )
          }
        }
      }
    }
  }
}