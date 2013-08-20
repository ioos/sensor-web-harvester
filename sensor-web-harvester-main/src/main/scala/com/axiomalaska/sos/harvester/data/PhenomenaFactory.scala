package com.axiomalaska.sos.harvester.data

import com.axiomalaska.phenomena.Phenomena
import scala.collection.JavaConversions._
import com.axiomalaska.phenomena.Phenomenon
import com.axiomalaska.sos.harvester.Units;
import com.axiomalaska.sos.harvester.Units

class PhenomenaFactory {

  def findPhenomenon(databasePhenomenon:DatabasePhenomenon):Phenomenon ={
    Phenomena.instance.getAllPhenomena().find(phenomenon =>{
        phenomenon.getId() == databasePhenomenon.tag
    }) match{
      case Some(phenomenon) =>{
        phenomenon
      }
      case None =>{
        findCustomPhenomenon(databasePhenomenon.tag)
      }
    }
  }

  def findCustomPhenomenon(url: String): Phenomenon = {
    url match {
      case CustomGlosPhenomenon(_, "stream_gage_height") => {
        Phenomena.instance.createHomelessParameter("stream_gage_height",
          Phenomena.GENERIC_FAKE_MMI_URL_PREFIX, Units.METERS)
      }
      case CustomGlosPhenomenon(_, "ground_temperature_observed") => {
        Phenomena.instance.createHomelessParameter(
          "ground_temperature_observed",
          Phenomena.GENERIC_FAKE_MMI_URL_PREFIX,
          Units.FAHRENHEIT)
      }
      case CustomGlosPhenomenon(_, "water_level") => {
        Phenomena.instance.createHomelessParameter("water_level",
          Phenomena.GENERIC_FAKE_MMI_URL_PREFIX, Units.FEET)
      }
      case CustomGlosPhenomenon(_, "sun_radiation") => {
        Phenomena.instance.createHomelessParameter("sun_radiation",
          Phenomena.GENERIC_FAKE_MMI_URL_PREFIX, "rads")
      }
      case CustomGlosPhenomenon(_, "sea_level_pressure") =>{
        Phenomena.instance.createHomelessParameter("sea_level_pressure", 
            Phenomena.GENERIC_FAKE_MMI_URL_PREFIX, Units.HECTOPASCAL)
      }
      case CustomGlosPhenomenon(_, "stream_flow") =>{
        Phenomena.instance.createHomelessParameter(
                "stream_flow", Units.CUBIC_METER_PER_SECOUND)
      }
      case _ => {
        throw new Exception("Did not find Phenomenon - " + url)
      }
    }
  }
}

object CustomPhenomenon{
  def unapply(url:String): Option[(String, String)] ={
    val index = url.lastIndexOf("/")
    if (index > 0) {

      val parts = url.splitAt(index + 1)

      if (parts._1.startsWith("http://")) {
        Some(parts)
      } else {
        None
      }
    } else {
      None
    }
  }
}

object CustomGlosPhenomenon{
  def unapply(url:String): Option[(String, String)] ={
    url match{
      case CustomPhenomenon(Phenomena.GENERIC_FAKE_MMI_URL_PREFIX, name) =>{
        Some((Phenomena.GENERIC_FAKE_MMI_URL_PREFIX, name))
      }
      case _ => None
    }
  }
}
