package com.axiomalaska.sos.harvester.source.observationretriever

import org.apache.log4j.Logger
import org.joda.time.DateTime
import org.joda.time.DateTimeZone

import com.axiomalaska.sos.harvester.StationQuery;
import com.axiomalaska.sos.harvester.data.LocalPhenomenon
import com.axiomalaska.sos.harvester.data.LocalSensor
import com.axiomalaska.sos.harvester.data.LocalStation
import com.axiomalaska.sos.harvester.data.ObservationValues
import com.axiomalaska.sos.tools.HttpSender

/***
 * Currently not used. Has been replace with the NDBC SOS retriever
 */
class NdbcObservationRetriever(private val stationQuery:StationQuery)
	extends ObservationValuesCollectionRetriever {
  
  private val valueParser = """(\d{4})\s+(\d{2})\s+(\d{2})\s+(\d{2})\s+(\d{2})\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\n""".r
  private val specParser = """(\d{4})\s+(\d{2})\s+(\d{2})\s+(\d{2})\s+(\d{2})\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\n""".r
  
  private val LOGGER = Logger.getLogger(getClass())
  private val directionParser = """.*\((\d*).*\).*""".r
  private val valueNoUnitsParser = """(\d*.?\d*).*?""".r
  private val gmtTimeZone = DateTimeZone.forID("GMT")
  
  // ---------------------------------------------------------------------------
  // ObservationRetriever Members
  // ---------------------------------------------------------------------------

  def getObservationValues(station: LocalStation, sensor: LocalSensor, 
      phenomenon: LocalPhenomenon, startDate: DateTime):List[ObservationValues] = {

    LOGGER.info("NDBC: Collecting for station - " + station.databaseStation.foreign_tag)
    
    val data = HttpSender.downloadReadFile("http://www.ndbc.noaa.gov/data/5day2/" + 
        station.databaseStation.foreign_tag + "_5day.txt")
         
    val observationValuesCollection = 
      createSensorObservationValuesCollection(station, sensor, phenomenon)
      
    for (matchingPattern <- valueParser.findAllIn(data)) {
      val valueParser(year, month, day, hour, min, wdir, wspd, gst, wvht, dpd, apd,
        mwd, pres, atmp, wtmp, dewp, vis, ptdy, tide) = matchingPattern
      
      val date = new DateTime(year.toInt, month.toInt, day.toInt, 
            hour.toInt, min.toInt, 0, gmtTimeZone)
      if (date.isAfter(startDate)) {
        for (observationValue <- observationValuesCollection) {
          observationValue.observedProperty.foreign_tag match {
            case "WDIR" if (validValue(wdir)) => {
              observationValue.addValue(wdir.toDouble, date)
            }
            case "WSPD" if (validValue(wspd)) => {
              observationValue.addValue(wspd.toDouble, date)
            }
            case "GST" if (validValue(gst)) => {
              observationValue.addValue(gst.toDouble, date)
            }
            case "WVHT" if(validValue(wvht))=> {
            	observationValue.addValue(wvht.toDouble, date)
            }
            case "DPD" if (validValue(dpd)) => {
              observationValue.addValue(dpd.toDouble, date)
            }
            case "PRES" if (validValue(pres)) => {
              observationValue.addValue(pres.toDouble, date)
            }
            case "ATMP" if (validValue(atmp)) => {
              observationValue.addValue(atmp.toDouble, date)
            }
            case "WTMP" if (validValue(wtmp)) => {
              observationValue.addValue(wtmp.toDouble, date)
            }
            case "DEWP" if (validValue(dewp)) => {
              observationValue.addValue(dewp.toDouble, date)
            }
            case "TIDE" if (validValue(tide)) => {
              observationValue.addValue(tide.toDouble, date)
            }
            case _ => //do nothing
          }
        }
      }
    }

    if (shouldReadWaveFile(station)) {
      val specResults = HttpSender.downloadReadFile(
        "http://www.ndbc.noaa.gov/data/5day2/" + station.databaseStation.foreign_tag + "_5day.spec")

      for (matchingPattern <- specParser.findAllIn(specResults.replace("2011", ";2011"))) {
        val specParser(year, month, day, hour, min, wvht, swh, swp, wwh,
          wwp, swd, wwd, steepness, apd, mwd) = matchingPattern

        val date = new DateTime(year.toInt, month.toInt, day.toInt, 
            hour.toInt, min.toInt, 0, gmtTimeZone)
        
        if (date.isAfter(startDate)) {
          for (observationValues <- observationValuesCollection) {
            observationValues.observedProperty.foreign_tag match {
//              case "WVHT" if (validValue(wvht)) => {
//                sensorObservationValue.addValue(wvht.toDouble, date)
//              }
              case "SwH" if (validValue(swh)) => {
                observationValues.addValue(swh.toDouble, date)
              }
              case "SwP" if (validValue(swp)) => {
                observationValues.addValue(swp.toDouble, date)
              }
              case "WWH" if (validValue(wwh)) => {
                observationValues.addValue(wwh.toDouble, date)
              }
              case "WWP" if (validValue(wwp)) => {
                observationValues.addValue(wwp.toDouble, date)
              }
              case "SwD" if (validValue(swd)) => {
                observationValues.addValue(parseDirection(swd), date)
              }
              case "WWD" if (validValue(wwd)) => {
                observationValues.addValue(parseDirection(wwd), date)
              }
              case "MWD" if (validValue(mwd)) => {
                observationValues.addValue(mwd.toDouble, date)
              }
              case "APD" if (validValue(apd)) => {
                observationValues.addValue(apd.toDouble, date)
              }
              case _ => //do nothing
            }
          }
        }
      }
    }
    
    observationValuesCollection
  }
  
  // ---------------------------------------------------------------------------
  // Private Members
  // ---------------------------------------------------------------------------

  private def createSensorObservationValuesCollection(station: LocalStation,
    sensor: LocalSensor, phenomenon: LocalPhenomenon): List[ObservationValues] = {
    val observedProperties = stationQuery.getObservedProperties(
      station.databaseStation, sensor.databaseSensor,
      phenomenon.databasePhenomenon)

    for (observedProperty <- observedProperties) yield {
      new ObservationValues(observedProperty, sensor, 
          phenomenon, observedProperty.foreign_units)
    }
  }
  
  private def validValue(value: String): Boolean = {
    value != "MM" && value != "-99" && value != "N/A"
  }
  
  private def parseDirection(value: String): Double = {
    value match {
      case "N" => 0
      case "NNE" => 22.5
      case "NE" => 45.5
      case "ENE" => 67.5
      case "E" => 90.0
      case "ESE" => 112.5
      case "SE" => 135.0
      case "SSE" => 157.5
      case "S" => 180.0
      case "SSW" => 202.5
      case "SW" => 225.0
      case "WSW" => 247.5
      case "W" => 270.0
      case "WNW" => 292.50
      case "NW" => 315.0
      case "NNW" => 337.5
    }
  }
  
  private def shouldReadWaveFile(station:LocalStation):Boolean ={
    HttpSender.doesUrlExist(
        "http://www.ndbc.noaa.gov/data/5day2/" + 
        station.databaseStation.foreign_tag + "_5day.spec")
  }
}
