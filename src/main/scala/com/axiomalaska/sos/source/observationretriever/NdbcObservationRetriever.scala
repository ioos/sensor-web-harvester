package com.axiomalaska.sos.source.observationretriever

import com.axiomalaska.sos.source.StationQuery
import com.axiomalaska.sos.ObservationRetriever
import com.axiomalaska.sos.data.SosStation
import java.util.Calendar
import com.axiomalaska.sos.data.ObservationCollection
import com.axiomalaska.sos.data.SosSensor
import com.axiomalaska.sos.source.data.LocalStation
import com.axiomalaska.sos.source.data.DatabaseSensor
import com.axiomalaska.sos.source.data.DatabasePhenomenon
import java.text.SimpleDateFormat
import com.axiomalaska.sos.tools.HttpSender
import com.axiomalaska.sos.source.data.DatabaseStation
import com.axiomalaska.sos.source.data.LocalSensor
import com.axiomalaska.sos.source.data.LocalPhenomenon
import java.util.TimeZone
import com.axiomalaska.sos.source.data.ObservationValues
import scala.collection.JavaConversions._

import org.apache.log4j.Logger

class NdbcObservationRetriever(private val stationQuery:StationQuery, 
    private val logger: Logger = Logger.getRootLogger())
	extends ObservationValuesCollectionRetriever {
  
  private val valueParser = """(\d{4})\s+(\d{2})\s+(\d{2})\s+(\d{2})\s+(\d{2})\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\n""".r
  private val specParser = """(\d{4})\s+(\d{2})\s+(\d{2})\s+(\d{2})\s+(\d{2})\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\n""".r
  
  private val directionParser = """.*\((\d*).*\).*""".r
  private val valueNoUnitsParser = """(\d*.?\d*).*?""".r
  private val parseDate = new SimpleDateFormat("HHmm 'GMT' MM/dd/yy")
  private val httpSender = new HttpSender()
  
  // ---------------------------------------------------------------------------
  // ObservationRetriever Members
  // ---------------------------------------------------------------------------

  def getObservationValues(station: LocalStation, sensor: LocalSensor, 
      phenomenon: LocalPhenomenon, startDate: Calendar):List[ObservationValues] = {
    
    val data = httpSender.downloadReadFile("http://www.ndbc.noaa.gov/data/5day2/" + 
        station.databaseStation.foreign_tag + "_5day.txt")
         
    val observationValuesCollection = 
      createSensorObservationValuesCollection(station, sensor, phenomenon)
      
    for (matchingPattern <- valueParser.findAllIn(data)) {
      val valueParser(year, month, day, hour, min, wdir, wspd, gst, wvht, dpd, apd,
        mwd, pres, atmp, wtmp, dewp, vis, ptdy, tide) = matchingPattern
      
      val date = createDate(year.toInt, month.toInt, day.toInt, hour.toInt, min.toInt)
      if (date.after(startDate)) {
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
      val specResults = httpSender.downloadReadFile(
        "http://www.ndbc.noaa.gov/data/5day2/" + station.databaseStation.foreign_tag + "_5day.spec")

      for (matchingPattern <- specParser.findAllIn(specResults.replace("2011", ";2011"))) {
        val specParser(year, month, day, hour, min, wvht, swh, swp, wwh,
          wwp, swd, wwd, steepness, apd, mwd) = matchingPattern

        val date = createDate(year.toInt, month.toInt, day.toInt, hour.toInt, min.toInt)
        if (date.after(startDate)) {
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
      new ObservationValues(observedProperty, sensor, phenomenon, observedProperty.foreign_units)
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
  
  private def createDate(year: Int, month: Int, day: Int, hour: Int, min: Int): Calendar = {
    val calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"))
    calendar.set(Calendar.YEAR, year)
    calendar.set(Calendar.MONTH, month - 1)
    calendar.set(Calendar.DAY_OF_MONTH, day)
    calendar.set(Calendar.HOUR_OF_DAY, hour)
    calendar.set(Calendar.MINUTE, min)
    calendar.set(Calendar.SECOND, 0)

    // The time is not able to be changed from the timezone if this is not set. 
    calendar.getTime()

    return calendar
  }
  
  private def shouldReadWaveFile(station:LocalStation):Boolean ={
    httpSender.doesUrlExists("http://www.ndbc.noaa.gov/data/5day2/" + station.databaseStation.foreign_tag + "_5day.spec")
  }
}