package com.axiomalaska.sos.source.observationretriever

import com.axiomalaska.sos.data.SosStation
import com.axiomalaska.sos.data.SosSensor
import com.axiomalaska.sos.data.ObservationCollection
import com.axiomalaska.sos.source.StationQuery
import com.axiomalaska.sos.source.data.LocalStation
import com.axiomalaska.sos.source.data.LocalSensor
import com.axiomalaska.sos.source.data.LocalPhenomenon
import com.axiomalaska.sos.source.data.ObservationValues
import com.axiomalaska.sos.ObservationRetriever
import com.axiomalaska.sos.tools.HttpPart
import com.axiomalaska.sos.tools.HttpSender
import scala.collection.mutable
import scala.collection.JavaConversions._
import java.util.Calendar
import java.util.TimeZone
import java.text.SimpleDateFormat
import org.apache.log4j.Logger

class NoaaWeatherObservationRetriever(private val stationQuery: StationQuery, 
    private val logger: Logger = Logger.getRootLogger())
  extends ObservationValuesCollectionRetriever {

  // ---------------------------------------------------------------------------
  // Private Data
  // ---------------------------------------------------------------------------

  private val httpSender = new HttpSender()
  private val parser = """<tr[^>]*><td[^>]*>([^<]*)</td>[^<]*<td[^>]*>([^<]*)</td>[^<]*<td[^>]*>([^<]*)</td>[^<]*<td[^>]*>([^<]*)</td>[^<]*<td[^>]*>([^<]*)</td>[^<]*<td[^>]*>([^<]*)</td>[^<]*<td[^>]*>([^<]*)</td>[^<]*<td[^>]*>([^<]*)</td>[^<]*<td[^>]*>([^<]*)</td>[^<]*<td[^>]*>([^<]*)</td>[^<]*<td[^>]*>([^<]*)</td>[^<]*<td[^>]*>([^<]*)</td>[^<]*<td[^>]*>([^<]*)</td>[^<]*<td[^>]*>([^<]*)</td>[^<]*<td[^>]*>([^<]*)</td></tr>""".r
  private val lastModifiedParser = """.*<OPTION SELECTED>([^<]*)<OPTION>.*""".r
  private val dateParser = new SimpleDateFormat(" MMM dd, yyyy - hh:mm aa z ")
  private val timezoneParser = """<th rowspan="3" width="32">Time<br>\((.*)\)</th>""".r
  
  // ---------------------------------------------------------------------------
  // ObservationValuesCollectionRetriever Members
  // ---------------------------------------------------------------------------
  
  def getObservationValues(station: LocalStation, sensor: LocalSensor, 
      phenomenon: LocalPhenomenon, startDate: Calendar):List[ObservationValues] ={
    val rawData =
      httpSender.sendGetMessage("http://www.nws.noaa.gov/data/obhistory/" +
        station.databaseStation.foreign_tag + ".html")

    val timezone = timezoneParser.findFirstMatchIn(rawData) match{
      case Some(timezoneMatch) =>{
    	  timezoneMatch.group(1).toUpperCase
      }
      case None => "UTC"
    }
    
    val observationValuesCollection =
      createSensorObservationValuesCollection(station, sensor, phenomenon)

    for (patternMatch <- parser.findAllIn(rawData)) {
      val parser(day, time, wind, vis, weather, skyCond, air, dwpt, max, min,
        altimeter, seaLevel, prec1, prec3, prec6) = patternMatch

      val calendar = createDate(day, time, timezone)
      if (calendar.after(startDate)) {
        for (observationValue <- observationValuesCollection) {
          observationValue.observedProperty.foreign_tag match {
            case "Wind Speed" => {
              getWindSpeedAndDirection(wind) match {
                case Some((speed, direction)) => {
                  observationValue.addValue(speed, calendar)
                }
                case None => //do nothing
              }
            }
            case "Wind Direction" => {
              getWindSpeedAndDirection(wind) match {
                case Some((speed, direction)) => {
                  observationValue.addValue(direction, calendar)
                }
                case None => //do nothing
              }
            }
            case "Temperature" => {
              pareseDouble(air) match {
                case Some(value) => {
                  observationValue.addValue(value, calendar)
                }
                case None => //do nothing
              }
            }
            case "Dew Point" => {
              pareseDouble(dwpt) match {
                case Some(value) => {
                  observationValue.addValue(value, calendar)
                }
                case None => //do nothing
              }
            }
            case "Pressure" => {
              pareseDouble(altimeter) match {
                case Some(value) => {
                  observationValue.addValue(value, calendar)
                }
                case None => //do nothing
              }
            }
          }
        }
      }
    }

    return observationValuesCollection
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
  
  private def pareseDouble(rawValue: String): Option[Double] = {
    rawValue match {
      case "NA" => None
      case x: String => Some(x.toDouble)
    }
  }

  private def getWindSpeedAndDirection(rawText: String): Option[(Double, Double)] = {
    rawText match {
      case "NA" => None
      case _ => Some((getWindSpeed(rawText), getWindDirection(rawText)))
    }
  }

  private def getWindSpeed(rawText: String): Double = {
    rawText match {
      case "Calm" => {
        0
      }
      case s: String => {
        s.split(" ")(1).toDouble
      }
    }
  }

  private def getWindDirection(rawText: String): Double = {

    rawText match {
      case "Calm" => {
        0
      }
      case s: String => {
        s.split(" ")(0) match {
          case "N" => 0
          case "Vrbl" => 0
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
    }
  }

  private def createDate(dayRaw: String, timeRaw: String, timezone:String): Calendar = {
    val calendar = Calendar.getInstance(TimeZone.getTimeZone(timezone))
    val hour = timeRaw.split(":")(0).toInt
    val mins = timeRaw.split(":")(1).toInt
    val day = dayRaw.toInt

    val (month, year) = if (calendar.get(Calendar.DAY_OF_MONTH) < day) {
      calendar.get(Calendar.MONTH) match {
        case 0 => (11, calendar.get(Calendar.YEAR) - 1)
        case x: Int => (x - 1, calendar.get(Calendar.YEAR))
      }
    } else {
      (calendar.get(Calendar.MONTH), calendar.get(Calendar.YEAR))
    }

    calendar.set(Calendar.YEAR, year)
    calendar.set(Calendar.MONTH, month)
    calendar.set(Calendar.DAY_OF_MONTH, day)
    calendar.set(Calendar.HOUR_OF_DAY, hour)
    calendar.set(Calendar.MINUTE, mins)
    calendar.set(Calendar.SECOND, 0)

    // The time is not able to be changed from the timezone if this is not set. 
    calendar.getTime()

    return calendar
  }
}