package com.axiomalaska.sos.harvester.source.observationretriever

import java.util.Calendar
import java.util.TimeZone

import org.apache.log4j.Logger
import org.joda.time.DateTime
import org.joda.time.DateTimeZone

import com.axiomalaska.sos.harvester.StationQuery;
import com.axiomalaska.sos.harvester.source.SourceUrls
import com.axiomalaska.sos.harvester.data.LocalPhenomenon
import com.axiomalaska.sos.harvester.data.LocalSensor
import com.axiomalaska.sos.harvester.data.LocalStation
import com.axiomalaska.sos.harvester.data.ObservationValues
import com.axiomalaska.sos.tools.HttpSender

class NoaaWeatherObservationRetriever(private val stationQuery: StationQuery)
  extends ObservationValuesCollectionRetriever {

  // ---------------------------------------------------------------------------
  // Private Data
  // ---------------------------------------------------------------------------
  private val LOGGER = Logger.getLogger(getClass())
  private val parser = """<tr[^>]*><td[^>]*>([^<]*)</td>[^<]*<td[^>]*>([^<]*)</td>[^<]*<td[^>]*>([^<]*)</td>[^<]*<td[^>]*>([^<]*)</td>[^<]*<td[^>]*>([^<]*)</td>[^<]*<td[^>]*>([^<]*)</td>[^<]*<td[^>]*>([^<]*)</td>[^<]*<td[^>]*>([^<]*)</td>[^<]*<td[^>]*>([^<]*)</td>[^<]*<td[^>]*>([^<]*)</td>[^<]*<td[^>]*>([^<]*)</td>[^<]*<td[^>]*>([^<]*)</td>[^<]*<td[^>]*>([^<]*)</td>[^<]*<td[^>]*>([^<]*)</td>[^<]*<td[^>]*>([^<]*)</td></tr>""".r
  private val lastModifiedParser = """.*<OPTION SELECTED>([^<]*)<OPTION>.*""".r
  private val timezoneParser = """<th rowspan="3" width="32">Time<br>\((.*)\)</th>""".r
  
  // ---------------------------------------------------------------------------
  // ObservationValuesCollectionRetriever Members
  // ---------------------------------------------------------------------------
  
  def getObservationValues(station: LocalStation, sensor: LocalSensor, 
      phenomenon: LocalPhenomenon, startDate: DateTime):List[ObservationValues] ={

    LOGGER.info("NOOA-WEATHER: Collecting for station - " + 
        station.databaseStation.foreign_tag)
    
    val rawData =
      HttpSender.sendGetMessage(SourceUrls.NOAA_WEATHER_OBSERVATION_RETRIEVAL +
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
      if (calendar.isAfter(startDate)) {
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

  private def createDate(dayRaw: String, timeRaw: String, timezone:String): DateTime = {
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

    new DateTime(year, month+1, day, hour, mins, DateTimeZone.forTimeZone(TimeZone.getTimeZone(timezone)))
  }
}
