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
import javax.measure.Measure
import javax.measure.unit.NonSI
import javax.measure.unit.SI

import org.apache.log4j.Logger

class SnoTelObservationRetriever(private val stationQuery: StationQuery, 
    private val logger: Logger = Logger.getRootLogger())
  extends ObservationValuesCollectionRetriever {

  // ---------------------------------------------------------------------------
  // Private Data
  // ---------------------------------------------------------------------------
  
  private val parseDate = new SimpleDateFormat("yyyy-MM-ddHH:mm")
  private val httpSender = new HttpSender()
  
  // ---------------------------------------------------------------------------
  // ObservationValuesCollectionRetriever Members
  // ---------------------------------------------------------------------------
  
  def getObservationValues(station: LocalStation, sensor: LocalSensor, 
      phenomenon: LocalPhenomenon, startDate: Calendar):List[ObservationValues] ={
    val observationValuesCollection =
      createSensorObservationValuesCollection(station, sensor, phenomenon)

    val data = getRawData(station)
    
    if(data == null){
      return Nil
    }

    val lines = data.split("\n");

    val headerLine = createHeaders(lines(1).split(","))

    for (index <- 2 until lines.length) {
      val line = lines(index)
      val values = line.split(",");
      val calendar = createDate(values(1), values(2))
      if (calendar.after(startDate)) {
        for (columnIndex <- 3 until values.length) {
          val header = headerLine.get(columnIndex)

          parseDouble(values(columnIndex), header) match {
            case Some(value) => {
              observationValuesCollection.find(observationValues => 
                observationValues.observedProperty.foreign_tag.equalsIgnoreCase(header._1) && 
                observationValues.observedProperty.depth == header._2) match {
                case Some(observationValues) 
                	if(!observationValues.containsDate(calendar)) => {
                  observationValues.addValue(value, calendar)
                }
                case _ => //do nothing
              }
            }
            case None => //do nothing
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
  
  private def getRawData(station: LocalStation): String = {
    val parts = List[HttpPart](
      new HttpPart("time_zone", "PST"),
      new HttpPart("sitenum", station.databaseStation.foreign_tag),
      new HttpPart("timeseries", "Hourly"),
      new HttpPart("interval", "WEEK"),
      new HttpPart("format", "copy"),
      new HttpPart("report", "ALL"))

    return httpSender.sendPostMessage(
      "http://www.wcc.nrcs.usda.gov/nwcc/view", parts)
  }

  private def parseDouble(text: String, headerName: (String, Double)): Option[java.lang.Double] = {
    if (!text.equalsIgnoreCase("miss'g")) {
      val value = text.toDouble
      if (value == -99.9) {
        return None
      } else if (headerName._1 == "SNWD" && value < 0) {
        return None
      } else {
        return Some(text.toDouble)
      }
    } else {
      return None
    }
  }

  private def createDate(dayRawText: String, timeRawText: String): Calendar = {
    val date = parseDate.parse(dayRawText + timeRawText)
    val calendar = Calendar.getInstance(TimeZone
      .getTimeZone("PST"))
    calendar.set(Calendar.YEAR, date.getYear() + 1900)
    calendar.set(Calendar.MONTH, date.getMonth())
    calendar.set(Calendar.DAY_OF_MONTH, date.getDate())
    calendar.set(Calendar.HOUR_OF_DAY, date.getHours())
    calendar.set(Calendar.MINUTE, date.getMinutes())
    calendar.set(Calendar.SECOND, 0)

    // The time is not able to be changed from the 
    //setTimezone if this is not set. Java Error
    calendar.getTime()

    return calendar
  }

  private def createHeaders(headers: Array[String]): List[(String, Double)] = {
    val formatedHeaders = for (originalHeader <- headers) yield {
      val index = originalHeader.indexOf(".")

      val (base, depth) = if (index != -1) {
        val base = originalHeader.substring(0, index)

        val depthIndex = originalHeader.indexOf(":")
        val depth = if (depthIndex > 0) {
          val valueRaw = originalHeader.substring(depthIndex + 1, originalHeader.size).replaceFirst("\\(.*\\)", "")
          val valueInches = valueRaw.toDouble
          val valueMeters = Measure.valueOf(valueInches,
            NonSI.INCH).doubleValue(SI.METER).abs
          valueMeters
        } else {
          0.0
        }
        
        (base, depth)
      } else {
        var header = originalHeader.replace(".", "")
        header = header.replace("-", "")
        header = header.replace(":", "")
        var split = header.split(" ")

        (split(0), 0.0)
      }
      (base, depth)
    }

    return formatedHeaders.toList
  }
}