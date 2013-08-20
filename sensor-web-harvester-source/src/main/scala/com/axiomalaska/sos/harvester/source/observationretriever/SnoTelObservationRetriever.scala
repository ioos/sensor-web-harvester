package com.axiomalaska.sos.harvester.source.observationretriever

import java.util.TimeZone
import scala.Array.canBuildFrom
import scala.collection.JavaConversions.seqAsJavaList
import org.apache.log4j.Logger
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.joda.time.format.DateTimeFormat
import com.axiomalaska.sos.harvester.StationQuery
import com.axiomalaska.sos.harvester.source.SourceUrls
import com.axiomalaska.sos.tools.HttpPart
import com.axiomalaska.sos.tools.HttpSender
import javax.measure.Measure
import javax.measure.unit.NonSI
import javax.measure.unit.SI
import com.axiomalaska.sos.harvester.data.LocalStation
import com.axiomalaska.sos.harvester.data.LocalSensor
import com.axiomalaska.sos.harvester.data.LocalPhenomenon
import com.axiomalaska.sos.harvester.data.ObservationValues

class SnoTelObservationRetriever(private val stationQuery: StationQuery)
  extends ObservationValuesCollectionRetriever {

  // ---------------------------------------------------------------------------
  // Private Data
  // ---------------------------------------------------------------------------
  private val LOGGER = Logger.getLogger(getClass())
  private val pstTimeZone = DateTimeZone.forTimeZone(TimeZone.getTimeZone("PST"))
  private val parseDate = DateTimeFormat.forPattern("yyyy-MM-ddHH:mm")
            .withZone(pstTimeZone)
  private val httpSender = new HttpSender()

  // ---------------------------------------------------------------------------
  // ObservationValuesCollectionRetriever Members
  // ---------------------------------------------------------------------------

  def getObservationValues(station: LocalStation, sensor: LocalSensor,
    phenomenon: LocalPhenomenon, startDate: DateTime): List[ObservationValues] = {

    LOGGER.info("SNO-TEL: Collecting for station - " + station.databaseStation.foreign_tag)

    val observationValuesCollection =
      createSensorObservationValuesCollection(station, sensor, phenomenon)

    val data = getRawData(station)

    if (data == null) {
      return Nil
    }

    val lines = data.split("\n").dropWhile(!_.startsWith("Site Id"))

    val headerLine = createHeaders(lines.head.split(","))
    var preivousDateOption: Option[DateTime] = None

    for {
      line <- lines.tail
      val values = line.split(",");
      val calendar = createDate(values(1), values(2))
      if (calendar.isAfter(startDate) &&
        (preivousDateOption.isEmpty || !areDatesEqual(preivousDateOption.get, calendar)))
    } {
      preivousDateOption = Some(calendar)
      for {
        columnIndex <- 3 until values.length
        val header = headerLine.get(columnIndex)
      } {

        parseDouble(values(columnIndex), header) match {
          case Some(value) => {
            observationValuesCollection.find(observationValues =>
              isObservationValue(observationValues, header._1, header._2)) match {
              case Some(observationValues) if (!observationValues.containsDate(calendar)) => {
                observationValues.addValue(value, calendar)
              }
              case _ => //do nothing
            }
          }
          case None => //do nothing
        }
      }
    }

    return observationValuesCollection
  }
  
  // ---------------------------------------------------------------------------
  // Private Members
  // ---------------------------------------------------------------------------

  private def areDatesEqual(date1:DateTime, date2:DateTime):Boolean ={
    date1.getYear() == date2.getYear() && 
    date1.getMonthOfYear() == date2.getMonthOfYear() && 
    date1.getDayOfMonth() == date2.getDayOfMonth() && 
    date1.getHourOfDay() == date2.getHourOfDay()
  }
  
  private def isObservationValue(observationValues:ObservationValues, 
      foreign_tag:String, depth:Double):Boolean ={
    val areEqual = observationValues.observedProperty.foreign_tag.equalsIgnoreCase(foreign_tag) && 
    observationValues.observedProperty.depth == depth
    
    areEqual
  }
  
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

    return httpSender.sendPostMessage(SourceUrls.SNOTEL_OBSERVATION_RETRIEVAL, parts)
  }

  private def parseDouble(text: String, headerName: (String, Double)): Option[Double] = {
    val value = if (!text.equalsIgnoreCase("miss'g")) {
      val value = text.toDouble
      if (value == -99.9) {
        None
      } else if (headerName._1 == "SNWD" && value < 0) {
        None
      } else {
        Some(text.toDouble)
      }
    } else {
      None
    }
    
    value
  }

  private def createDate(dayRawText: String, timeRawText: String) = 
    parseDate.parseDateTime(dayRawText + timeRawText)

  /**
   * return (name, depth)
   */
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
        
        (base, depth * (-1))
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
