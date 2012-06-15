package com.axiomalaska.sos.source.observationretriever

import com.axiomalaska.sos.data.SosStation
import com.axiomalaska.sos.data.SosPhenomenon
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

class SnoTelObservationRetriever(private val stationQuery: StationQuery)
  extends ObservationRetriever {

  // ---------------------------------------------------------------------------
  // Private Data
  // ---------------------------------------------------------------------------
  
  private val parseDate = new SimpleDateFormat("yyyy-MM-ddHH:mm")
  private val httpSender = new HttpSender()
  
  // ---------------------------------------------------------------------------
  // ObservationRetriever Members
  // ---------------------------------------------------------------------------

  override def getObservationCollection(station: SosStation,
    sensor: SosSensor, phenomenon: SosPhenomenon, startDate: Calendar): ObservationCollection = {

    (station, sensor, phenomenon) match {
      case (localStation: LocalStation, localSensor: LocalSensor, localPhenomenon: LocalPhenomenon) => {
        buildObservationCollection(localStation, localSensor, localPhenomenon, startDate)
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Private Members
  // ---------------------------------------------------------------------------

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

  private def buildObservationCollection(station: LocalStation,
    sensor: LocalSensor, phenomenon: LocalPhenomenon, startDate: Calendar): ObservationCollection = {
    val observationValuesCollection =
      createSensorObservationValuesCollection(station, sensor, phenomenon)

    val data = getRawData(station)

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
              observationValuesCollection.find(_.observedProperty.foreign_tag.equalsIgnoreCase(header)) match {
                case Some(observationValues) => {
                  observationValues.addValue(value, calendar)
                }
                case None => //do nothing
              }
            }
            case None => //do nothing
          }
        }
      }
    }

    return createObservationCollection(station, observationValuesCollection)
  }

  private def createObservationCollection(station: LocalStation,
    observationValuesCollection: List[ObservationValues]): ObservationCollection = {
    val filteredObservationValuesCollection = observationValuesCollection.filter(_.getDates.size > 0)

    if (filteredObservationValuesCollection.size == 1) {
      val observationValues = filteredObservationValuesCollection.head
      val observationCollection = new ObservationCollection()
      observationCollection.setObservationDates(observationValues.getDates)
      observationCollection.setObservationValues(observationValues.getValues)
      observationCollection.setPhenomenon(observationValues.phenomenon)
      observationCollection.setSensor(observationValues.sensor)
      observationCollection.setStation(station)

      observationCollection
    } else {
      println("Error more than one observationValues")
      return null
    }
  }

  private def createSensorObservationValuesCollection(station: LocalStation,
    sensor: LocalSensor, phenomenon: LocalPhenomenon): List[ObservationValues] = {
    val observedProperties = stationQuery.getObservedProperties(
      station.databaseStation, sensor.databaseSensor,
      phenomenon.databasePhenomenon)

    for (observedProperty <- observedProperties) yield {
      new ObservationValues(observedProperty, sensor, phenomenon)
    }
  }

  private def parseDouble(text: String, headerName: String): Option[java.lang.Double] = {
    if (!text.equalsIgnoreCase("miss'g")) {
      val value = text.toDouble
      if (value == -99.9) {
        return None
      } else if (headerName == "SNWDI1" && value < 0) {
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

  private def createHeaders(headers: Array[String]): List[String] = {
    val formatedHeaders = for (originalHeader <- headers) yield {
      var header = originalHeader.replace(".", "")
      header = header.replace("-", "")
      header = header.replace(":", "")
      var split = header.split(" ")

      split(0)
    }

    return formatedHeaders.toList
  }
}