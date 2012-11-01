package com.axiomalaska.sos.source.observationretriever

import java.util.Calendar
import java.util.TimeZone
import java.util.Date
import java.text.SimpleDateFormat

import scala.collection.mutable
import scala.collection.JavaConversions._

import org.jsoup.Jsoup

import com.axiomalaska.sos.ObservationRetriever
import com.axiomalaska.sos.data.ObservationCollection
import com.axiomalaska.sos.data.SosSensor
import com.axiomalaska.sos.data.SosStation
import com.axiomalaska.sos.tools.HttpPart
import com.axiomalaska.sos.tools.HttpSender
import com.axiomalaska.sos.source.data.LocalStation
import com.axiomalaska.sos.source.data.LocalSensor
import com.axiomalaska.sos.source.data.LocalPhenomenon
import com.axiomalaska.sos.source.data.ObservationValues
import com.axiomalaska.sos.source.StationQuery

import org.apache.log4j.Logger

class RawsObservationRetriever(private val stationQuery:StationQuery, 
    private val logger: Logger = Logger.getRootLogger())
	extends ObservationValuesCollectionRetriever {
  
  // ---------------------------------------------------------------------------
  // Private Data
  // ---------------------------------------------------------------------------
  
  private val yearFormatDate = new SimpleDateFormat("yy");
  private val monthFormatDate = new SimpleDateFormat("MM");
  private val dayFormatDate = new SimpleDateFormat("dd");
  private val dateParser = new SimpleDateFormat("yyyyMMddHHmm");
  private val httpSender = new HttpSender()
  
  // ---------------------------------------------------------------------------
  // ObservationValuesCollectionRetriever Members
  // ---------------------------------------------------------------------------
  
  def getObservationValues(station: LocalStation, sensor: LocalSensor, 
      phenomenon: LocalPhenomenon, startDate: Calendar):List[ObservationValues] ={

    val data = getRawData(station, startDate)
    
    if(data == null){
      return Nil
    }
    
    val observationValuesCollection = 
      createSensorObservationValuesCollection(station, sensor, phenomenon)

    val doc = Jsoup.parse(data)

    val body = doc.getElementsByTag("PRE").text()

    if (body.isEmpty) {
      null
    } else {
      //finding headers
      val lines = body.split("\n")
      val headers = mutable.ListBuffer[String]()

      for (index <- 0 until lines.length) {Nil
        var line = lines(index)
        if (line.startsWith(":")) {
          if (line.contains("Day of Year") &&
            lines(index + 1).contains("Time of Day")) {
            headers.add("DateTime")
          } else if (!line.contains("Time of Day")) {
            line = line.replace(":", "");
            line = line.replaceAll("\\(.*\\)", "");
            line = line.replace(" ", "");
            line = line.replace("-", "");
            line = line.toUpperCase();

            headers.add(line)
          }
        } else {
          val columns = line.split(",");
          val calendar = createDate(columns(0))
          if (calendar.after(startDate)) {
            for (columnIndex <- 1 until headers.length) {

              val columnName = headers.get(columnIndex)
              observationValuesCollection.find(
                  _.observedProperty.foreign_tag.equalsIgnoreCase(columnName)) match {
                case Some(sensorObservationValue) => {
                  if (columnIndex < columns.length) {
                    val value = columns(columnIndex)
                    if (!value.isEmpty) {
                      sensorObservationValue.addValue(value.toDouble, calendar)
                    }
                  }
                }
                case None => //do nothing
              }
            }
          }
        }
      }

      observationValuesCollection
    }
  }

  // ---------------------------------------------------------------------------
  // Private Members
  // ---------------------------------------------------------------------------

  private def createSensorObservationValuesCollection(station: LocalStation, sensor: LocalSensor,
    phenomenon: LocalPhenomenon): List[ObservationValues] = {
    val observedProperties = stationQuery.getObservedProperties(
      station.databaseStation, sensor.databaseSensor, phenomenon.databasePhenomenon)

    for (observedProperty <- observedProperties) yield {
      new ObservationValues(observedProperty, sensor, phenomenon, observedProperty.foreign_units)
    }
  }
  
  private def getRawData(databaseStation:LocalStation, startDate: Calendar): String = {
    val thirtyDaysBefore = Calendar.getInstance()
    thirtyDaysBefore.add(Calendar.DAY_OF_MONTH, -29)

    val startDateAlaskaTimeZone = if (startDate.before(thirtyDaysBefore)) {
      getDateObjectInAlaskaTime(thirtyDaysBefore)
    } else {
      getDateObjectInAlaskaTime(startDate)
    }

    val endDateAlaskaTimeZone = getDateObjectInAlaskaTime(Calendar.getInstance)

    val startMonth = monthFormatDate.format(startDateAlaskaTimeZone)
    val startDay = dayFormatDate.format(startDateAlaskaTimeZone)
    val startYear = yearFormatDate.format(startDateAlaskaTimeZone)
    val endMonth = monthFormatDate.format(endDateAlaskaTimeZone)
    val endDay = dayFormatDate.format(endDateAlaskaTimeZone)
    val endYear = yearFormatDate.format(endDateAlaskaTimeZone)

    val foreignTag = databaseStation.databaseStation.foreign_tag
    val foreignId = foreignTag.substring(foreignTag.length() - 4, foreignTag.length());

    val parts = List(
      new HttpPart("stn", foreignId),
      new HttpPart("smon", startMonth),
      new HttpPart("sday", startDay),
      new HttpPart("syea", startYear),
      new HttpPart("emon", endMonth),
      new HttpPart("eday", endDay),
      new HttpPart("eyea", endYear),
      new HttpPart("dfor", "02"),
      new HttpPart("srce", "W"),
      new HttpPart("miss", "03"),
      new HttpPart("flag", "N"),
      new HttpPart("Dfmt", "02"),
      new HttpPart("Tfmt", "01"),
      new HttpPart("Head", "02"),
      new HttpPart("Deli", "01"),
      new HttpPart("unit", "M"),
      new HttpPart("WsMon", "01"),
      new HttpPart("WsDay", "01"),
      new HttpPart("WeMon", "12"),
      new HttpPart("WeDay", "31"),
      new HttpPart("WsHou", "00"),
      new HttpPart("WeHou", "24"))

    val results = httpSender.sendPostMessage(
      "http://www.raws.dri.edu/cgi-bin/wea_list2.pl", parts)

    return results;
  }
  
  private def createDate(rawText: String): Calendar = {
    val date = dateParser.parse(rawText)
    val calendar = Calendar.getInstance(TimeZone
      .getTimeZone("US/Alaska"))
    calendar.set(Calendar.YEAR, date.getYear() + 1900)
    calendar.set(Calendar.MONTH, date.getMonth())
    calendar.set(Calendar.DAY_OF_MONTH, date.getDate())
    calendar.set(Calendar.HOUR_OF_DAY, date.getHours())
    calendar.set(Calendar.MINUTE, date.getMinutes())
    calendar.set(Calendar.SECOND, 0)

    // The time is not able to be changed from the timezone if this is not set. 
    calendar.getTime()

    return calendar
  }
  
  private def getDateObjectInAlaskaTime(calendar: Calendar): Date = {
    val copyCalendar = calendar.clone().asInstanceOf[Calendar]
    copyCalendar.setTimeZone(TimeZone.getTimeZone("US/Alaska"))
    val localCalendar = Calendar.getInstance()
    localCalendar.set(Calendar.YEAR, copyCalendar.get(Calendar.YEAR))
    localCalendar.set(Calendar.MONTH, copyCalendar.get(Calendar.MONTH))
    localCalendar.set(Calendar.DAY_OF_MONTH, copyCalendar.get(Calendar.DAY_OF_MONTH))
    localCalendar.set(Calendar.HOUR_OF_DAY, copyCalendar.get(Calendar.HOUR_OF_DAY))
    localCalendar.set(Calendar.MINUTE, copyCalendar.get(Calendar.MINUTE))
    localCalendar.set(Calendar.SECOND, copyCalendar.get(Calendar.SECOND))

    return localCalendar.getTime()
  }
}