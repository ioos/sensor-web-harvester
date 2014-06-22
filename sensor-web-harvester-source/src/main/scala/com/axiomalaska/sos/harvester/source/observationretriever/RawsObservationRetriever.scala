package com.axiomalaska.sos.harvester.source.observationretriever

import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Date
import java.util.TimeZone

import scala.collection.JavaConversions.bufferAsJavaList
import scala.collection.JavaConversions.seqAsJavaList
import scala.collection.mutable

import org.apache.log4j.Logger
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.joda.time.format.DateTimeFormat
import org.jsoup.Jsoup

import com.axiomalaska.sos.harvester.StationQuery;
import com.axiomalaska.sos.harvester.source.SourceUrls
import com.axiomalaska.sos.harvester.data.LocalPhenomenon
import com.axiomalaska.sos.harvester.data.LocalSensor
import com.axiomalaska.sos.harvester.data.LocalStation
import com.axiomalaska.sos.harvester.data.ObservationValues
import com.axiomalaska.sos.tools.HttpPart
import com.axiomalaska.sos.tools.HttpSender

/**
 * The time zone is currently only correct for Alaska stations. 
 */
class RawsObservationRetriever(private val stationQuery:StationQuery)
	extends ObservationValuesCollectionRetriever {
  
  // ---------------------------------------------------------------------------
  // Private Data
  // ---------------------------------------------------------------------------
  
  private val LOGGER = Logger.getLogger(getClass())
  private val yearFormatDate = new SimpleDateFormat("yy");
  private val monthFormatDate = new SimpleDateFormat("MM");
  private val dayFormatDate = new SimpleDateFormat("dd");
  private val httpSender = new HttpSender()
  private val akTimeZone = DateTimeZone.forID("US/Alaska")
  private val dateParser = DateTimeFormat.forPattern("yyyyMMddHHmm")
            .withZone(akTimeZone)
            
  // ---------------------------------------------------------------------------
  // ObservationValuesCollectionRetriever Members
  // ---------------------------------------------------------------------------
  
  def getObservationValues(station: LocalStation, sensor: LocalSensor, 
      phenomenon: LocalPhenomenon, startDate: DateTime):List[ObservationValues] ={

    LOGGER.info("RAWS: Collecting for station - " + station.databaseStation.foreign_tag)

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
          if (calendar.isAfter(startDate)) {
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
  
  private def getRawData(databaseStation:LocalStation, startDate: DateTime): String = {
    val thirtyDaysBefore = DateTime.now()
    thirtyDaysBefore.minusDays(29)

    val startDateAlaskaTimeZone = if (startDate.isBefore(thirtyDaysBefore)) {
      getDateObjectInAlaskaTime(thirtyDaysBefore)
    } else {
      getDateObjectInAlaskaTime(startDate)
    }

    val endDateAlaskaTimeZone = getDateObjectInAlaskaTime(DateTime.now())

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

    httpSender.sendPostMessage(
        SourceUrls.RAWS_OBSERVATION_RETRIEVAL, parts)
  }
  
  private def createDate(rawText: String) = dateParser.parseDateTime(rawText)
  
  private def getDateObjectInAlaskaTime(calendar: DateTime): Date = {
    val copyCalendar = calendar.toCalendar(null)
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
