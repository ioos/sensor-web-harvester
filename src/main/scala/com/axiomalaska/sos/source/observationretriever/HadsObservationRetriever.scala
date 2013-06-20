package com.axiomalaska.sos.source.observationretriever

import com.axiomalaska.sos.tools.HttpPart
import com.axiomalaska.sos.tools.HttpSender
import com.axiomalaska.sos.source.StationQuery
import com.axiomalaska.sos.source.data.LocalStation
import com.axiomalaska.sos.source.data.LocalSensor
import com.axiomalaska.sos.source.data.LocalPhenomenon
import com.axiomalaska.sos.source.data.ObservationValues
import scala.collection.JavaConversions._
import scala.util.matching.Regex
import java.util.Calendar
import java.util.TimeZone
import java.text.SimpleDateFormat
import org.apache.log4j.Logger
import com.axiomalaska.sos.source.SourceUrls
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.joda.time.DateTimeZone

class HadsObservationRetriever(private val stationQuery:StationQuery) 
	extends ObservationValuesCollectionRetriever {

  
  // ---------------------------------------------------------------------------
  // Private Data
  // ---------------------------------------------------------------------------
  
  private val LOGGER = Logger.getLogger(getClass())
  private val httpSender = new HttpSender()
  private val gmtTimeZone = DateTimeZone.forID("GMT")
  private val gmtTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm")
            .withZone(gmtTimeZone)
            
  // ---------------------------------------------------------------------------
  // ObservationValuesCollectionRetriever Members
  // ---------------------------------------------------------------------------
  
  def getObservationValues(station: LocalStation,
    sensor: LocalSensor, phenomenon: LocalPhenomenon, startDate: DateTime): 
    List[ObservationValues] = {
      
    LOGGER.info("HADS: Collecting for station - " + 
        station.databaseStation.foreign_tag)

    val parts = List[HttpPart](
      new HttpPart("state", "nil"),
      new HttpPart("hsa", "nil"),
      new HttpPart("of", "1"),
      new HttpPart("nesdis_ids", station.databaseStation.foreign_tag),
      new HttpPart("sinceday", calculatedSinceDay(startDate)))

    val result =
      httpSender.sendPostMessage(SourceUrls.HADS_OBSERVATION_RETRIEVAL, parts);

    if (result != null) {
      val observationValuesCollections =
        createSensorObservationValuesCollection(station, sensor, phenomenon)

      for { observationValues <- observationValuesCollections } {
        collectValues(result, observationValues, startDate, DateTime.now())
      }

      observationValuesCollections
    } else {
      Nil
    }
  }
  
  // ---------------------------------------------------------------------------
  // Private Members
  // ---------------------------------------------------------------------------
  
  private def createSensorObservationValuesCollection(station: LocalStation, 
      sensor: LocalSensor, phenomenon: LocalPhenomenon): List[ObservationValues] = {
    val observedProperties = stationQuery.getObservedProperties(
      station.databaseStation, sensor.databaseSensor, phenomenon.databasePhenomenon)

    for (observedProperty <- observedProperties) yield {
      new ObservationValues(observedProperty, sensor, phenomenon, observedProperty.foreign_units)
    }
  }
  
  private def collectValues(data: String, observationValues: ObservationValues,
    startDate: DateTime, endDate: DateTime) {
    
    val pattern = new Regex(observationValues.observedProperty.foreign_tag
      + """\|(\d{4}-\d{2}-\d{2} \d{2}:\d{2})\|(-?\d+.\d+)\|  \|""")

    for (patternMatch <- pattern.findAllIn(data)) {
      val pattern(rawDate, value) = patternMatch
      val calendar = gmtTimeFormatter.parseDateTime(rawDate)
      if (calendar.isAfter(startDate) && calendar.isBefore(endDate)) {

        observationValues.addValue(value.toDouble, calendar)
      }
    }
  }
  
  private def calculatedSinceDay(startDate: DateTime):String ={
    val currentDate:DateTime = DateTime.now()
    val copyStartDate:DateTime = startDate.toDateTime()
    var days = 0
    while(copyStartDate.isBefore(currentDate) && days < 6){
      days += 1
      copyStartDate.plusDays(1)
    }

    val sinceday = days match {
      case 1 => {
        val copyStartDate = startDate.toDateTime()
        var hours = 0
        while (copyStartDate.isBefore(currentDate)) {
          hours += 1
          copyStartDate.plusDays(1)
        }
        hours * -1
      }
      case x: Int => x
    }
    
    sinceday.toString()
  }
}