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
import com.axiomalaska.sos.source.data.RawValues

class HadsObservationRetriever(private val stationQuery:StationQuery) 
	extends ObservationValuesCollectionRetriever {

  // ---------------------------------------------------------------------------
  // Private Data
  // ---------------------------------------------------------------------------
  
  private val LOGGER = Logger.getLogger(getClass())
            
  // ---------------------------------------------------------------------------
  // ObservationValuesCollectionRetriever Members
  // ---------------------------------------------------------------------------
  
  def getObservationValues(station: LocalStation,
    sensor: LocalSensor, phenomenon: LocalPhenomenon, startDate: DateTime): 
    List[ObservationValues] = {
      
    LOGGER.info("HADS: Collecting for station - " + 
        station.databaseStation.foreign_tag)

    val result = HadsObservationRetriever.getRawData(
        station.databaseStation.foreign_tag, startDate)

    if (result != null) {
      collectionObservationValues(result, startDate, station, sensor, phenomenon)
    } else {
      Nil
    }
  }

  // ---------------------------------------------------------------------------
  // Private Members
  // ---------------------------------------------------------------------------

  private def collectionObservationValues(result: String, startDate: DateTime,
    station: LocalStation, sensor: LocalSensor,
    phenomenon: LocalPhenomenon): List[ObservationValues] = {
    val observationValuesCollections =
        createSensorObservationValuesCollection(station, sensor, phenomenon)

    val phenomenonForeignTags = observationValuesCollections.map(_.observedProperty.foreign_tag)
      for {
        rawValues <- HadsObservationRetriever.createRawValues(result,
         phenomenonForeignTags)
         observationValues <- observationValuesCollections.find(
             _.observedProperty.foreign_tag == rawValues.phenomenonForeignTag)
         (dateTime, value) <- rawValues.values
        if (dateTime.isAfter(startDate))
      } {
        observationValues.addValue(value.toDouble, dateTime)
      }

    observationValuesCollections
  }
  
  private def createSensorObservationValuesCollection(station: LocalStation, 
      sensor: LocalSensor, phenomenon: LocalPhenomenon): List[ObservationValues] = {
    val observedProperties = stationQuery.getObservedProperties(
      station.databaseStation, sensor.databaseSensor, phenomenon.databasePhenomenon)

    for (observedProperty <- observedProperties) yield {
      new ObservationValues(observedProperty, sensor, phenomenon, observedProperty.foreign_units)
    }
  }

  private def collectValues(data: String, observationValues: ObservationValues,
    startDate: DateTime) {

  }
}
object HadsObservationRetriever{
  private val httpSender = new HttpSender()
  private val gmtTimeZone = DateTimeZone.forID("GMT")
  private val gmtTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm")
            .withZone(gmtTimeZone)
  
  /**
   * Get the raw unparsed string data for the station from the source
   */
  def getRawData(stationForeignId: String, startDate: DateTime):String = {
    val parts = List[HttpPart](
      new HttpPart("state", "nil"),
      new HttpPart("hsa", "nil"),
      new HttpPart("of", "1"),
      new HttpPart("nesdis_ids", stationForeignId),
      new HttpPart("sinceday", calculatedSinceDay(startDate)))

    httpSender.sendPostMessage(SourceUrls.HADS_OBSERVATION_RETRIEVAL, parts)
  }
  
  /**
   * Parse the raw unparsed data into DateTime, Value list. 
   * 
   * phenomenonForeignTag - The HADS Phenomenon Foreign Tag
   */
  def createRawValues(data: String, 
      phenomenonForeignTags: List[String]): List[RawValues] ={
    for { phenomenonForeignTag <- phenomenonForeignTags } yield {
      val pattern = new Regex(phenomenonForeignTag
        + """\|(\d{4}-\d{2}-\d{2} \d{2}:\d{2})\|(-?\d+.\d+)\|  \|""")

      val values = (for {
        patternMatch <- pattern.findAllIn(data)
        val pattern(rawDate, value) = patternMatch
        val calendar = gmtTimeFormatter.parseDateTime(rawDate)
      } yield {
        (calendar, value.toDouble)
      }).toList
      
      RawValues(phenomenonForeignTag, values)
    }
  }
  
  // ---------------------------------------------------------------------------
  // Private Members
  // ---------------------------------------------------------------------------

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