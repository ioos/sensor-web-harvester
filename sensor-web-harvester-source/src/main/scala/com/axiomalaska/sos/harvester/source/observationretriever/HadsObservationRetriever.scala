package com.axiomalaska.sos.harvester.source.observationretriever

import scala.collection.JavaConversions.seqAsJavaList
import scala.util.matching.Regex

import org.apache.log4j.Logger
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.joda.time.Duration
import org.joda.time.format.DateTimeFormat

import com.axiomalaska.sos.harvester.StationQuery;
import com.axiomalaska.sos.harvester.source.SourceUrls
import com.axiomalaska.sos.harvester.data.LocalPhenomenon
import com.axiomalaska.sos.harvester.data.LocalSensor
import com.axiomalaska.sos.harvester.data.LocalStation
import com.axiomalaska.sos.harvester.data.ObservationValues
import com.axiomalaska.sos.harvester.data.RawValues
import com.axiomalaska.sos.tools.HttpPart
import com.axiomalaska.sos.tools.HttpSender

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
}
object HadsObservationRetriever{
  private val httpSender = new HttpSender()
  private val gmtTimeZone = DateTimeZone.forID("GMT")
  private val gmtTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm")
            .withZone(gmtTimeZone)
  
  /**
   * Get the raw unparsed string data for the state ID from the source
   */
  def getStateRawData(stateId: String, startDate: DateTime):String = {
    val parts = List[HttpPart](
      new HttpPart("state", stateId),
      new HttpPart("hsa", "nil"),
      new HttpPart("of", "1"),
      new HttpPart("nesdis_ids", "nil"),
      new HttpPart("sinceday", calculatedSinceDay(startDate)))

    httpSender.sendPostMessage(SourceUrls.HADS_OBSERVATION_RETRIEVAL, parts)
  }
  
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
    val duration = new Duration(DateTime.now(), startDate)
    
    val hoursDiff = math.abs(duration.getStandardHours())
    
    val daysSince = if(hoursDiff > 24*5){
      6
    } else if(hoursDiff >= 24){
      math.ceil(hoursDiff /24.0).toInt
    } else if(hoursDiff < 24){
      hoursDiff * -1
    }
    
    daysSince.toString
  }
}
