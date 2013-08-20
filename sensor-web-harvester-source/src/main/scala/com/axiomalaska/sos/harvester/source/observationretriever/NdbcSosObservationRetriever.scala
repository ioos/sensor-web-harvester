package com.axiomalaska.sos.harvester.source.observationretriever

import scala.collection.mutable
import scala.collection.JavaConversions._
import org.apache.log4j.Logger

import com.axiomalaska.sos.harvester.StationQuery;
import com.axiomalaska.sos.harvester.source.SourceUrls
import org.joda.time.DateTime
import com.axiomalaska.sos.harvester.data.RawValues
import com.axiomalaska.sos.harvester.data.LocalStation
import com.axiomalaska.sos.harvester.data.LocalSensor
import com.axiomalaska.sos.harvester.data.ObservationValues
import com.axiomalaska.sos.harvester.data.LocalPhenomenon
import com.axiomalaska.sos.harvester.source.SourceUrls

class NdbcSosObservationRetriever(private val stationQuery:StationQuery) 
	extends ObservationValuesCollectionRetriever {
  
  private val sosRawDataRetriever = new SosRawDataRetriever()

  // ---------------------------------------------------------------------------
  // ObservationValuesCollectionRetriever Members
  // ---------------------------------------------------------------------------

  def getObservationValues(station: LocalStation, sensor: LocalSensor,
    phenomenon: LocalPhenomenon, startDate: DateTime): List[ObservationValues] = {

    val sensorForeignId = sosRawDataRetriever.getSensorForeignId(phenomenon)
    val rawData = NdbcSosObservationRetriever.getRawData(sensorForeignId, 
        station.databaseStation.foreign_tag, startDate, DateTime.now())

    val observationValuesCollection = createSensorObservationValuesCollection(
        station, sensor, phenomenon)
    val phenomenonForeignTags = observationValuesCollection.map(
        _.observedProperty.foreign_tag)

    for {
      rawValue <- NdbcSosObservationRetriever.createRawValues(
          rawData, phenomenonForeignTags)
      observationValues <- observationValuesCollection.find(
          _.observedProperty.foreign_tag == rawValue.phenomenonForeignTag)
      (dateTime, value) <- rawValue.values
      if (dateTime.isAfter(startDate))
    } {
      observationValues.addValue(value, dateTime)
    }

    observationValuesCollection
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
}

object NdbcSosObservationRetriever{
	private val sosRawDataRetriever = new SosRawDataRetriever()
    
  /**
   * Get the raw unparsed string data for the station from the source
   */
  def getRawData(sensorForeignId:String, stationTag:String, 
      startDate:DateTime, endDate:DateTime):String = {
    
    sosRawDataRetriever.getRawData(SourceUrls.NDBC_SOS, stationTag, 
        sensorForeignId, startDate, endDate)
  }
  
  /**
   * Parse the raw unparsed data into DateTime, Value list. 
   * 
   * phenomenonForeignTag - The HADS Phenomenon Foreign Tag
   */
  def createRawValues(rawData: String, 
      phenomenonForeignTags: List[String]): List[RawValues] ={
    sosRawDataRetriever.createRawValues(rawData, phenomenonForeignTags)
  }
}
