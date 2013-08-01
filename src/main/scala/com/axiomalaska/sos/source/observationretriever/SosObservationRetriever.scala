package com.axiomalaska.sos.source.observationretriever

import com.axiomalaska.sos.source.StationQuery
import java.util.Calendar
import com.axiomalaska.sos.source.data.LocalStation
import net.opengis.ows.x11.ExceptionReportDocument
import scala.collection.mutable
import net.opengis.gml.x32.TimeInstantType
import net.opengis.gml.x32.ValueArrayPropertyType
import com.axiomalaska.sos.source.data.ObservationValues
import com.axiomalaska.sos.source.data.LocalSensor
import com.axiomalaska.sos.source.data.LocalPhenomenon
import scala.collection.JavaConversions._
import org.apache.log4j.Logger
import com.axiomalaska.phenomena.Phenomenon
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import com.axiomalaska.phenomena.Phenomena
import scala.xml.Node
import org.joda.time.format.DateTimeFormat
import com.axiomalaska.sos.source.data.RawValues

abstract class SosObservationRetriever(private val stationQuery:StationQuery) 
	extends ObservationValuesCollectionRetriever {
  
  // ---------------------------------------------------------------------------
  // Private Data
  // ---------------------------------------------------------------------------
  
  private val LOGGER = Logger.getLogger(getClass())
  private val sosRawDataRetriever = new SosRawDataRetriever()
  
  protected val serviceUrl:String

  // ---------------------------------------------------------------------------
  // ObservationValuesCollectionRetriever Members
  // ---------------------------------------------------------------------------

  def getObservationValues(station: LocalStation, sensor: LocalSensor,
    phenomenon: LocalPhenomenon, startDate: DateTime): List[ObservationValues] = {

    val sensorForeignId = sosRawDataRetriever.getSensorForeignId(phenomenon)

    val rawData = sosRawDataRetriever.getRawData(serviceUrl, 
        sensorForeignId, station.databaseStation.foreign_tag, startDate, DateTime.now())

    val observationValuesCollection = createSensorObservationValuesCollection(
        station, sensor, phenomenon)
    val phenomenonForeignTags = observationValuesCollection.map(_.observedProperty.foreign_tag)

    for {
      rawValue <- sosRawDataRetriever.createRawValues(rawData, phenomenonForeignTags)
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

