package com.axiomalaska.sos.harvester.source.observationretriever

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.Logger
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.joda.time.format.DateTimeFormat
import org.w3c.dom.ls.DOMImplementationLS

import com.axiomalaska.sos.harvester.StationQuery;
import com.axiomalaska.sos.harvester.data.LocalPhenomenon
import com.axiomalaska.sos.harvester.data.LocalSensor
import com.axiomalaska.sos.harvester.data.LocalStation
import com.axiomalaska.sos.harvester.data.ObservationValues
import com.axiomalaska.sos.tools.HttpSender

import webservices2.RequestsServiceLocator

class NerrsObservationRetriever(private val stationQuery:StationQuery, 
    private val logger: Logger = Logger.getRootLogger())
	extends ObservationValuesCollectionRetriever {
  
  // ---------------------------------------------------------------------------
  // Private Data
  // ---------------------------------------------------------------------------
  
  private val httpSender = new HttpSender()
  private val gmtTimeZone = DateTimeZone.forID("US/Alaska")
  private val dateFormat = new SimpleDateFormat("MM/dd/yyyy")
  private val gmtTimeFormatter = DateTimeFormat.forPattern("MM/dd/yyyy HH:mm")
            .withZone(gmtTimeZone)
            
  // ---------------------------------------------------------------------------
  // ObservationValuesCollectionRetriever Members
  // ---------------------------------------------------------------------------
  
  def getObservationValues(station: LocalStation, sensor: LocalSensor, 
      phenomenon: LocalPhenomenon, startDate: DateTime):List[ObservationValues] ={

    logger.info("NERRS: Collecting for station - " + station.databaseStation.foreign_tag)
    
    val observationValuesCollection = 
      createSensorObservationValuesCollection(station, sensor, phenomenon)

    getXml(station, startDate, observationValuesCollection) match {
      case Some(xml) => {
        var priorCalendar:DateTime = null
        for { row <- (xml \\ "data") } {
          val rawDate = (row \\ "utcStamp").text
          val calendar = createDate(rawDate)
          if (priorCalendar == null || !priorCalendar.equals(calendar)) {
            priorCalendar = calendar
            if (calendar.isAfter(startDate)) {
              for {
                observationValues <- observationValuesCollection
                val param = observationValues.observedProperty.foreign_tag
                val rawValue = (row \\ param).text
                if (rawValue.nonEmpty)
              } {

                val value = rawValue.toDouble

                observationValues.addValue(value, calendar)
              }
            }
          }
        }
        observationValuesCollection.filter(_.getValues.size > 0)
      }
      case None => {
        Nil
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Private Members
  // ---------------------------------------------------------------------------
            
  private def createDate(rawText: String) = gmtTimeFormatter.parseDateTime(rawText)
  
  private def getXml(station: LocalStation, startDate: DateTime, 
      observationValues: List[ObservationValues]):Option[scala.xml.Elem]={
    try {
      val locator = new RequestsServiceLocator()

      val requests = locator.getRequestsCfc()

      val stationCode = station.databaseStation.foreign_tag
      val minDate = dateFormat.format(startDate.toDate())
      val maxDate = dateFormat.format(Calendar.getInstance().getTime())
      val param = observationValues.map(_.observedProperty.foreign_tag).mkString(",")

      val doc = requests.exportAllParamsDateRangeXMLNew(stationCode, minDate, maxDate, param)

      val domImplLS = doc.getImplementation().asInstanceOf[DOMImplementationLS]

      val serializer = domImplLS.createLSSerializer()
      val str = serializer.writeToString(doc)

      Some(scala.xml.XML.loadString(str))
    } catch {
      case e: Exception => {
        None
      }
    }
  }
  
  private def createSensorObservationValuesCollection(station: LocalStation, sensor: LocalSensor,
    phenomenon: LocalPhenomenon): List[ObservationValues] = {
    val observedProperties = stationQuery.getObservedProperties(
      station.databaseStation, sensor.databaseSensor, phenomenon.databasePhenomenon)

    for (observedProperty <- observedProperties) yield {
      new ObservationValues(observedProperty, sensor, phenomenon, observedProperty.foreign_units)
    }
  }
}
