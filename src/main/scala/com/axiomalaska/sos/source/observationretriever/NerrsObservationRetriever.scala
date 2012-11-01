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
import webservices2.RequestsServiceLocator
import org.w3c.dom.ls.DOMImplementationLS

class NerrsObservationRetriever(private val stationQuery:StationQuery, 
    private val logger: Logger = Logger.getRootLogger())
	extends ObservationValuesCollectionRetriever {
  
  // ---------------------------------------------------------------------------
  // Private Data
  // ---------------------------------------------------------------------------
  
  private val httpSender = new HttpSender()
  private val dateFormat = new SimpleDateFormat("MM/dd/yyyy")
  private val dateParser = new SimpleDateFormat("MM/dd/yyyy HH:mm")
  
  // ---------------------------------------------------------------------------
  // ObservationValuesCollectionRetriever Members
  // ---------------------------------------------------------------------------
  
  def getObservationValues(station: LocalStation, sensor: LocalSensor, 
      phenomenon: LocalPhenomenon, startDate: Calendar):List[ObservationValues] ={
    
    val observationValuesCollection = 
      createSensorObservationValuesCollection(station, sensor, phenomenon)

    getXml(station, startDate, observationValuesCollection) match {
      case Some(xml) => {
        var priorCalendar:Calendar = null
        for { row <- (xml \\ "data") } {
          val rawDate = (row \\ "utcStamp").text
          val calendar = createDate(rawDate)
          if (priorCalendar == null || !priorCalendar.equals(calendar)) {
            priorCalendar = calendar
            if (calendar.after(startDate)) {
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
    calendar.set(Calendar.MILLISECOND, 0)

    // The time is not able to be changed from the timezone if this is not set. 
    calendar.getTime()

    return calendar
  }
  
  private def getXml(station: LocalStation, startDate: Calendar, 
      observationValues: List[ObservationValues]):Option[scala.xml.Elem]={
    try {
      val locator = new RequestsServiceLocator()

      val requests = locator.getRequestsCfc()

      val stationCode = station.databaseStation.foreign_tag
      val minDate = dateFormat.format(startDate.getTime())
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