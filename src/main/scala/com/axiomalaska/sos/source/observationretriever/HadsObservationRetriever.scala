package com.axiomalaska.sos.source.observationretriever

import com.axiomalaska.sos.ObservationRetriever
import com.axiomalaska.sos.data.SosPhenomenon
import com.axiomalaska.sos.data.SosSensor
import com.axiomalaska.sos.data.ObservationCollection
import com.axiomalaska.sos.data.SosStation
import com.axiomalaska.sos.tools.HttpPart
import com.axiomalaska.sos.tools.HttpSender

import com.axiomalaska.sos.source.StationQuery
import com.axiomalaska.sos.source.data.LocalStation
import com.axiomalaska.sos.source.data.LocalSensor
import com.axiomalaska.sos.source.data.LocalPhenomenon
import com.axiomalaska.sos.source.data.ObservedProperty
import com.axiomalaska.sos.source.data.ObservationValues

import scala.collection.JavaConversions._
import scala.util.matching.Regex

import java.util.Calendar
import java.util.TimeZone
import java.text.SimpleDateFormat

class HadsObservationRetriever(private val stationQuery:StationQuery) 
	extends ObservationRetriever {

  private val httpSender = new HttpSender()
  private val parseDate = new SimpleDateFormat("yyyy-MM-dd HH:mm")
  
  override def getObservationCollection(station: SosStation,
    sensor: SosSensor, phenomenon:SosPhenomenon, startDate: Calendar): ObservationCollection = {

    station match{
      case databaseStation:LocalStation =>{
        getObservationCollection(databaseStation, sensor, phenomenon, startDate)
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Private Members
  // ---------------------------------------------------------------------------

  private def getObservationCollection(databaseStation: LocalStation,
    sensor: SosSensor, phenomenon: SosPhenomenon, startDate: Calendar): ObservationCollection = {

    val parts = List[HttpPart](
      new HttpPart("state", "nil"),
      new HttpPart("hsa", "nil"),
      new HttpPart("of", "1"),
      new HttpPart("nesdis_ids", databaseStation.databaseStation.foreign_tag),
      new HttpPart("sinceday", calculatedSinceDay(startDate)))

    val result =
      httpSender.sendPostMessage(
        "http://amazon.nws.noaa.gov/nexhads2/servlet/DecodedData", parts);

    val observationValuesCollections =
      createSensorObservationValuesCollection(databaseStation, sensor, phenomenon)

    for { observationValues <- observationValuesCollections } {
      collectValues(result, observationValues, startDate, Calendar.getInstance)
    }

    createObservationCollection(databaseStation, observationValuesCollections)
  }
  
  private def createObservationCollection(databaseStation: LocalStation, 
      observationValuesCollection:List[ObservationValues]):ObservationCollection ={
      val filteredObservationValuesCollection = observationValuesCollection.filter(_.getDates.size > 0)
      
      if(filteredObservationValuesCollection.size == 1){
        val observationValues = filteredObservationValuesCollection.head
        val observationCollection = new ObservationCollection()
        observationCollection.setObservationDates(observationValues.getDates)
        observationCollection.setObservationValues(observationValues.getValues)
        observationCollection.setPhenomenon(observationValues.phenomenon)
        observationCollection.setSensor(observationValues.sensor)
        observationCollection.setStation(databaseStation)
        
        observationCollection
      }
      else{
        println("Error more than one observationValues")
        return null
      }
  }
  
  private def createSensorObservationValuesCollection(station: SosStation,
    sensor: SosSensor, phenomenon: SosPhenomenon): List[ObservationValues] = {
    (station, sensor, phenomenon) match {
      case (localStation: LocalStation, localSensor: LocalSensor, localPhenomenon: LocalPhenomenon) => {
        val observedProperties = stationQuery.getObservedProperties(
          localStation.databaseStation, localSensor.databaseSensor,
          localPhenomenon.databasePhenomenon)

        for (observedProperty <- observedProperties) yield {
          new ObservationValues(observedProperty, sensor, phenomenon)
        }
      }
      case _ => Nil
    }
  }
  
  private def collectValues(data: String, observationValues: ObservationValues,
    startDate: Calendar, endDate: Calendar) {
    
    val pattern = new Regex(observationValues.observedProperty.foreign_tag
      + """\|(\d{4}-\d{2}-\d{2} \d{2}:\d{2})\|(-?\d+.\d+)\|  \|""")

    for (patternMatch <- pattern.findAllIn(data)) {
      val pattern(rawDate, value) = patternMatch
      val calendar = createDate(rawDate)
      if (calendar.after(startDate) && calendar.before(endDate)) {

        observationValues.addValue(value.toDouble, calendar)
      }
    }
  }

  private def createDate(rawDate: String): Calendar = {
    val date = parseDate.parse(rawDate);

    val calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT"))
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
  
  private def calculatedSinceDay(startDate: Calendar):String ={
    val currentDate = Calendar.getInstance()
    val copyStartDate = startDate.clone().asInstanceOf[Calendar]
    var days = 0
    while(copyStartDate.before(currentDate) && days < 6){
      days += 1
      copyStartDate.add(Calendar.DAY_OF_MONTH, 1)
    }

    val sinceday = days match {
      case 1 => {
        val copyStartDate = startDate.clone().asInstanceOf[Calendar]
        var hours = 0
        while (copyStartDate.before(currentDate)) {
          hours += 1
          copyStartDate.add(Calendar.HOUR_OF_DAY, 1)
        }
        hours * -1
      }
      case x: Int => x
    }
    
    sinceday.toString()
  }
}