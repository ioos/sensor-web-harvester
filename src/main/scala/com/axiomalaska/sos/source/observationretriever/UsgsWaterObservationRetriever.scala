package com.axiomalaska.sos.source.observationretriever


import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.TimeZone
import java.util.Date
import com.axiomalaska.sos.tools.HttpPart
import com.axiomalaska.sos.tools.HttpSender
import com.axiomalaska.sos.source.data.ObservationValues
import com.axiomalaska.sos.source.StationQuery
import com.axiomalaska.sos.source.data.ObservedProperty
import com.axiomalaska.sos.source.data.LocalStation
import com.axiomalaska.sos.source.data.LocalSensor
import com.axiomalaska.sos.source.data.LocalPhenomenon
import scala.collection.JavaConversions._
import org.cuahsi.waterML.x11.TimeSeriesResponseDocument
import org.cuahsi.waterML.x11.TsValuesSingleVariableType
import org.cuahsi.waterML.x11.ValueSingleVariable
import org.apache.log4j.Logger
import com.axiomalaska.sos.source.SourceUrls
import org.joda.time.DateTime
import org.joda.time.DateTimeZone

class UsgsWaterObservationRetriever(private val stationQuery:StationQuery, 
    private val logger: Logger = Logger.getRootLogger())
	extends ObservationValuesCollectionRetriever {

  // ---------------------------------------------------------------------------
  // Private Data
  // ---------------------------------------------------------------------------

  private val formatDate = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")

  // ---------------------------------------------------------------------------
  // ObservationValuesCollectionRetriever Members
  // ---------------------------------------------------------------------------

  def getObservationValues(station: LocalStation, sensor: LocalSensor,
    phenomenon: LocalPhenomenon, startDate: DateTime): List[ObservationValues] = {

    logger.info("USGS-WATER: Collecting for station - " + station.databaseStation.foreign_tag)
    
    val observedProperties = stationQuery.getObservedProperties(
      station.databaseStation, sensor.databaseSensor, phenomenon.databasePhenomenon)

    val rawData = getRawData(station, sensor, phenomenon, startDate)
    if (rawData != null) {
      val document = TimeSeriesResponseDocument.Factory.parse(rawData)

      val observationValuesCollection =
        for {
          timeSeriesTypes <- document.getTimeSeriesResponse().getTimeSeriesArray()
          val variableCode = timeSeriesTypes.getVariable().getVariableCodeArray(0).getStringValue()
          val noDataValue = timeSeriesTypes.getVariable().getNoDataValue()
          if (timeSeriesTypes.getValuesArray().nonEmpty)
          observationValues <- createSensorObservationValuesCollection(
            timeSeriesTypes.getValuesArray().head, observedProperties,
            variableCode, noDataValue, sensor, phenomenon, startDate)
        } yield { observationValues }

      return observationValuesCollection.toList
    } else {
      Nil
    }
  }
  
  // ---------------------------------------------------------------------------
  // Private Members
  // ---------------------------------------------------------------------------
  
  def getRawData(station: LocalStation,
    sensor: LocalSensor, phenomenon: LocalPhenomenon, startDate: DateTime): String = {

    val observedProperties = stationQuery.getObservedProperties(
        station.databaseStation, sensor.databaseSensor, phenomenon.databasePhenomenon)
    
    val parameterCd = observedProperties.map(_.foreign_tag).mkString(",")
        
    val endDate = DateTime.now()
    
    val thrityDayBefore = DateTime.now().minusDays(30)
    
    val (formatedStartDate, formatedEndDate) = 
      if(startDate.isBefore(thrityDayBefore)){
    	(formatDate.format(getDateObjectInGMT(thrityDayBefore)), 
    	    formatDate.format(getDateObjectInGMT(endDate)))
    }
    else{
    	(formatDate.format(getDateObjectInGMT(startDate)), 
    	    formatDate.format(getDateObjectInGMT(endDate)))
    }
    
    val parts = List( 
        new HttpPart("sites", station.databaseStation.foreign_tag), 
        new HttpPart("parameterCd", parameterCd), 
        new HttpPart("startDT", formatedStartDate), 
        new HttpPart("endDT", formatedEndDate))
        
    val result =
      HttpSender.sendGetMessage(SourceUrls.USGS_WATER_OBSERVATION_RETRIEVAL, parts, false)
    
    return result
  }
  
  private def createSensorObservationValuesCollection(
      tsValuesSingleVariableType:TsValuesSingleVariableType, 
      observedProperties:List[ObservedProperty], 
      variableCode:String, noDataValue:Double, 
      sensor: LocalSensor, phenomenon: LocalPhenomenon, 
      startDate: DateTime):Option[ObservationValues] = {

    val observedProperty =
      observedProperties.find(_.foreign_tag == variableCode) match {
        case Some(observedProperty) => observedProperty
        case None => return None //throw new SensorObservationException("Data Not found: " + variableCode)
      }

    val observationValues = new ObservationValues(observedProperty, sensor, 
        phenomenon, observedProperty.foreign_units)

    for {
      valueSingleVariable <- tsValuesSingleVariableType.getValueArray()
      val calendar = createDate(valueSingleVariable)
      val value = valueSingleVariable.getBigDecimalValue().doubleValue()
      if (noDataValue != value)
      if (calendar.isAfter(startDate))
    } {
      observationValues.addValue(value, calendar)
    }
    
    Some(observationValues)
  }
  
  private def getDateObjectInGMT(calendar:DateTime):Date={
    val copyCalendar = calendar.toCalendar(null)
    copyCalendar.setTimeZone(TimeZone.getTimeZone("GMT"))
    val localCalendar = Calendar.getInstance()
    localCalendar.set(Calendar.YEAR, copyCalendar.get(Calendar.YEAR))
    localCalendar.set(Calendar.MONTH, copyCalendar.get(Calendar.MONTH))
    localCalendar.set(Calendar.DAY_OF_MONTH, copyCalendar.get(Calendar.DAY_OF_MONTH))
    localCalendar.set(Calendar.HOUR_OF_DAY,copyCalendar.get(Calendar.HOUR_OF_DAY))
    localCalendar.set(Calendar.MINUTE, copyCalendar.get(Calendar.MINUTE))
    localCalendar.set(Calendar.SECOND, copyCalendar.get(Calendar.SECOND))
    
    // The time is not able to be changed from the 
    //setTimezone if this is not set. Java Error
    localCalendar.getTime()
    
    return localCalendar.getTime()
  }
  
  private def createDate(valueSingleVariable: ValueSingleVariable): DateTime = {
    val vsCalendar = valueSingleVariable.getDateTime()

    return new DateTime(vsCalendar.get(Calendar.YEAR), 
        vsCalendar.get(Calendar.MONTH) + 1, vsCalendar.get(Calendar.DAY_OF_MONTH), 
        vsCalendar.get(Calendar.HOUR_OF_DAY), vsCalendar.get(Calendar.MINUTE), 0, 
        DateTimeZone.forTimeZone(vsCalendar.getTimeZone()))
  }
}