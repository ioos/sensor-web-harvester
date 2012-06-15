package com.axiomalaska.sos.source.observationretriever

import com.axiomalaska.sos.source.StationQuery
import com.axiomalaska.sos.ObservationRetriever
import com.axiomalaska.sos.data.SosSensor
import com.axiomalaska.sos.data.SosPhenomenon
import com.axiomalaska.sos.data.SosStation
import java.util.Calendar
import com.axiomalaska.sos.data.ObservationCollection
import com.axiomalaska.sos.source.data.LocalStation
import net.opengis.ows.x11.ExceptionReportDocument
import net.opengis.om.x10.CompositeObservationDocument
import scala.collection.mutable
import gov.noaa.ioos.x061.CompositePropertyType
import gov.noaa.ioos.x061.ValueArrayType
import gov.noaa.ioos.x061.CompositeValueType
import net.opengis.gml.x32.ValueArrayPropertyType
import gov.noaa.ioos.x061.NamedQuantityType
import net.opengis.gml.x32.TimeInstantType
import com.axiomalaska.sos.source.data.ObservationValues
import com.axiomalaska.sos.source.data.LocalSensor
import com.axiomalaska.sos.source.data.LocalPhenomenon
import com.axiomalaska.sos.source.data.ObservedProperty
import scala.collection.JavaConversions._
import com.axiomalaska.sos.source.data.SensorPhenomenonIds

class NoaaNosCoOpsObservationRetriever(private val stationQuery:StationQuery) 
	extends ObservationRetriever {
  
  // ---------------------------------------------------------------------------
  // Private Data
  // ---------------------------------------------------------------------------

  private val sosRawDataRetriever = new SosRawDataRetriever()
  private val serviceUrl = "http://opendap.co-ops.nos.noaa.gov/ioos-dif-sos/SOS"
  private val offeringTag = "urn:x-noaa:def:station:NOAA.NOS.CO-OPS::"
  private val observedPropertyTag = "http://mmisw.org/ont/cf/parameter/"
  
  // ---------------------------------------------------------------------------
  // ObservationRetriever Members
  // ---------------------------------------------------------------------------

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

    val sensorForeignId = getSensorForeignId(phenomenon)
    
    val rawData = sosRawDataRetriever.getRawData(serviceUrl, offeringTag,
          observedPropertyTag, databaseStation.databaseStation.foreign_tag,
          sensorForeignId, startDate, Calendar.getInstance)
    
    createCompositeObservationDocument(rawData) match {
      case Some(compositeObservationDocument) => {
        return buildSensorObservationValues(compositeObservationDocument, 
            databaseStation, sensor, phenomenon, startDate)
      }
      case None => {
        val exceptionDocument =
          ExceptionReportDocument.Factory.parse(rawData);

        val fullMessage = exceptionDocument.getExceptionReport().
          getExceptionArray()(0).getExceptionTextArray().mkString(", ");

        println(fullMessage)
        
        return null
      }
    }
    
    return null
  }
  
  private def getSensorForeignId(phenomenon: SosPhenomenon):String = {
    val localPhenomenon = phenomenon.asInstanceOf[LocalPhenomenon]
    
    localPhenomenon.databasePhenomenon.id match{
      case SensorPhenomenonIds.BAROMETRIC_PRESSURE =>{
        "air_pressure"
      }
      case SensorPhenomenonIds.AIR_TEMPERATURE =>{
        "air_temperature"
      }
      case SensorPhenomenonIds.SEA_WATER_TEMPERATURE =>{
        "sea_water_temperature"
      }
      case SensorPhenomenonIds.CURRENT_DIRECTION =>{
        "currents"
      }
      case SensorPhenomenonIds.CURRENT_SPEED =>{
        "currents"
      }
      case SensorPhenomenonIds.WATER_LEVEL_PREDICTIONS =>{
        "sea_surface_height_amplitude_due_to_equilibrium_ocean_tide"
      }
      case SensorPhenomenonIds.WATER_LEVEL =>{
        "water_surface_height_above_reference_datum"
      }
      case SensorPhenomenonIds.WIND_DIRECTION =>{
        "winds"
      }
      case SensorPhenomenonIds.WIND_GUST =>{
        "winds"
      }
      case SensorPhenomenonIds.WIND_SPEED =>{
        "winds"
      }
      case SensorPhenomenonIds.WIND_GUST_DIRECTION =>{
        "winds"
      }
      case SensorPhenomenonIds.WIND_WAVE_DIRECTION =>{
        "waves"
      }
      case SensorPhenomenonIds.WIND_WAVE_PERIOD =>{
        "waves"
      }
      case SensorPhenomenonIds.WIND_WAVE_HEIGHT =>{
        "waves"
      }
      case SensorPhenomenonIds.SWELL_PERIOD =>{
        "waves"
      }
      case SensorPhenomenonIds.SWELL_HEIGHT =>{
        "waves"
      }
      case SensorPhenomenonIds.SWELL_WAVE_DIRECTION =>{
        "waves"
      }
      case SensorPhenomenonIds.DOMINANT_WAVE_DIRECTION =>{
        "waves"
      }
      case SensorPhenomenonIds.DOMINANT_WAVE_PERIOD =>{
        "waves"
      }
      case SensorPhenomenonIds.SIGNIFICANT_WAVE_HEIGHT =>{
        "waves"
      }
      case SensorPhenomenonIds.AVERAGE_WAVE_PERIOD =>{
        "waves"
      }
    }
  }
  
  private def buildSensorObservationValues(
    compositeObservationDocument: CompositeObservationDocument, 
    databaseStation: LocalStation, sensor: SosSensor, phenomenon: SosPhenomenon, 
    startDate: Calendar): ObservationCollection = {
      
    val observationValuesCollection = createSensorObservationValuesCollection(databaseStation, sensor, phenomenon)
    
    for (xmlObject <- 
     getValueArrayType(compositeObservationDocument).getValueComponents().getAbstractValueArray();
     val compositeValue = xmlObject.asInstanceOf[CompositeValueType]) {

      val calendar = getTime(compositeValue.getValueComponents()) match {
        case Some(time) => time
        case None => throw new Exception("Date not found")
      }

      if (calendar.after(startDate)) {
        for (namedQuantityType <- getValues(compositeValue.getValueComponents())) {
          observationValuesCollection.find(observationValues =>
            observationValues.observedProperty.foreign_tag == namedQuantityType.getName()) match {
            case Some(sensorObservationValue) => sensorObservationValue.addValue(namedQuantityType.getDoubleValue(), calendar)
            case None => {
              val observedProperty = observationValuesCollection.find(observationValues =>
                observationValues.observedProperty.foreign_tag == namedQuantityType.getName()) match {
                case Some(observationValues) => {
                  observationValues.addValue(namedQuantityType.getDoubleValue(), calendar)
                }
                case None => //println("namedQuantityType.getName(): " + namedQuantityType.getName())
              }
            }
          }
        }
      }
    }

    return createObservationCollection(databaseStation, observationValuesCollection)
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

  private def getValueArrayType(compositeObservationDocument: CompositeObservationDocument):ValueArrayType={
    val compositePropertyType: CompositePropertyType =
      compositeObservationDocument.getObservation().getResult().asInstanceOf[CompositePropertyType]

    val valueArrayType: ValueArrayType =
      compositePropertyType.getComposite().getValueComponents().getAbstractValueArray(1).asInstanceOf[ValueArrayType]

    val compositeValueType: CompositeValueType =
      valueArrayType.getValueComponents().getAbstractValueArray(0).asInstanceOf[CompositeValueType]

    val valueArrayType1: ValueArrayType =
      compositeValueType.getValueComponents().getAbstractValueArray(1).asInstanceOf[ValueArrayType]
    
    return valueArrayType1
  }
  
  private def getValues(valuePropertyType: ValueArrayPropertyType): List[NamedQuantityType] = {
    val compositeValueType = valuePropertyType.getAbstractValueArray(1).
      asInstanceOf[CompositeValueType]

    val values = compositeValueType.getValueComponents().getAbstractValueArray().collect{
      case namedQuantity: NamedQuantityType => namedQuantity}

    return values.toList
  }

  private def getTime(valuePropertyType: ValueArrayPropertyType): Option[Calendar] = {
    val calendarOptions = for {xmlObject <- valuePropertyType.getAbstractValueArray()} yield{
      xmlObject match {
        case compositeValueType: CompositeValueType if (compositeValueType.getValueComponents().getAbstractTimeObjectArray().length == 1) => {
          val timeInstantType =
            compositeValueType.getValueComponents().getAbstractTimeObjectArray()(0).asInstanceOf[TimeInstantType]

          val calendar = createDate(timeInstantType.getTimePosition().getObjectValue().asInstanceOf[Calendar])

          Some(calendar)
        }
        case _ => None
      }
    }
    val calendars = calendarOptions.filter(_.nonEmpty)
    
    if(calendars.length > 0){
      return calendars(0)
    }
    else
    {
      return None 
    }
  }
  
  private def createDate(xmlCalendar: Calendar): Calendar = {
    val calendar = Calendar.getInstance(xmlCalendar.getTimeZone())
    calendar.set(Calendar.YEAR, xmlCalendar.get(Calendar.YEAR))
    calendar.set(Calendar.MONTH, xmlCalendar.get(Calendar.MONTH))
    calendar.set(Calendar.DAY_OF_MONTH, xmlCalendar.get(Calendar.DAY_OF_MONTH))
    calendar.set(Calendar.HOUR_OF_DAY, xmlCalendar.get(Calendar.HOUR_OF_DAY))
    calendar.set(Calendar.MINUTE, xmlCalendar.get(Calendar.MINUTE))
    calendar.set(Calendar.SECOND, 0)
    
    // The time is not able to be changed from the 
    //setTimezone if this is not set. Java Error
    calendar.getTime()

    return calendar
  }
  
  private def createCompositeObservationDocument(data: String): 
      Option[CompositeObservationDocument] = {
    try {
      val compositeObservationDocument =
        CompositeObservationDocument.Factory.parse(data)

      return Some[CompositeObservationDocument](compositeObservationDocument)
    } catch {
      case e: Exception => e.printStackTrace()
    }

    return None;
  }
}