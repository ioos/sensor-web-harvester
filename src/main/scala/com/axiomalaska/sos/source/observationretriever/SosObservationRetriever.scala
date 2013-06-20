package com.axiomalaska.sos.source.observationretriever

import com.axiomalaska.sos.source.StationQuery
import java.util.Calendar
import com.axiomalaska.sos.source.data.LocalStation
import net.opengis.ows.x11.ExceptionReportDocument
//import net.opengis.om.x10.CompositeObservationDocument
import scala.collection.mutable
//import gov.noaa.ioos.x061.CompositePropertyType
//import gov.noaa.ioos.x061.ValueArrayType
//import gov.noaa.ioos.x061.CompositeValueType
//import gov.noaa.ioos.x061.NamedQuantityType
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

abstract class SosObservationRetriever(private val stationQuery:StationQuery) 
	extends ObservationValuesCollectionRetriever {
  
  // ---------------------------------------------------------------------------
  // Private Data
  // ---------------------------------------------------------------------------
  private val LOGGER = Logger.getLogger(getClass())
  private val sosRawDataRetriever = new SosRawDataRetriever()
  private val phenomena = stationQuery.getAllPhenomena
  protected val serviceUrl:String
  
  // ---------------------------------------------------------------------------
  // ObservationValuesCollectionRetriever Members
  // ---------------------------------------------------------------------------
  
  def getObservationValues(station: LocalStation, sensor: LocalSensor, 
      phenomenon: LocalPhenomenon, startDate: DateTime):List[ObservationValues] ={

    val thirdyDaysOld = DateTime.now()
    thirdyDaysOld.minusDays(-30)
    
    val adjustedStartDate = if(startDate.isBefore(thirdyDaysOld)){
      thirdyDaysOld
    }
    else{
      startDate
    }
    
    val sensorForeignId = getSensorForeignId(phenomenon)
    
    val rawData = sosRawDataRetriever.getRawData(serviceUrl,
          station.databaseStation.foreign_tag,
          sensorForeignId, adjustedStartDate, DateTime.now())
    
//    createCompositeObservationDocument(rawData) match {
//      case Some(compositeObservationDocument) => {
//        return buildSensorObservationValues(compositeObservationDocument, 
//            station, sensor, phenomenon, startDate)
//      }
//      case None => {
////        val exceptionDocument =
////          ExceptionReportDocument.Factory.parse(rawData);
////
////        val fullMessage = exceptionDocument.getExceptionReport().
////          getExceptionArray()(0).getExceptionTextArray().mkString(", ");
//        
//        return Nil
//      }
//    }
    
    return Nil
  }
  
  // ---------------------------------------------------------------------------
  // Private Members
  // ---------------------------------------------------------------------------
  
  private def getSensorForeignId(phenomenon: Phenomenon):String = {
    val localPhenomenon = phenomenon.asInstanceOf[LocalPhenomenon]
    
    val phenom = phenomena.foldLeft("")((ret,phen) => if (phen.id == localPhenomenon.databasePhenomenon.id) localPhenomenon.databasePhenomenon.tag else ret)
    
    return phenom
    
//    localPhenomenon.databasePhenomenon.id match{
//      case SensorPhenomenonIds.BAROMETRIC_PRESSURE =>{
//        "http://mmisw.org/ont/cf/parameter/air_pressure"
//      }
//      case SensorPhenomenonIds.AIR_TEMPERATURE =>{
//        "http://mmisw.org/ont/cf/parameter/air_temperature"
//      }
//      case SensorPhenomenonIds.SEA_WATER_TEMPERATURE =>{
//        "http://mmisw.org/ont/cf/parameter/sea_water_temperature"
//      }
//      case SensorPhenomenonIds.CURRENT_DIRECTION =>{
//        "http://mmisw.org/ont/cf/parameter/currents"
//      }
//      case SensorPhenomenonIds.CURRENT_SPEED =>{
//        "http://mmisw.org/ont/cf/parameter/currents"
//      }
//      case SensorPhenomenonIds.WATER_LEVEL_PREDICTIONS =>{
//        "http://mmisw.org/ont/cf/parameter/sea_surface_height_amplitude_due_to_equilibrium_ocean_tide"
//      }
//      case SensorPhenomenonIds.WATER_LEVEL =>{
//        "http://mmisw.org/ont/cf/parameter/water_surface_height_above_reference_datum"
//      }
//      case SensorPhenomenonIds.WIND_DIRECTION =>{
//        "http://mmisw.org/ont/cf/parameter/winds"
//      }
//      case SensorPhenomenonIds.WIND_GUST =>{
//        "http://mmisw.org/ont/cf/parameter/winds"
//      }
//      case SensorPhenomenonIds.WIND_SPEED =>{
//        "http://mmisw.org/ont/cf/parameter/winds"
//      }
//      case SensorPhenomenonIds.WIND_GUST_DIRECTION =>{
//        "http://mmisw.org/ont/cf/parameter/winds"
//      }
//      case SensorPhenomenonIds.WIND_WAVE_DIRECTION =>{
//        "http://mmisw.org/ont/cf/parameter/waves"
//      }
//      case SensorPhenomenonIds.WIND_WAVE_PERIOD =>{
//        "http://mmisw.org/ont/cf/parameter/waves"
//      }
//      case SensorPhenomenonIds.WIND_WAVE_HEIGHT =>{
//        "http://mmisw.org/ont/cf/parameter/waves"
//      }
//      case SensorPhenomenonIds.SWELL_PERIOD =>{
//        "http://mmisw.org/ont/cf/parameter/waves"
//      }
//      case SensorPhenomenonIds.SWELL_HEIGHT =>{
//        "http://mmisw.org/ont/cf/parameter/waves"
//      }
//      case SensorPhenomenonIds.SWELL_WAVE_DIRECTION =>{
//        "http://mmisw.org/ont/cf/parameter/waves"
//      }
//      case SensorPhenomenonIds.DOMINANT_WAVE_DIRECTION =>{
//        "http://mmisw.org/ont/cf/parameter/waves"
//      }
//      case SensorPhenomenonIds.DOMINANT_WAVE_PERIOD =>{
//        "http://mmisw.org/ont/cf/parameter/waves"
//      }
//      case SensorPhenomenonIds.SIGNIFICANT_WAVE_HEIGHT =>{
//        "http://mmisw.org/ont/cf/parameter/waves"
//      }
//      case SensorPhenomenonIds.AVERAGE_WAVE_PERIOD =>{
//        "http://mmisw.org/ont/cf/parameter/waves"
//      }
//      case SensorPhenomenonIds.SALINITY =>{
//        "http://mmisw.org/ont/cf/parameter/sea_water_salinity"
//      }
//      case SensorPhenomenonIds.WIND_VERTICAL_VELOCITY =>{
//        "http://mmisw.org/ont/cf/parameter/winds"
//      }
//    }
  }
  
//  private def buildSensorObservationValues(
//    compositeObservationDocument: CompositeObservationDocument, 
//    station: LocalStation, sensor: LocalSensor, phenomenon: LocalPhenomenon, 
//    startDate: DateTime): List[ObservationValues] = {
//      
//    val observationValuesCollection = createSensorObservationValuesCollection(
//        station, sensor, phenomenon)
    
//    for (xmlObject <- 
//     getValueArrayType(compositeObservationDocument).getValueComponents().getAbstractValueArray();
//     val compositeValue = xmlObject.asInstanceOf[CompositeValueType]) {
//
//      val calendar = getTime(compositeValue.getValueComponents()) match {
//        case Some(time) => time
//        case None => throw new Exception("Date not found")
//      }
//
//      if (calendar.isAfter(startDate)) {
//        for (namedQuantityType <- getValues(compositeValue.getValueComponents())) {
//          observationValuesCollection.find(observationValues =>
//            observationValues.observedProperty.foreign_tag == 
//              namedQuantityType.getName()) match {
//            case Some(sensorObservationValue) => 
//              sensorObservationValue.addValue(namedQuantityType.getDoubleValue(), calendar)
//            case None => {
//              val observedProperty = observationValuesCollection.find(observationValues =>
//                observationValues.observedProperty.foreign_tag == namedQuantityType.getName()) match {
//                case Some(observationValues) => {
//                  observationValues.addValue(namedQuantityType.getDoubleValue(), calendar)
//                }
//                case None => //println("namedQuantityType.getName(): " + namedQuantityType.getName())
//              }
//            }
//          }
//        }
//      }
//    }

//    observationValuesCollection
//  }
  
  private def createSensorObservationValuesCollection(station: LocalStation, sensor: LocalSensor,
    phenomenon: LocalPhenomenon): List[ObservationValues] = {
    val observedProperties = stationQuery.getObservedProperties(
      station.databaseStation, sensor.databaseSensor, phenomenon.databasePhenomenon)

    for (observedProperty <- observedProperties) yield {
      new ObservationValues(observedProperty, sensor, phenomenon, observedProperty.foreign_units)
    }
  }

//  private def getValueArrayType(compositeObservationDocument: CompositeObservationDocument):ValueArrayType={
//    val compositePropertyType: CompositePropertyType =
//      compositeObservationDocument.getObservation().getResult().asInstanceOf[CompositePropertyType]
//
//    val valueArrayType: ValueArrayType =
//      compositePropertyType.getComposite().getValueComponents().getAbstractValueArray(1).asInstanceOf[ValueArrayType]
//
//    val compositeValueType: CompositeValueType =
//      valueArrayType.getValueComponents().getAbstractValueArray(0).asInstanceOf[CompositeValueType]
//
//    val valueArrayType1: ValueArrayType =
//      compositeValueType.getValueComponents().getAbstractValueArray(1).asInstanceOf[ValueArrayType]
//    
//    return valueArrayType1
//  }
//  
//  private def getValues(valuePropertyType: ValueArrayPropertyType): List[NamedQuantityType] = {
//    val compositeValueType = valuePropertyType.getAbstractValueArray(1).
//      asInstanceOf[CompositeValueType]
//
//    val values = compositeValueType.getValueComponents().getAbstractValueArray().collect{
//      case namedQuantity: NamedQuantityType => namedQuantity}
//
//    return values.toList
//  }
//
//  private def getTime(valuePropertyType: ValueArrayPropertyType): Option[DateTime] = {
//    val calendarOptions = for {xmlObject <- valuePropertyType.getAbstractValueArray()} yield{
//      xmlObject match {
//        case compositeValueType: CompositeValueType 
//        	if (compositeValueType.getValueComponents().getAbstractTimeObjectArray().length == 1) => {
//          val timeInstantType =
//            compositeValueType.getValueComponents().getAbstractTimeObjectArray()(0).asInstanceOf[TimeInstantType]
//
//          val calendar = createDate(timeInstantType.getTimePosition().getObjectValue().asInstanceOf[Calendar])
//
//          Some(calendar)
//        }
//        case _ => None
//      }
//    }
//    val calendars = calendarOptions.filter(_.nonEmpty)
//    
//    if(calendars.length > 0){
//      return calendars(0)
//    }
//    else
//    {
//      return None 
//    }
//  }
  
  private def createDate(xmlCalendar: Calendar): DateTime = {
    new DateTime(xmlCalendar.get(Calendar.YEAR), 
        xmlCalendar.get(Calendar.MONTH)+1, xmlCalendar.get(Calendar.DAY_OF_MONTH), 
        xmlCalendar.get(Calendar.HOUR_OF_DAY), xmlCalendar.get(Calendar.MINUTE), 0,
        DateTimeZone.forID(xmlCalendar.getTimeZone().getID()))
  }

//  private def createCompositeObservationDocument(data: String): Option[CompositeObservationDocument] = {
//    if (data != null && !data.contains("ExceptionReport")) {
//      try {
//        val compositeObservationDocument =
//          CompositeObservationDocument.Factory.parse(data)
//
//        return Some[CompositeObservationDocument](compositeObservationDocument)
//      } catch {
//        case e: Exception => println("error parsing data")
//      }
//    }
//
//    return None;
//  }
}