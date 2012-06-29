package com.axiomalaska.sos.source.observationretriever

import java.util.Calendar
import java.util.TimeZone
import java.text.SimpleDateFormat
import java.util.Date
import javax.xml.namespace.QName
import net.opengis.gml.TimePeriodType
import net.opengis.ogc.BinaryTemporalOpType
import net.opengis.sos.x10.GetObservationDocument
import scala.collection.JavaConversions._
import com.axiomalaska.sos.tools.HttpSender

import org.apache.log4j.Logger

class SosRawDataRetriever(private val logger: Logger = Logger.getRootLogger()) {
  private val formatDate = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
  private val httpSender = new HttpSender()
  
  def getRawData(serviceUrl:String, offeringTag:String, observedPropertyTag:String, 
      stationForeignId:String, observedProperty:String, 
      startDate: Calendar, endDate: Calendar): String = {
    
    val copyStartDate = getDateObjectInGMTTime(startDate)
    val copyEndDate = getDateObjectInGMTTime(endDate)
        
    val beginPosition = formatDate.format(copyStartDate);
    val endPosition = formatDate.format(copyEndDate);

    val offering = offeringTag + stationForeignId;
    val observedPropertyFullName = observedPropertyTag + observedProperty
    val responseFormat = "text/xml;schema=\"ioos/0.6.1\"";

    val xb_getObsDoc = GetObservationDocument.Factory.newInstance();

    val xb_getObs = xb_getObsDoc.addNewGetObservation();

    val obsCursor = xb_getObs.newCursor();
    obsCursor.toFirstContentToken();
    obsCursor.insertNamespace("om", "http://www.opengis.net/om/1.0");
    obsCursor.insertNamespace("sos", "http://www.opengis.net/sos/1.0");
    obsCursor.insertNamespace("gml", "http://www.opengis.net/gml");
    obsCursor.insertNamespace("ogc", "http://www.opengis.net/ogc");
    obsCursor.insertNamespace("xsi",
      "http://www.w3.org/2001/XMLSchema-instance");

    //
    // set required elements:
    //
    xb_getObs.setService("SOS");

    xb_getObs.setVersion("1.0.0");

    xb_getObs.setOffering(offering);

    xb_getObs.setResponseFormat(responseFormat);

    val observedPropertyFullNames = new Array[String](1)
    observedPropertyFullNames(0) = observedPropertyFullName
    xb_getObs.setObservedPropertyArray(observedPropertyFullNames);

    val xb_binTempOp = BinaryTemporalOpType.Factory.newInstance();

    xb_binTempOp.addNewPropertyName();
    var cursor = xb_binTempOp.newCursor();
    cursor.toChild(new QName("http://www.opengis.net/ogc", "PropertyName"));
    cursor.setTextValue("om:samplingTime");

    val xb_timePeriod = TimePeriodType.Factory.newInstance();

    val xb_beginPosition = xb_timePeriod.addNewBeginPosition();
    xb_beginPosition.setStringValue(beginPosition);

    val xb_endPosition = xb_timePeriod.addNewEndPosition();
    xb_endPosition.setStringValue(endPosition);

    xb_binTempOp.setTimeObject(xb_timePeriod);

    val eventTime = xb_getObs.addNewEventTime();
    eventTime.setTemporalOps(xb_binTempOp);

    // rename elements:
    cursor = eventTime.newCursor();
    cursor.toChild(new QName("http://www.opengis.net/ogc", "temporalOps"));
    cursor.setName(new QName("http://www.opengis.net/ogc", "TM_During"));

    cursor.toChild(new QName("http://www.opengis.net/gml", "_TimeObject"));
    cursor.setName(new QName("http://www.opengis.net/gml", "TimePeriod"));

    xb_getObs.setResultModel((new QName("http://www.opengis.net/gml",
      "Observation")));

    var request = xb_getObsDoc.xmlText();
    request = request.replace("gml=\"http://www.opengis.net/gml\"",
      "gml=\"http://www.opengis.net/gml/3.2\"");

    val results = httpSender.sendPostMessage(serviceUrl, request);

    return results;
  }
  
  def getRawDataLatest(serviceUrl:String, offeringTag:String, observedPropertyTag:String, 
      stationForeignId:String, observedProperty:String): String = {

    val offering = offeringTag + stationForeignId;
    val observedPropertyFullName = observedPropertyTag + observedProperty
    val responseFormat = "text/xml;schema=\"ioos/0.6.1\"";

    val xb_getObsDoc = GetObservationDocument.Factory.newInstance();

    val xb_getObs = xb_getObsDoc.addNewGetObservation();

    val obsCursor = xb_getObs.newCursor();
    obsCursor.toFirstContentToken();
    obsCursor.insertNamespace("om", "http://www.opengis.net/om/1.0");
    obsCursor.insertNamespace("sos", "http://www.opengis.net/sos/1.0");
    obsCursor.insertNamespace("gml", "http://www.opengis.net/gml");
    obsCursor.insertNamespace("ogc", "http://www.opengis.net/ogc");
    obsCursor.insertNamespace("xsi",
      "http://www.w3.org/2001/XMLSchema-instance");

    //
    // set required elements:
    //
    xb_getObs.setService("SOS");

    xb_getObs.setVersion("1.0.0");

    xb_getObs.setOffering(offering);

    xb_getObs.setResponseFormat(responseFormat);

    val observedPropertyFullNames = new Array[String](1)
    observedPropertyFullNames(0) = observedPropertyFullName
    xb_getObs.setObservedPropertyArray(observedPropertyFullNames);

    xb_getObs.setResultModel((new QName("http://www.opengis.net/gml",
      "Observation")));

    var request = xb_getObsDoc.xmlText();
    request = request.replace("gml=\"http://www.opengis.net/gml\"",
      "gml=\"http://www.opengis.net/gml/3.2\"");

    val results = httpSender.sendPostMessage(serviceUrl, request);

    return results;
  }
  
  // ---------------------------------------------------------------------------
  // Private Members
  // ---------------------------------------------------------------------------

  private def getDateObjectInGMTTime(calendar:Calendar):Date={
    val copyCalendar = calendar.clone().asInstanceOf[Calendar]
    copyCalendar.setTimeZone(TimeZone.getTimeZone("GMT"))
    val localCalendar = Calendar.getInstance()
    localCalendar.set(Calendar.YEAR, copyCalendar.get(Calendar.YEAR))
    localCalendar.set(Calendar.MONTH, copyCalendar.get(Calendar.MONTH))
    localCalendar.set(Calendar.DAY_OF_MONTH, copyCalendar.get(Calendar.DAY_OF_MONTH))
    localCalendar.set(Calendar.HOUR_OF_DAY,copyCalendar.get(Calendar.HOUR_OF_DAY))
    localCalendar.set(Calendar.MINUTE, copyCalendar.get(Calendar.MINUTE))
    localCalendar.set(Calendar.SECOND, copyCalendar.get(Calendar.SECOND))
    
    return localCalendar.getTime()
  }
}