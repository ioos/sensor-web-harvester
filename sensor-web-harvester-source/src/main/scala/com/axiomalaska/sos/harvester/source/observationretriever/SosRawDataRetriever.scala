package com.axiomalaska.sos.harvester.source.observationretriever

import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Date
import java.util.TimeZone

import scala.Option.option2Iterable
import scala.collection.JavaConversions.bufferAsJavaList
import scala.collection.mutable
import scala.xml.Node

import org.apache.log4j.Logger
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import com.axiomalaska.phenomena.Phenomena
import com.axiomalaska.phenomena.Phenomenon
import com.axiomalaska.sos.harvester.data.RawValues
import com.axiomalaska.sos.tools.HttpSender

import javax.xml.namespace.QName
import net.opengis.gml.TimePeriodType
import net.opengis.ogc.BinaryTemporalOpType
import net.opengis.sos.x10.GetObservationDocument

class SosRawDataRetriever() {
  private val formatDate = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
  private val LOGGER = Logger.getLogger(getClass())
  private val dateParser = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ssZ")
  private case class NamedQuantity(name:String, value:Double)
  
  def getRawData(serviceUrl:String, 
      stationPostFixName:String, observedProperty:String, 
      startDate: DateTime, endDate: DateTime): String = {

    LOGGER.debug("SNO-RAW: Collecting for station - " + stationPostFixName)
    
    val copyStartDate = getDateObjectInGMTTime(startDate)
    val copyEndDate = getDateObjectInGMTTime(endDate)
        
    val beginPosition = formatDate.format(copyStartDate);
    val endPosition = formatDate.format(copyEndDate);

    val offering = "urn:ioos:station:" + stationPostFixName;
    
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
    observedPropertyFullNames(0) = observedProperty
    xb_getObs.setObservedPropertyArray(observedPropertyFullNames);

    val xb_binTempOp = BinaryTemporalOpType.Factory.newInstance();

    xb_binTempOp.addNewPropertyName();
    val cursor1 = xb_binTempOp.newCursor();
    cursor1.toChild(new QName("http://www.opengis.net/ogc", "PropertyName"));
    cursor1.setTextValue("om:samplingTime");

    val xb_timePeriod = TimePeriodType.Factory.newInstance();

    val xb_beginPosition = xb_timePeriod.addNewBeginPosition();
    xb_beginPosition.setStringValue(beginPosition);

    val xb_endPosition = xb_timePeriod.addNewEndPosition();
    xb_endPosition.setStringValue(endPosition);

    xb_binTempOp.setTimeObject(xb_timePeriod);

    val eventTime = xb_getObs.addNewEventTime();
    eventTime.setTemporalOps(xb_binTempOp);

    // rename elements:
    val cursor2 = eventTime.newCursor();
    cursor2.toChild(new QName("http://www.opengis.net/ogc", "temporalOps"));
    cursor2.setName(new QName("http://www.opengis.net/ogc", "TM_During"));

    cursor2.toChild(new QName("http://www.opengis.net/gml", "_TimeObject"));
    cursor2.setName(new QName("http://www.opengis.net/gml", "TimePeriod"));

    xb_getObs.setResultModel((new QName("http://www.opengis.net/gml",
      "Observation")));

    val request = xb_getObsDoc.xmlText().replace("gml=\"http://www.opengis.net/gml\"",
      "gml=\"http://www.opengis.net/gml/3.2\"");

    val results = HttpSender.sendPostMessage(serviceUrl, request);

    return results;
  }
  
  def getRawDataLatest(serviceUrl:String, 
      stationPostFixName:String, 
      observedProperty:String): String = {

    val offering = "urn:ioos:station:" + stationPostFixName;
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
    observedPropertyFullNames(0) = observedProperty
    xb_getObs.setObservedPropertyArray(observedPropertyFullNames);

    xb_getObs.setResultModel((new QName("http://www.opengis.net/gml",
      "Observation")));

    val request = xb_getObsDoc.xmlText().replace("gml=\"http://www.opengis.net/gml\"",
      "gml=\"http://www.opengis.net/gml/3.2\"");

    val results = HttpSender.sendPostMessage(serviceUrl, request)

    return results;
  }
  
  /**
   * The SOS that we are pulling the data from has different Phenomenon URLs than the SOS
   * we are placing the data into. For example the pulling data SOS has one URL for
   * wind where the SOS we are pushing data into has three wind_speed, wind_direction, and wind_gust
   */
  def getSensorForeignId(phenomenon: Phenomenon): String = {

    if (phenomenon.getTag == Phenomena.instance.AIR_PRESSURE.getTag) {
      "http://mmisw.org/ont/cf/parameter/air_pressure"
    } else if (phenomenon.getTag == Phenomena.instance.AIR_TEMPERATURE.getTag) {
      "http://mmisw.org/ont/cf/parameter/air_temperature"
    } else if (phenomenon.getTag == Phenomena.instance.SEA_WATER_TEMPERATURE.getTag) {
      "http://mmisw.org/ont/cf/parameter/sea_water_temperature"
    } else if (phenomenon.getTag == Phenomena.instance.CURRENT_DIRECTION.getTag ||
      phenomenon.getTag == Phenomena.instance.CURRENT_SPEED.getTag) {
      "http://mmisw.org/ont/cf/parameter/currents"
    } else if (phenomenon.getTag == Phenomena.instance.SEA_SURFACE_HEIGHT_ABOVE_SEA_LEVEL.getTag) {
      "http://mmisw.org/ont/cf/parameter/water_surface_height_above_reference_datum"
    } else if (phenomenon.getTag ==
      Phenomena.instance.SEA_SURFACE_HEIGHT_AMPLITUDE_DUE_TO_GEOCENTRIC_OCEAN_TIDE.getTag) {
      "http://mmisw.org/ont/cf/parameter/sea_surface_height_amplitude_due_to_equilibrium_ocean_tide"
    } else if (phenomenon.getTag == Phenomena.instance.WIND_FROM_DIRECTION.getTag ||
      phenomenon.getTag == Phenomena.instance.WIND_SPEED_OF_GUST.getTag ||
      phenomenon.getTag == Phenomena.instance.WIND_SPEED.getTag ||
      phenomenon.getTag == Phenomena.instance.WIND_GUST_FROM_DIRECTION.getTag ||
      phenomenon.getTag == Phenomena.instance.WIND_VERTICAL_VELOCITY.getTag) {
      "http://mmisw.org/ont/cf/parameter/winds"
    } else if (phenomenon.getTag == Phenomena.instance.SALINITY.getTag()) {
      "http://mmisw.org/ont/cf/parameter/sea_water_salinity"
    } else if (phenomenon.getTag == Phenomena.instance.SEA_SURFACE_SWELL_WAVE_TO_DIRECTION.getTag ||
      phenomenon.getTag == Phenomena.instance.SEA_SURFACE_WIND_WAVE_TO_DIRECTION.getTag ||
      phenomenon.getTag == Phenomena.instance.SEA_SURFACE_WIND_WAVE_PERIOD.getTag ||
      phenomenon.getTag == Phenomena.instance.SEA_SURFACE_WIND_WAVE_SIGNIFICANT_HEIGHT.getTag ||
      phenomenon.getTag == Phenomena.instance.SEA_SURFACE_SWELL_WAVE_PERIOD.getTag ||
      phenomenon.getTag == Phenomena.instance.SEA_SURFACE_SWELL_WAVE_SIGNIFICANT_HEIGHT.getTag ||
      phenomenon.getTag == Phenomena.instance.SEA_SURFACE_SWELL_WAVE_TO_DIRECTION.getTag ||
      phenomenon.getTag == Phenomena.instance.SEA_SURFACE_DOMINANT_WAVE_TO_DIRECTION.getTag ||
      phenomenon.getTag == Phenomena.instance.DOMINANT_WAVE_PERIOD.getTag ||
      phenomenon.getTag == Phenomena.instance.SIGNIFICANT_WAVE_HEIGHT.getTag ||
      phenomenon.getTag == Phenomena.instance.MEAN_WAVE_PERIOD.getTag) {
      "http://mmisw.org/ont/cf/parameter/waves"
    } else {
      throw new Exception("Sensor Foreign Id not found: " + phenomenon.getTag())
    }
  }
  
  def createCurrentsRawValues(rawData: String, 
      phenomenonForeignTags: List[String]): List[RawValues] = {
    val xmlResult = scala.xml.XML.loadString(rawData)
    xmlResult.find(node => node.label == "CompositeObservation") match {
      case Some(compositeObservationDocument) ⇒ {

        val binsXml = xmlResult \ "procedure" \ "Process" \ "CompositeContext" \ "valueComponents" \ "ContextArray" \
          "valueComponents" \ "CompositeContext" \ "valueComponents" \ "ContextArray" \
          "valueComponents" \ "CompositeContext" \ "valueComponents" \ "ContextArray" \
          "valueComponents"

        val bins = (for {
          binXml ← binsXml \ "Context"
          val name = (binXml \ "@name").toString
          val units = binXml \ "@uom"
          val value = binXml.text.toDouble
        } yield {
          (name, value)
        }).toArray

        val composites = compositeObservationDocument \ "result" \ "Composite" \ "valueComponents" \
          "Array" \ "valueComponents" \ "Composite" \ "valueComponents" \ "Array" \ "valueComponents" \ "Composite"

        val rawValuesMap = new mutable.HashMap[String, mutable.ListBuffer[(DateTime, Double)]]()

        phenomenonForeignTags.foreach(tag =>
          rawValuesMap.put(tag, new mutable.ListBuffer[(DateTime, Double)]()))

        for {
          composite <- composites
          valueComponents <- composite \ "valueComponents"
          compositeContextNode <- valueComponents \ "CompositeContext"
          val dateTime = createDate(compositeContextNode)
          binnedQuantity <- getBinnedQuantities(valueComponents, bins)
        } {
          rawValuesMap.get(binnedQuantity.name) match {
            case Some(values) =>
              values.add((dateTime, binnedQuantity.value))
            case None => // do nothing
          }
        }

        (for { (phenomenonForeignTag, values) <- rawValuesMap } yield {
          RawValues(phenomenonForeignTag, values.toList)
        }).toList
      }
      case None => {
        Nil
      }
    }
  }
  
  private case class BinnedQuantity(name:String, value:Double)
  
  private def getBinnedQuantities(valueComponentsNode:Node, bins:Array[(String, Double)]):List[BinnedQuantity] ={
    (for{(compositeValue, bin) <- (valueComponentsNode \ "ValueArray" \ "valueComponents" \ "CompositeValue").zip(bins)} yield {
      for{quantity <- compositeValue \ "valueComponents" \ "Quantity"} yield {
        val units = quantity \ "@uom"
        val name = ((quantity \ "@name") + "-" + bin._2)
        val value = quantity.text.toDouble
        
        BinnedQuantity(name, value)
      }
    }).flatten.toList
  }
  
  /**
   * Parse the raw unparsed data into DateTime, Value list.
   *
   * phenomenonForeignTag - The Phenomenon Foreign Tag
   */
  def createRawValues(rawData: String, phenomenonForeignTags: List[String]): List[RawValues] = {
    val xmlResult = scala.xml.XML.loadString(rawData)
    xmlResult.find(node => node.label == "CompositeObservation") match {
      case Some(compositeObservationDocument) => {
        val resultNode = compositeObservationDocument \ "result"

        val arrayNodeOption = (resultNode \ "Composite" \ "valueComponents" \
          "Array" \ "valueComponents" \ "Composite" \ "valueComponents" \ "Array").headOption

        val rawValuesMap = new mutable.HashMap[String, mutable.ListBuffer[(DateTime, Double)]]()

        phenomenonForeignTags.foreach(tag =>
          rawValuesMap.put(tag, new mutable.ListBuffer[(DateTime, Double)]()))

        for {
          arrayNode <- arrayNodeOption
          compositeNode <- arrayNode \\ "Composite"
          valueComponents <- (compositeNode \ "valueComponents").headOption
          compositeContextNode <- (valueComponents \ "CompositeContext").headOption
          val dateTime = createDate(compositeContextNode)
          namedQuantity <- getNamedQuantities(valueComponents)
        } {
          rawValuesMap.get(namedQuantity.name) match {
            case Some(values) =>
              values.add((dateTime, namedQuantity.value))
            case None => // do nothing
          }
        }

        (for { (phenomenonForeignTag, values) <- rawValuesMap } yield {
          RawValues(phenomenonForeignTag, values.toList)
        }).toList
      }
      case None => {
        Nil
      }
    }
  }
  
  // ---------------------------------------------------------------------------
  // Private Members
  // ---------------------------------------------------------------------------
  
  private def createDate(compositeContextNode:Node):DateTime ={
    (compositeContextNode \\ "timePosition").headOption match{
      case Some(timePositionNode) =>{
        dateParser.parseDateTime(timePositionNode.text)
      }
      case None => throw new Exception("did not find datetime")
    }
  }
  
 private def getNamedQuantities(valueComponentsNode:Node):List[NamedQuantity] ={
    (for{valueNode <- (valueComponentsNode \ "CompositeValue")
      quantity <- valueNode \\ "Quantity"
      names <- quantity.attribute("name")
      nameNode <- names.headOption
      val name = nameNode.text
      if(quantity.text.nonEmpty)
      val value = quantity.text.toDouble} yield{
      NamedQuantity(name, value)
    }).toList
  }
 
  private def getDateObjectInGMTTime(calendar:DateTime):Date={
    val copyCalendar = calendar.toCalendar(null)
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