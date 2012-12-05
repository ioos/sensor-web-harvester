/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.axiomalaska.sos.source.isowriter

import com.axiomalaska.sos.source.StationQuery
import com.axiomalaska.sos.source.data.LocalStation
import com.axiomalaska.sos.tools.HttpSender
import java.io.File
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.log4j.Logger

class NdbcIsoWriter(private val stationQuery:StationQuery, 
    private val isoDirectory: String,
    private val logger: Logger = Logger.getRootLogger()) extends ISOWriter {

  private val currentDate = Calendar.getInstance
  private val httpSender = new HttpSender()
  private val dateParser = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
  private val ndbcDSURL = "http://sdftest.ndbc.noaa.gov/sos/server.php?request=DescribeSensor&service=SOS&version=1.0.0&outputformat=text/xml;subtype=%22sensorML/1.0.1%22&procedure=urn:ioos:station:wmo:"
  
  
  def writeISOFile(station: LocalStation, sosURL: String) = {
    // we need the following for the station in order to write the template:
    // name (which is actually going to be the station's URN)
    val stId = "urn:ioos:station:" + station.getNetworks.get(0).getId + ":" + station.getId
    // latitude
    val latitude = station.getLocation.getLatitude
    // longitude
    val longitude = station.getLocation.getLongitude
    // date of earliest available data (this is probably gonna be tricky)
    // date of last available data (if not currently updated)
    val stDateTimes = getDateTimesForStation(station, sosURL)
    stDateTimes.map(dt => logger info "have time for station: " + dt.toString)
    // current date
    val date = (currentDate.get(Calendar.MONTH)+1) + "-" + currentDate.get(Calendar.DAY_OF_MONTH) + "-" + currentDate.get(Calendar.YEAR)
    // abstract
    val stAbstract = getStationDescription(station.getId)
    // template in xml form
    val xml = readInTemplate
    xml match {
      case Some(xml) => {
          val modifiedXML = writeExtents(xml, latitude, longitude, date, stId, stDateTimes, stAbstract, station.getName)
          scala.xml.XML.save(isoDirectory + "/" + station.getId + ".xml", modifiedXML.head)
          logger info "wrote to" + isoDirectory + "/" + station.getId + ".xml"
      }
    }
  }
  
//  implicit def addCopyToNode(elem: scala.xml.Node) = new {
//    def copy(label: String = elem.label, text: String = elem.text, namespace: String = elem.namespace) : scala.xml.Node =
//      scala.xml.Elem()
//  }
  
  private def writeExtents(xml: scala.xml.Elem, latitude: Double, longitude: Double,
                           todate: String, stID: String, datetimes: List[Calendar],
                           stAbstract: String, stName: String) : Seq[scala.xml.Node] = {
    logger info "writing latitude: " + latitude.toString + " longitude: " + longitude.toString + " date: " + todate
    new scala.xml.transform.RewriteRule {
      override def transform(n: scala.xml.Node): scala.xml.Node = n match {
        case lon: scala.xml.Elem if lon.label.toLowerCase.contains("longitude") => writeGCODecimal(lon, longitude)
        case lat: scala.xml.Elem if lat.label.toLowerCase.contains("latitude") => writeGCODecimal(lat, latitude)
        case pos: scala.xml.Elem if pos.label.toLowerCase.equals("ex_temporalextent") => writeGMLPositions(pos, datetimes.head, datetimes.last)
        case dt: scala.xml.Elem if dt.label.toLowerCase.equals("ci_date") => writeGCODateTime(dt, currentDate)
        case d: scala.xml.Elem if d.label.toLowerCase.equals("datestamp") => writeGCODate(d, currentDate)
        case fi: scala.xml.Elem if fi.label.toLowerCase.equals("fileidentifier") => writeGCOCharacterString(fi, stID)
        case title: scala.xml.Elem if title.label.toLowerCase.equals("title") => writeGCOCharacterString(title, stName)
        case abst: scala.xml.Elem if abst.label.toLowerCase.equals("abstract") => writeGCOCharacterString(abst, stAbstract)
        case auth: scala.xml.Elem if auth.label.toLowerCase.equals("authority") => auth
        case elem: scala.xml.Elem => elem copy(child = elem.child flatMap (this transform))
        case other => other
      }
    } transform xml
  }
  
  private def writeGCODecimal(elem: scala.xml.Elem, decimal: Double) : scala.xml.Node = {
    new scala.xml.transform.RewriteRule {
      override def transform(n: scala.xml.Node): scala.xml.Node = n match {
        case dec: scala.xml.Node if dec.label.toLowerCase.equals("decimal") =>
          <gco:Decimal>{decimal.toString}</gco:Decimal>
        case elem: scala.xml.Elem => elem copy(child = elem.child flatMap (this transform))
        case other => other
      }
    } transform elem head
  }
  
  private def writeGMLPositions(elem: scala.xml.Elem, datetimeStart: Calendar, datetimeEnd: Calendar) : scala.xml.Node = {
    new scala.xml.transform.RewriteRule {
      override def transform(n: scala.xml.Node): scala.xml.Node = n match {
        case start: scala.xml.Node if start.label.toLowerCase.equals("beginposition") =>
          <gml:beginPosition>{formatDateTime(datetimeStart)}</gml:beginPosition>
        case end: scala.xml.Node if end.label.toLowerCase.equals("endposition") =>
          <gml:endPosition>{formatDateTime(datetimeEnd)}</gml:endPosition>
        case elem: scala.xml.Elem => elem copy(child = elem.child flatMap (this transform))
        case other => other
      }
    } transform elem head
  }
  
  private def writeGCODateTime(elem: scala.xml.Elem, datetime: Calendar) : scala.xml.Node = {
    new scala.xml.transform.RewriteRule {
      override def transform(n: scala.xml.Node): scala.xml.Node = n match {
        case dec: scala.xml.Node if dec.label.toLowerCase.equals("datetime") =>
          <gco:DateTime>{formatDateTime(datetime)}</gco:DateTime>
        case elem: scala.xml.Elem => elem copy(child = elem.child flatMap (this transform))
        case other => other
      }
    } transform elem head
  }
  
  private def writeGCODate(elem: scala.xml.Elem, datetime: Calendar) : scala.xml.Node = {
    new scala.xml.transform.RewriteRule {
      override def transform(n: scala.xml.Node): scala.xml.Node = n match {
        case dec: scala.xml.Node if dec.label.toLowerCase.equals("date") =>
          <gco:Date>{formatDate(datetime)}</gco:Date>
        case elem: scala.xml.Elem => elem copy(child = elem.child flatMap (this transform))
        case other => other
      }
    } transform elem head
  }
  
  private def writeGCOCharacterString(elem: scala.xml.Elem, cstring: String) : scala.xml.Node = {
    new scala.xml.transform.RewriteRule {
      override def transform(n: scala.xml.Node): scala.xml.Node = n match {
        case dec: scala.xml.Node if dec.label.toLowerCase.equals("characterstring") =>
          <gco:CharacterString>{cstring}</gco:CharacterString>
        case elem: scala.xml.Elem => elem copy(child = elem.child flatMap (this transform))
        case other => other
      }
    } transform elem head
  }
  
  private def formatDateTime(datetime: Calendar) : String = datetime.get(Calendar.YEAR) + "-" +
    (if(datetime.get(Calendar.MONTH)+1 < 10) "0" else "") + (datetime.get(Calendar.MONTH)+1) + "-" +
      (if(datetime.get(Calendar.DAY_OF_MONTH) < 10) "0" else "") + datetime.get(Calendar.DAY_OF_MONTH) + "T" +
      (if(datetime.get(Calendar.HOUR_OF_DAY) < 10) "0" else "") +datetime.get(Calendar.HOUR_OF_DAY) + ":" +
      (if(datetime.get(Calendar.MINUTE) < 10) "0" else "") + datetime.get(Calendar.MINUTE) + ":" +
      (if(datetime.get(Calendar.SECOND) < 10) "0" else "") + datetime.get(Calendar.SECOND) + "." +
      (if(datetime.get(Calendar.MILLISECOND) < 100) (if(datetime.get(Calendar.MILLISECOND) < 10) "00" else "0") else "") +
      datetime.get(Calendar.MILLISECOND) + "Z"
  
  private def formatDate(date: Calendar) : String = date.get(Calendar.YEAR) + "-" +
    (if(date.get(Calendar.MONTH)+1 < 10) "0" else "") + (date.get(Calendar.MONTH) + 1) + "-" +
    (if(date.get(Calendar.DAY_OF_MONTH) < 10) "0" else "") + date.get(Calendar.DAY_OF_MONTH)
  
  private def getDateTimesForStation(station: LocalStation, sosURL: String) : List[Calendar] = {
    val getObsLatest = <GetObservation xmlns="http://www.opengis.net/sos/1.0"
                         xmlns:ows="http://www.opengis.net/ows/1.1" 
                          xmlns:gml="http://www.opengis.net/gml"
                          xmlns:ogc="http://www.opengis.net/ogc"
                          xmlns:om="http://www.opengis.net/om/1.0"
                          xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                          xsi:schemaLocation="http://www.opengis.net/sos/1.0 http://schemas.opengis.net/sos/1.0.0/sosGetObservation.xsd"
                          service="SOS" version="1.0.0" srsName="http://www.opengis.net/def/crs/EPSG/0/4326">
                      <offering>{station.getNetworks.get(0).getId}</offering>
                      <procedure>urn:ioos:station:{station.getNetworks.get(0).getId.replace("network-", "")}:{station.getId}</procedure>
                      <observedProperty>{station.getSensors.get(0).getPhenomena.get(0).getId}</observedProperty>
                      <responseFormat>text/xml;subtype=&quot;om/1.0.0&quot;</responseFormat>
                    </GetObservation>
    val response = httpSender.sendPostMessage(sosURL, getObsLatest.toString)
//    logger info "For request:\n" + getObsLatest.toString + "\nGot Response:\n" + response.toString
    if (response != null) {
      val xml = loadXMLFromString(response.toString)
      xml match {
        case Some(xml) => {
            // read in the sampling times
            val datetimes = for (st <- xml \\ "samplingTime" \ "TimePeriod") yield st.child.map(n => n match {
                case bp: scala.xml.Elem if bp.label.toLowerCase.equals("beginposition") => bp.text
                case ep: scala.xml.Elem if ep.label.toLowerCase.equals("endposition") => ep.text
                case _ => ""
              }).filter(p => !p.equals(""))
            // create a number of calendar instances to our datetimes
            val retvals = datetimes.flatten.map(t => Calendar.getInstance)
            // set the value of the calendars to time based on our parser
            (retvals, datetimes.flatten).zipped map ((r,d) => r.setTimeInMillis(dateParser.parse(d).getTime))
            // i think this forces the calendars to update their times
            retvals map (r => r.getTime)
            // return list of calendars
            return retvals.toList
        }
      }
    }
    Nil
  }
  
  private def getStationDescription(id: String) : String = {
    val requestUrl = ndbcDSURL + id.toLowerCase
    val response = httpSender.sendGetMessage(requestUrl)
    if (response != null) {
      logger info "For request:\n" + requestUrl + "\nGot response:\n" + response.toString
      val xml = loadXMLFromString(response.toString)
      xml match {
        case Some(xml) => {
            // get all gml:description, then return its head
            val descs = for (gml <- xml \\ "description") yield gml.text
            return if(descs.nonEmpty) descs.head else "empty abstract"
        }
      }
    }
    "empty abstract"
  }
  
  private def readInTemplate() : Option[scala.xml.Elem] = {
    try {
      Some(scala.xml.XML.loadFile(new File(isoDirectory + "/iso_template.xml")))
    } catch{
      case ex: Exception => {
          logger error "Unable to load file into xml: " + isoDirectory + "/iso_template.xml\n" + ex.toString
          None
      }
    }
  }
  
  private def loadXMLFromString(stringToLoad : String) : Option[scala.xml.Elem] = {
    try {
      Some(scala.xml.XML.loadString(stringToLoad))
    } catch{
      case ex: Exception => {
          logger.error("Unable to load string into xml: " + stringToLoad + "\n" + ex.toString)
          None
      }
    }
  }
}
