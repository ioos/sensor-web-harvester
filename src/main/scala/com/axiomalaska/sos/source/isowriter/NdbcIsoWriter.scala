/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.axiomalaska.sos.source.isowriter

import com.axiomalaska.sos.source.StationQuery
import com.axiomalaska.sos.source.data.LocalStation
import com.axiomalaska.sos.tools.HttpSender
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.log4j.Logger

class NdbcIsoWriter(private val stationQuery:StationQuery, 
    private val templateFile: String,
    private val isoDirectory: String,
    private val logger: Logger = Logger.getRootLogger()) extends ISOWriterImpl(stationQuery, templateFile, isoDirectory, logger) {

  private val httpSender = new HttpSender()
  private val dateParser = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
  
  private val ndbcDSURL = "http://sdftest.ndbc.noaa.gov/sos/server.php?request=GetCapabilities&service=SOS&Sections=Contents"
  private val ndbcGetCapsFile = isoDirectory + "/ndbc_getcaps_section.xml"
  private val ndbcServiceType = "National Data Buoy Center SOS"
  private val ndbcOrgName = "National Data Buoy Center"
  private val ndbcSOSUrl = "http://sdf.ndbc.noaa.gov/sos/"
  private val ndbcRole = "originator"
  
  
  private var ndbcGetCaps: scala.xml.Elem = null
  
  // overwrites ////////////////////////////////////////////////////////////////
  
  override def initialSetup(station: LocalStation) : Boolean = {
    // get the caps file
    ndbcGetCaps = readInGetCapsXML(true)
    // failed to initialize if we did not read in the get caps file
    if (ndbcGetCaps == null) {
      logger error "Could not read the ndbc get capabilities document either locally or remotely!"
      return false
    }
    true
  }
  
  override def getStationTemporalExtents(station: LocalStation) : (Calendar, Calendar) = {
    val stid = station.getId.toLowerCase
    val retval = for {
      obsoff <- ndbcGetCaps \\ "ObservationOffering"
      val attrs = obsoff.attributes.filter(a => a.value.filter(v => v.text.toLowerCase.contains(stid)).nonEmpty)
      if (attrs.nonEmpty)
    } yield {
      val begintime = (obsoff \ "time" \ "TimePeriod" \ "beginPosition").text.trim
      val endtime = (obsoff \ "time" \ "TimePeriod" \ "endPosition").text.trim
      // convert to calendar
      val startCal = Calendar.getInstance
      startCal.setTimeInMillis(dateParser.parse(begintime).getTime)
      if (endtime == null || endtime.equals(""))
        return (startCal, null)
      val endCal = Calendar.getInstance
      endCal.setTimeInMillis(dateParser.parse(endtime).getTime)
      (startCal, endCal)
    }
    if (retval.nonEmpty)
      return retval.head
    (null, null)
  }
  
  override def getSensorTagsAndNames(station: LocalStation) : List[(String,String)] = {
    // read through get caps and get the observedProperties of the station
    val stid = station.getId.toLowerCase
    val retval = for {
      obsoff <- ndbcGetCaps \\ "ObservationOffering"
      val attrs = obsoff.attributes.filter(a => a.value.filter(v => v.text.toLowerCase.contains(stid)).nonEmpty)
      if (attrs.nonEmpty)
    } yield {
      val tn = for (obsprop <- obsoff \ "observedProperty") yield {
        val vals = obsprop.attributes.map{ _.value }.flatten
        val tags = vals.map{ _.text.trim }.filter{ _.contains("/") }
        tags.foreach{ logger info  "have tag " + _  }
        val names = tags.map{ s => s.substring(s.lastIndexOf("/") + 1) }
        tags zip names
      }
      tn.toList.flatten
    }
    retval.toList.flatten
  }
  
  override def getStationAbstract(station: LocalStation) : String = {
    val stid = station.getId.toLowerCase
    val retval = for {
      obsoff <- ndbcGetCaps \\ "ObservationOffering"
      val attrs = obsoff.attributes.filter(a => a.value.filter(v => v.text.toLowerCase.contains(stid)).nonEmpty)
      if (attrs.nonEmpty)
    } yield {
      (obsoff \ "description").text.trim
    }
    retval.filter{ _.length != 0 }.head
  }
  
  override def checkToAddServiceIdent : Boolean = {
    true
  }
  
  /**
   * @return string - service title, string - organisation name, string - role, string - service type, string - service url, string - service description
   */
  override def getServiceInformation(station: LocalStation) : List[ServiceIdentification] = {
    (ndbcServiceType, ndbcOrgName, ndbcRole, ndbcServiceType, ndbcSOSUrl, ndbcServiceType)
  }
  
  //////////////////////////////////////////////////////////////////////////////
    
  private def readInGetCapsXML(readFile: Boolean) : scala.xml.Elem = {
    if (readFile) {
      // try to read in xml file first
      try {
        val fxml = scala.xml.XML.load(scala.xml.Source.fromFile(ndbcGetCapsFile))
        if (fxml != null && fxml.nonEmpty)
          return fxml
      } catch {
        case ex: Exception => logger warn ex.toString
      }
    }
    
    // no file, get the get caps
    logger info "Could not or would not load local ndbc get caps file, requesting remote copy"
    val response = httpSender.sendGetMessage(ndbcDSURL)
    if (response != null) {
      val xml = loadXMLFromString(response.toString)
      xml match {
        case Some(xml) => {
          // save it
          scala.xml.XML.save(ndbcGetCapsFile, xml)
          // return it
          return xml
        }
      }
    }
    
    // return null if we couldn't load a file for get it from the source
    null
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
