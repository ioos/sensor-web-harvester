/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.axiomalaska.sos.harvester.iso

import com.axiomalaska.sos.harvester.StationQuery;
import com.axiomalaska.sos.harvester.data.LocalStation
import com.axiomalaska.sos.tools.HttpSender
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.log4j.Logger

// **************************************************************************
// **************************************************************************
// **************************************************************************
// **************************************************************************
// **************************************************************************
// superceded by NdbcSosIsoWriter
// **************************************************************************
// **************************************************************************
// **************************************************************************
// **************************************************************************
// **************************************************************************

class NdbcIsoWriter(private val stationQuery:StationQuery,
    private val templateFile: String,
    private val isoDirectory: String,
    private val overwrite: Boolean) 
    extends ISOWriterImpl(stationQuery, templateFile, isoDirectory, overwrite) {

  private val httpSender = new HttpSender()
  private val dateParser = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
  private val LOGGER = Logger.getLogger(getClass())
    
  private val ndbcDSURL = "http://sdftest.ndbc.noaa.gov/sos/server.php?request=GetCapabilities&service=SOS&Sections=Contents"
  private val ndbcGetCapsFile = isoDirectory + "/ndbc_getcaps_section.xml"
  private val ndbcSOSName = "National Data Buoy Center SOS"
  private val ndbcOrgName = "National Data Buoy Center"
  private val ndbcGetCapsUrl = "http://sdf.ndbc.noaa.gov/sos/server.php?request=GetCapabilities&service=SOS"
  private val ndbcDescSenUrl = "http://sdf.ndbc.noaa.gov/sos/server.php?request=DescribeSensor&service=SOS&version=1.0.0&outputformat=text/xml;subtype=%22sensorML/1.0.1%22&procedure=urn:ioos:station:wmo:"
  private val ndbcRole = "originator"
  private val ndbcGetObsUrl = "http://sdftest.ndbc.noaa.gov/sos/server.php?request=GetObservation&version=1.0.0&service=SOS&observedProperty=air_temperature&responseFormat=text%2Fxml%3Bsubtype%3D%22om%2F1.0.0%22&eventtime=latest&offering=urn:ioos:station:wmo:"
  
  private var ndbcGetCaps: scala.xml.Elem = null
  
  // overwrites ////////////////////////////////////////////////////////////////
  
  override def initialSetup(station: LocalStation) : Boolean = {
    // get the caps file
    ndbcGetCaps = readInGetCapsXML(true)
    // failed to initialize if we did not read in the get caps file
    if (ndbcGetCaps == null) {
      LOGGER error "Could not read the ndbc get capabilities document either locally or remotely!"
      return false
    }
    true
  }
  
  override def getSensorTagsAndNames(station: LocalStation) : List[(String,String)] = {
    // read through get caps and get the observedProperties of the station
    val stid = station.getId.toLowerCase.replace("wmo:", "")
    val retval = for {
      obsoff <- ndbcGetCaps \\ "ObservationOffering"
      val attrs = obsoff.attributes.filter(a => a.value.filter(v => v.text.toLowerCase.contains(stid)).nonEmpty)
      if (attrs.nonEmpty)
    } yield {
      val tn = for (obsprop <- obsoff \ "observedProperty") yield {
        val vals = obsprop.attributes.map{ _.value }.flatten
        val tags = vals.map{ _.text.trim }.filter{ _.contains("/") }
        val names = tags.map{ s => s.substring(s.lastIndexOf("/") + 1) }
        tags zip names
      }
      tn.toList.flatten
    }
    retval.toList.flatten
  }
  
  /**
   * @return string - service title, string - organisation name, string - role, string - service type, string - service url, string - service description
   */
  override def getServiceInformation(station: LocalStation) : List[ServiceIdentification] = {
    // create a service identification for SOS
    val citation = new ServiceIdentificationCitation(ndbcSOSName,ndbcOrgName,ndbcRole)
    val ops = getServiceIdOps(station)
    List(new ServiceIdentification(getStationAbstract(station),
        ndbcSOSName,station.getId,citation,getExtent(station),ops))
  }
  
  /*
    val indivName: String,
    val orgName: String,
    val phone: String,
    val address: String,
    val city: String,
    val state: String,
    val postal: String,
    val email: String,
    val webID: String,
    val webAddress: String,
    val role: String
   */
  override def getContacts(station: LocalStation) : List[Contact] = {
    val source = station.getSource
    val phone = "(228) 688-2805"
    val webID = "ndbc"
    val role = "distributor"
    List(new Contact(null,ndbcOrgName,phone,source.getAddress,source.getCity,
        source.getState,source.getZipcode,source.getEmail,webID,source.getWebAddress,role))
  }
  
  override def getFileIdentifier(station: LocalStation) : String = {
    station.databaseStation.foreign_tag
  }
  
  /*
    val idAbstract: String,
    val citation: DataIdentificationCitation,
    val keywords: List[String],
    val aggregate: DataIdentificationAggregate,
    val extent: ServiceIdentificationExtent
   */
  override def getDataIdentification(station: LocalStation): DataIdentification = {
    val idabstract = getStationAbstract(station)
    val citation = new DataIdentificationCitation(idabstract)
    val keywords = getSensorTagsAndNames(station).map(_._2)
    val agg = null
    val extent = getExtent(station)
    new DataIdentification(idabstract,citation,keywords,agg,extent)
  }
  
    override def getForeignTag(station: LocalStation) : String = {
      station.databaseStation.foreign_tag.toLowerCase.replace("wmo:", "")
    }
  
  //////////////////////////////////////////////////////////////////////////////
  
  override def getExtent(station: LocalStation): ServiceIdentificationExtent = {
    val temporals = getStationTemporalExtents(station)
    val begin = if (temporals._1 != null) formatDateTime(temporals._1) else ""
    val end = if (temporals._2 != null) formatDateTime(temporals._2) else ""
    new ServiceIdentificationExtent(station.getLocation.getY().toString, 
        station.getLocation.getX().toString,begin,end)
  }
  
  private def getStationAbstract(station: LocalStation) : String = {
    val stid = station.getId.toLowerCase.replace("wmo:", "")
    LOGGER.info(stid)
    val retval = for {
      obsoff <- ndbcGetCaps \\ "ObservationOffering"
      val attrs = obsoff.attributes.filter(a => a.value.filter(
          v => v.text.toLowerCase.contains(stid)).nonEmpty)
      if (attrs.nonEmpty)
    } yield {
      (obsoff \ "description").text.trim
    }
    retval.filter{ _.length != 0 }.head
  }    
      
  private def getStationTemporalExtents(station: LocalStation) : (Calendar, Calendar) = {
    val stid = station.getId.toLowerCase.replace("wmo:", "")
    val retval = for {
      obsoff <- ndbcGetCaps \\ "ObservationOffering"
      val attrs = obsoff.attributes.filter(a => a.value.filter(
          v => v.text.toLowerCase.contains(stid)).nonEmpty)
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
      
  private def getServiceIdOps(station: LocalStation) : List[ServiceIdentificationOperations] = {
    List(
      new ServiceIdentificationOperations("SOS Get Capabilities",ndbcGetCapsUrl,"SOS",ndbcSOSName),
      new ServiceIdentificationOperations("SOS Describe Sensor",ndbcDescSenUrl + 
          station.databaseStation.foreign_tag.toLowerCase,"SOS",ndbcSOSName),
      new ServiceIdentificationOperations("SOS Get Observation",ndbcGetObsUrl + 
          station.databaseStation.foreign_tag.toLowerCase,"SOS",ndbcSOSName)
    )
  }
  
  private def readInGetCapsXML(readFile: Boolean) : scala.xml.Elem = {
    if (readFile) {
      // try to read in xml file first
      try {
        val fxml = scala.xml.XML.load(scala.xml.Source.fromFile(ndbcGetCapsFile))
        if (fxml != null && fxml.nonEmpty)
          return fxml
      } catch {
        case ex: Exception => LOGGER warn ex.toString
      }
    }
    
    // no file, get the get caps
    LOGGER info "Could not or would not load local ndbc get caps file, requesting remote copy"
    val response = HttpSender.sendGetMessage(ndbcDSURL)
    if (response != null) {
      val xml = loadXMLFromString(response.toString)
      xml match {
        case Some(xml) => {
          // save it
          scala.xml.XML.save(ndbcGetCapsFile, xml)
          // return it
          return xml
        }
        case None => //TODO::What do we do here
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
          LOGGER.error("Unable to load string into xml: " + stringToLoad + "\n" + ex.toString)
          None
      }
    }
  }
}
