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
import java.util.Date
import org.apache.log4j.Logger

object StoretIsoWriter {
  private val wqpServiceName = "WQP SOS"
  private val wqpGetCaps = "GetCapabilities"
  private val wqpGetCapsUrl = "http://wqp-sos.herokuapp.com/sos?service=SOS&request=GetCapabilities"
  private val wqpGetCapsName = "SOS - GetCapabilities"
  private val wqpGetCapsDesc = "Sensor Observation Service - GetCapabilities"
  private val wqpDescSen = "DescribeSensor"
  private val wqpDescSenUrl = "http://wqp-sos.herokuapp.com/sos?service=SOS&request=DescribeSensor&version=1.0.0&outputformat=text/xml;subtype=%22sensorML/1.0.1%22&procedure="
  private val wqpDescSenName = "SOS - DescribeSensor"
  private val wqpDescSenDesc = "Sensor Observation Service - DescribeSensor"
  private val wqpGetObs = "GetObservation"
  private val wqpGetObsUrl = "http://wqp-sos.herokuapp.com/sos?service=SOS&request=GetObservation&version=1.0.0&responseformat=text/xml;subtype=%22om/1.0.0%22&eventtime=latest&offering="
  private val wqpGetObsName = "SOS - GetObservation"
  private val wqpGetObsDesc = "Sensor Observation Service - GetObservation"
  
  private val wqpIndividualName = "NWQMC - WQP"
  private val wqpOrgName = "National Water Quality Monitoring Council - Water Quality Portal"
  private val wqpEmail = "storet@epa.gov"
  private val wqpWebId = "wqp"
  private val wqpWebAddress = "http://waterqualitydata.us"
  
  private val storetIndividualName = "Jonathan Burian"
  private val storetOrgName = "STORET Data Warehouse"
  private val storetPhone = "(312) 886-2916"
  private val storetCity = "Chigaco"
  private val storetState = "IL"
  private val storetEmail = "burian.jonathan@epa.gov"
  private val storetWebId = "storet"
  private val storetWebAddress = "http://www.epa.gov/storet/"

  // url for querying storet stations, need to add 'organization' and 'siteid' for a successful query
  private val surl = "http://www.waterqualitydata.us/Station/search?countrycode=US&command.avoid=NWIS&mimeType=csv"
  private val rurl = "http://www.waterqualitydata.us/Result/search?countrycode=US&command.avoid=NWIS&mimeType=csv"
}

class StoretIsoWriter(private val stationQuery:StationQuery, 
  private val isoTemplateLocation: String,
  private val isoWriteDirectory: String,
  private val overwrite: Boolean) 
  extends ISOWriterImpl(stationQuery, isoTemplateLocation, 
      isoWriteDirectory, overwrite) {

import StoretIsoWriter._
  
  private val LOGGER = Logger.getLogger(getClass())
  private val httpSender = new HttpSender()
  private val dateParser = new SimpleDateFormat("yyyy-MM-ddHH:mm:ssz")

  var stationResponse: List[String] = null
  var resultResponse: List[String] = null

  override def initialSetup(station: LocalStation) : Boolean = {
    // request the station info
    try {
      // get org id and site id
      val site = java.net.URLEncoder.encode(station.databaseStation.foreign_tag, "UTF-8")
      val org = java.net.URLEncoder.encode(site.split("-").head, "UTF-8")
      val sResponse = HttpSender.sendGetMessage(surl + "&organization=" + org + "&siteid=" + site)
      if (sResponse != null) {
        val splitResponse = sResponse.toString split '\n'
        val removeFirstRow = splitResponse.toList.filter(s => !s.contains("OrganizationIdentifier"))
        stationResponse = filterCSV(removeFirstRow)
      } else {
        LOGGER error "Could not get station information for " + site
        return false
      }

      val rResponse = HttpSender.sendGetMessage(rurl + "&organization=" + org + "&siteid=" + site)
      if (rResponse != null) {
        val spltResponse = rResponse.toString split '\n'
        val removeFirstRow = spltResponse.toList.filter(s => !s.contains("OrganizationIdentifier"))
        resultResponse = filterCSV(removeFirstRow)
        LOGGER info "result line count: " + resultResponse.map(s => 1).foldLeft(0)(_+_)
      } else {
        LOGGER error "Could not get station results for " + site
        return false
      }
    } catch {
      case ex: Exception => {
          LOGGER error ex.toString
          return false
      }
    }

    true
  }

  override def getSensorTagsAndNames(station: LocalStation) : List[(String,String)] = {
    // go through the results and get the names of all lines w/o 'Non-detect'
    val splitLines = resultResponse filter { !_.contains("Non-detect") } map { _.split(",") }
    // 'charactername' is at index 31
    val retval = (splitLines map { _.zipWithIndex } flatten) map { s=> s._2 match { 
      case 31 => ( s._1, s._1 ); case _ => ( "", "" ) } } filter { _._1.length > 0 } groupBy { _._1 } map { _._2.head } toList;
    retval.map(s => (s._1.replaceAll("\0",","),s._2.replaceAll("\0",",")))
  }

  /*
  val srvAbstract: String,
  val serviceType: String,
  val id: String,
  val citation: ServiceIdentificationCitation,
  val extent: ServiceIdentificationExtent,
  val ops: List[ServiceIdentificationOperations]
  */
  override def getServiceInformation(station: LocalStation): List[ServiceIdentification] = {
    val srvabst = getStationAbstract
    val srvtype = getStationType
    val id = "SOS"
    val citation = new ServiceIdentificationCitation("WQP SOS","WQP SOS","distributor")
    val extent = getExtent(station)
    var getObsUrl: String = ""
    try {
      getObsUrl = wqpGetObsUrl + java.net.URLEncoder.encode(
          station.databaseStation.foreign_tag, "UTF-8") + 
          "&observedProperty=" + java.net.URLEncoder.encode(
              getSensorTagsAndNames(station).map(_._1).head, "UTF-8")
    }
    catch {
      case ex: Exception => {
          LOGGER error "Could not get an observed property for set, using 'Air Temperature' as default string (reason below)..."
          LOGGER error ex.toString
          ex.printStackTrace()
          getObsUrl = wqpGetObsUrl + java.net.URLEncoder.encode(
              station.databaseStation.foreign_tag, "UTF-8") + "&observedProperty=Air+Temperature"
      }
    }
    val descSenUrl = wqpDescSenUrl + java.net.URLEncoder.encode(
        station.databaseStation.foreign_tag, "UTF-8")
    val ops = List(
      new ServiceIdentificationOperations(wqpGetCaps, wqpGetCapsUrl, wqpGetCapsName, wqpGetCapsDesc),
      new ServiceIdentificationOperations(wqpDescSen, descSenUrl, wqpDescSenName, wqpDescSenDesc),
      new ServiceIdentificationOperations(wqpGetObs, getObsUrl, wqpGetObsName, wqpGetObsDesc)
    )
    List(new ServiceIdentification(srvabst, srvtype, id, citation, extent, ops))
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
    val webId: String,
    val webAddress: String,
    val role: String = "pointOfContact"
    */
  override def getContacts(station: LocalStation): List[Contact] = {
    List(
      new Contact(wqpIndividualName,wqpOrgName,null,null,null,null,null,
          wqpEmail,wqpWebId,wqpWebAddress,"distributor"),
      new Contact(storetIndividualName,storetOrgName,storetPhone,null,
          storetCity,storetState,null,storetEmail,storetWebId,storetWebAddress,"originator")
    )
  }
  
  override def getFileIdentifier(station: LocalStation): String = {
    station.databaseStation.foreign_tag
  }
  
  /*
    val idAbstract: String,
    val citation: DataIdentificationCitation,
    val keywords: List[String],
    val aggregate: DataIdentificationAggregate,
    val extent: ServiceIdentificationExtent
   */
  override def getDataIdentification(station: LocalStation) : DataIdentification = {
    val srvabst = getStationAbstract
    val citation = new DataIdentificationCitation(getStationName)
    val keywords = getSensorTagsAndNames(station).map(_._2)
    val agg = null
    val extent = getExtent(station)
    new DataIdentification(srvabst,citation,keywords,agg,extent)
  }

  override def getExtent(station: LocalStation): ServiceIdentificationExtent = {
    val temporals = getStationTemporalExtents
    val begin = if (temporals._1 != null) {
      if (temporals._1.getTimeInMillis != Long.MaxValue)
        formatDateTime(temporals._1)
      else
        "unknown"
    } else {
      ""
    }
    val end = if (temporals._2 != null) {
      if (temporals._2.getTimeInMillis != Long.MinValue)
        formatDateTime(temporals._2)
      else
        "unknown"
    } else {
      ""
    }
    new ServiceIdentificationExtent(station.getLocation.getY.toString, 
        station.getLocation.getX.toString,begin,end)
  }

  private def getStationName() : String = {
    // get the 3rd index of the station response when split on ','
    val splitLines = stationResponse map { _.split(",") } 
    // okay so below allows us to search by index looking for the earliest instance of index 3
    val retval = (splitLines map { _.zipWithIndex } flatten).foldRight(("", 0))((a,b) => 
      { b._2 match { case 3 => (b._1, b._2); case _ => (a._1, a._2) } } )._1.replaceAll("\"","")
    retval.replaceAll("\0", ",")
  }

  private def getStationType() : String = {
    val splitLines = stationResponse map { _.split(",") }
    (splitLines map { _.zipWithIndex } flatten).foldRight((""))((i,t) => {
        i._2 match {
          case 4 => i._1
          case _ => t
        }
      }).replaceAll("\0",",")
  }

  private def getStationID(station: LocalStation) : String = {
    // get the 2nd index of the station response when split on ','
    val splitLines = stationResponse map { _.split(",") }
    // okay so below allows us to search by index looking for the earliest instance of index 2
    val retval = (splitLines map { _.zipWithIndex } flatten).foldRight(("", 0))((a,b) => 
      { b._2 match { case 2 => (b._1, b._2); case _ => (a._1, a._2) } } )._1.replaceAll("\"","")
    retval.replaceAll("\0", ",")
  }

  private def getStationAbstract() : String = {
    // get the 1st and 3rd indices, combine them for our abstract
    val splitLines = stationResponse map { _.split(",") }
    val orgName = (splitLines map { _.zipWithIndex } flatten).foldRight(("",0))((a,b) => 
      { b._2 match { case 1 => (b._1, b._2); case _ => (a._1, a._2) } } )._1.replaceAll("\"","")
    val locName = (splitLines map { _.zipWithIndex } flatten).foldRight(("",0))((a,b) => 
      { b._2 match { case 3 => (b._1, b._2); case _ => (a._1, a._2) } } )._1.replaceAll("\"","")
    val retval = orgName + " - " + locName
    retval.replaceAll("\0", ",")
  }

  private def getStationTemporalExtents() : (Calendar,Calendar) = {
    try {
      var startDate: Calendar = Calendar.getInstance
      var endDate: Calendar = Calendar.getInstance
      endDate.setTimeInMillis(Long.MinValue)
      startDate.setTimeInMillis(Long.MaxValue)
      val splitLines = resultResponse map { _.split(",") }
      for (line <- splitLines) {
        val other = (line zipWithIndex) map (i => (i._1,i._2,""))
        // below creates a tuple seq of (String, int, String), folds them adding the values of index 6,7,8 together and returns this
        // the first string is the value of the index
        // the int is the index
        // the third value is a running string that is added to from the value of the index
        val timestring = ((line zipWithIndex) map (i => (i._1,i._2,""))).foldRight("",0,"")((a,b) => 
          { a._2 match { case 6|7 => ("",0,a._1 + b._3); case 8 => if (a._1.length < 1) ("",0,"GMT") else ("",0,a._1); case _ => ("",0,b._3) } } )._3
        // parse time string, compare to our start & end times
        val date = Calendar.getInstance
        date.setTimeInMillis(dateParser.parse(timestring).getTime)
        if (date.after(endDate))
          endDate = date
        if (date.before(startDate))
          startDate = date
      }
      (startDate,endDate)
    } catch {
      case ex: Exception => {
          LOGGER error ex.toString
          (null, null)
      }
    }
  }

  private def filterCSV(csv: List[String]) : List[String] = {
    var inQuote: Boolean = false
    csv map { l => {
      val newString = for (ch <- l) yield ch match {
        case '"' if (!inQuote) => { inQuote = true; '\1' }
        case ',' if (inQuote) => '\0'
        case '"' if (inQuote) => { inQuote = false; '\1' }
        case default => default
      }
      newString filter ( _ != '\1' )
    } }
  }
}
