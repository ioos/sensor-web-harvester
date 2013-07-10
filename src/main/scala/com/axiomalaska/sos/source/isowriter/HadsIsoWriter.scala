package com.axiomalaska.sos.source.isowriter

import com.axiomalaska.sos.source.{SourceUrls, StationQuery}
import com.axiomalaska.sos.source.data.{DatabaseSensor, SourceId, LocalStation}
import org.apache.log4j.Logger

class HadsIsoWriter(private val stationQuery:StationQuery,
    private val templateFile: String,
    private val isoDirectory: String,
    private val overwrite: Boolean) extends ISOWriterImpl(stationQuery, templateFile, isoDirectory, overwrite) {

  private val LOGGER = Logger.getLogger(getClass())

  private val hadsSOSName = "HADS SOS"
  private val hadsOrgName = "HADS"
  private val hadsRole = "originator"

  override def initialSetup(station: LocalStation): Boolean = {
    true
  }

  override def getServiceInformation(station: LocalStation): List[ServiceIdentification] = {
    val dbStation = station.databaseStation

    val srvabst = dbStation.description
    val srvtype = dbStation.platformType
    val id = "SOS"
    val citation = new ServiceIdentificationCitation(hadsSOSName, hadsOrgName, hadsRole)

    val extent = getExtent(station)

    val stationInfoUrl = SourceUrls.HADS_STATION_INFORMATION + dbStation.foreign_tag

    val ops = List(
      //new ServiceIdentificationOperations("SOS Get Capabilities", wqpGetCapsUrl, wqpGetCapsName, wqpGetCapsDesc),
      //new ServiceIdentificationOperations("SOS Describe Sensor", stationInfoUrl, "SOS", hadsSOSName),
      //new ServiceIdentificationOperations("SOS Get Observation", SourceUrls.HADS_OBSERVATION_RETRIEVAL, "SOS", hadsSOSName)
    )
    List(new ServiceIdentification(srvabst, srvtype, id, citation, extent, ops))
  }

  private def getExtent(station: LocalStation): ServiceIdentificationExtent = {
    // no temporal extents for HADS
    new ServiceIdentificationExtent(station.getLocation.getY.toString,
      station.getLocation.getX.toString, "", "")
  }

  override def getSensorTagsAndNames(station: LocalStation): List[(String, String)] = {
    val sensors = stationQuery.getAllSensors(station.databaseStation)
    return sensors.map { d:DatabaseSensor => {
      // @TODO is this one to one?
      val phen = stationQuery.getPhenomena(d)
      if (phen.length == 0)
        ("", "")
      else
        (phen.head.tag, d.tag)
    }}
  }


  override def getContacts(station: LocalStation) : List[Contact] = {
    val source = station.getSource
    val phone = "(999) 999-9999"
    List(new Contact(null,hadsOrgName,phone,source.getAddress,source.getCity,
      source.getState,source.getZipcode,source.getEmail,"hads",source.getWebAddress,hadsRole))
  }

  override def getFileIdentifier(station: LocalStation) : String = {
    station.databaseStation.foreign_tag
  }

  override def getDataIdentification(station: LocalStation): DataIdentification = {
    val idabstract = station.databaseStation.description
    val citation = new DataIdentificationCitation(idabstract)
    val keywords = getSensorTagsAndNames(station).map(_._2)
    val agg = null
    val extent = getExtent(station)
    new DataIdentification(idabstract,citation,keywords,agg,extent)
  }

  override def getForeignTag(station: LocalStation) : String = {
    station.databaseStation.foreign_tag
  }

}
