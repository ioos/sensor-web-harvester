package com.axiomalaska.sos.harvester.iso

import com.axiomalaska.sos.harvester.data.LocalStation
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.DateTime
import com.axiomalaska.sos.harvester.source.observationretriever.SosObservationRetriever.getSensorForeignId
import com.axiomalaska.sos.harvester.StationQuery;

import scala.collection.JavaConverters._

abstract class SosIsoWriter(private val stationQuery:StationQuery,
    private val templateFile: String,
    private val isoDirectory: String,
    private val overwrite: Boolean)
    extends ISOWriterImpl(stationQuery, templateFile, isoDirectory, overwrite) {

  // override these in your specific derived ISOWriter implementations
  protected val sosName : String
  protected val orgName : String
  protected val role = "originator"
  protected val contactPhone : String // eg "(999) 999-9999"
  protected val contactWebId : String
  protected val baseUrl : String     // not including any query params

  private val dateFormatter = ISODateTimeFormat.dateTime()

  override def initialSetup(station: LocalStation): Boolean = {
    true
  }

  override def getServiceInformation(station: LocalStation): List[ServiceIdentification] = {
    val dbStation = station.databaseStation

    val srvabst = dbStation.description match {
      case "" => dbStation.name
      case _ => dbStation.description
    }
    val srvtype = dbStation.platformType
    val id = "SOS"
    val citation = new ServiceIdentificationCitation(sosName, orgName, role)

    val extent = getExtent(station)

    // @TODO: need to use URL builder from sos-injector, but is private currently
    val getCapsUrl = baseUrl + "?service=SOS&request=GetCapabilities"
    val descSensorUrl = baseUrl + "?service=SOS&request=DescribeSensor&version=1.0.0&outputformat=text/xml;subtype=%22sensorML/1.0.1%22&procedure=urn:ioos:station:" + java.net.URLEncoder.encode(
        station.databaseStation.tag, "UTF-8")

    val obsProp = station.getSensors().asScala.flatMap(_.getPhenomena.asScala.map(getSensorForeignId(_))).head.split("/").last
    val getObsUrl = baseUrl + "?service=SOS&request=GetObservation&version=1.0.0&responseformat=text/xml;subtype=%22om/1.0.0%22&eventtime=latest&offering=urn:ioos:station:" + java.net.URLEncoder.encode(
          station.databaseStation.tag, "UTF-8") + "&observedProperty=" + java.net.URLEncoder.encode(obsProp, "UTF-8")

    val ops = List(
      new ServiceIdentificationOperations("SOS Get Capabilities", getCapsUrl, "SOS", "Sensor Observation Service - GetCapabilities"),
      new ServiceIdentificationOperations("SOS Describe Sensor", descSensorUrl, "SOS", "Sensor Observation Service - DescribeSensor"),
      new ServiceIdentificationOperations("SOS Get Observation", getObsUrl, "SOS", "Sensor Observation Service - GetObservation")
    )
    List(new ServiceIdentification(srvabst, srvtype, id, citation, extent, ops))
  }

  override def getExtent(station: LocalStation): ServiceIdentificationExtent = {
    val beginTime = station.databaseStation.timeBegin match {
      case null => "unknown"
      case _ => dateFormatter.withZoneUTC.print(station.databaseStation.timeBegin.getTime)
    }
    val endTime = station.databaseStation.timeEnd match {
      case null if beginTime == "unknown" || !station.databaseStation.active => "unknown"
      case null  => ""    // blank string means "now"
      case _ => dateFormatter.withZoneUTC.print(station.databaseStation.timeEnd.getTime)
    }

    new ServiceIdentificationExtent(station.getLocation.getY.toString,
      station.getLocation.getX.toString, beginTime, endTime)
  }

  override def getContacts(station: LocalStation) : List[Contact] = {
    val source = station.getSource
    List(new Contact(null, orgName, contactPhone, source.getAddress, source.getCity,
      source.getState, source.getZipcode, source.getEmail, contactWebId, source.getWebAddress, role))
  }
}
