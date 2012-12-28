package com.axiomalaska.sos.source.stationupdater

import org.apache.log4j.Logger
import com.axiomalaska.sos.tools.HttpSender
import com.axiomalaska.sos.source.BoundingBox
import com.axiomalaska.sos.source.StationQuery
import com.axiomalaska.sos.source.data.SourceId
import com.axiomalaska.sos.source.GeoTools
import com.axiomalaska.sos.source.observationretriever.SosRawDataRetriever
import com.axiomalaska.sos.source.data.Source
import com.axiomalaska.sos.source.data.DatabaseStation
import com.axiomalaska.sos.source.data.DatabaseSensor
import com.axiomalaska.sos.source.data.DatabasePhenomenon
import com.axiomalaska.sos.source.data.ObservedProperty
import com.axiomalaska.sos.source.data.SensorPhenomenonIds
import com.axiomalaska.sos.data.Location
import net.opengis.sos.x10.ObservationOfferingType
import net.opengis.sos.x10.CapabilitiesDocument
import net.opengis.sos.x10.GetCapabilitiesDocument
import net.opengis.om.x10.CompositeObservationDocument
import net.opengis.gml.x32.ValueArrayPropertyType
import gov.noaa.ioos.x061.NamedQuantityType
import gov.noaa.ioos.x061.CompositePropertyType
import gov.noaa.ioos.x061.CompositeValueType
import gov.noaa.ioos.x061.ValueArrayType
import com.axiomalaska.sos.source.SourceUrls

class NdbcSosStationUpdater(private val stationQuery: StationQuery,
  private val boundingBox: BoundingBox, 
  private val logger: Logger = Logger.getRootLogger()) extends 
  SosStationUpdater(stationQuery, boundingBox, logger) {

  // ---------------------------------------------------------------------------
  // StationUpdater Members
  // ---------------------------------------------------------------------------
  
  val name = "NDBC"
    
  // ---------------------------------------------------------------------------
  // SosStationUpdater Members
  // ---------------------------------------------------------------------------
  
  protected val serviceUrl = SourceUrls.NDBC_SOS
  protected val source = stationQuery.getSource(SourceId.NDBC)

  protected override def getCapabilitiesDocument():Option[CapabilitiesDocument] ={
    val request = 
        <GetCapabilities 
            xmlns="http://www.opengis.net/ows/1.1" 
            xmlns:ows="http://www.opengis.net/ows/1.1" 
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
            xsi:schemaLocation="http://www.opengis.net/ows/1.1 fragmentGetCapabilitiesRequest.xsd" 
            service="SOS">
            <AcceptVersions>
                <Version>1.0.0</Version>
            </AcceptVersions>
               <Sections>
                    <Section>ServiceProvider</Section>
                    <Section>ServiceIdentification</Section>
                    <Section>Contents</Section>
                </Sections>
                <AcceptFormats>
                    <OutputFormat>text/xml</OutputFormat>
                </AcceptFormats>
        </GetCapabilities>
          
    val httpSender = new HttpSender()
    val results = httpSender.sendPostMessage(serviceUrl, request.toString);

    if(results != null){
    	Some(CapabilitiesDocument.Factory.parse(results))
    }
    else{
      None
    }
  }
}