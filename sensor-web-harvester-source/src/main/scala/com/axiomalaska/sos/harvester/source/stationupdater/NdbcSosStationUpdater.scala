package com.axiomalaska.sos.harvester.source.stationupdater

import org.apache.log4j.Logger
import com.axiomalaska.sos.tools.HttpSender
import com.axiomalaska.sos.harvester.BoundingBox;
import com.axiomalaska.sos.harvester.source.SourceUrls;
import com.axiomalaska.sos.harvester.StationQuery;
import com.axiomalaska.sos.harvester.data.SourceId;

import net.opengis.sos.x10.CapabilitiesDocument

class NdbcSosStationUpdater(private val stationQuery: StationQuery,
  private val boundingBox: BoundingBox) extends 
  SosStationUpdater(stationQuery, boundingBox) {

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
    val results = HttpSender.sendPostMessage(serviceUrl, request.toString);

    if(results != null){
    	Some(CapabilitiesDocument.Factory.parse(results))
    }
    else{
      None
    }
  }
}