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
import com.axiomalaska.sos.source.data.LocalPhenomenon
import com.axiomalaska.sos.source.data.ObservedProperty
import com.axiomalaska.phenomena.Phenomena
import com.axiomalaska.phenomena.Phenomenon
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

class NoaaNosCoOpsStationUpdater(private val stationQuery: StationQuery,
  private val boundingBox: BoundingBox, 
  private val logger: Logger = Logger.getRootLogger()) extends 
  SosStationUpdater(stationQuery, boundingBox, logger) {

  // ---------------------------------------------------------------------------
  // SosStationUpdater Members
  // ---------------------------------------------------------------------------
  
  protected val serviceUrl = SourceUrls.NOAA_NOS_CO_OPS_SOS
  protected val source = stationQuery.getSource(SourceId.NOAA_NOS_CO_OPS)

  // ---------------------------------------------------------------------------
  // StationUpdater Members
  // ---------------------------------------------------------------------------
  
  val name = "NOAA NOS CO-OPS"
}