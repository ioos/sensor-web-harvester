package com.axiomalaska.sos.source.observationretriever

import com.axiomalaska.sos.source.StationQuery
import com.axiomalaska.sos.ObservationRetriever
import com.axiomalaska.sos.data.SosSensor
import com.axiomalaska.sos.data.SosStation
import java.util.Calendar
import com.axiomalaska.sos.data.ObservationCollection
import com.axiomalaska.sos.source.data.LocalStation
import net.opengis.ows.x11.ExceptionReportDocument
import net.opengis.om.x10.CompositeObservationDocument
import scala.collection.mutable
import gov.noaa.ioos.x061.CompositePropertyType
import gov.noaa.ioos.x061.ValueArrayType
import gov.noaa.ioos.x061.CompositeValueType
import net.opengis.gml.x32.ValueArrayPropertyType
import gov.noaa.ioos.x061.NamedQuantityType
import net.opengis.gml.x32.TimeInstantType
import com.axiomalaska.sos.source.data.ObservationValues
import com.axiomalaska.sos.source.data.LocalSensor
import com.axiomalaska.sos.source.data.LocalPhenomenon
import com.axiomalaska.sos.source.data.ObservedProperty
import scala.collection.JavaConversions._
import com.axiomalaska.sos.source.data.SensorPhenomenonIds
import org.apache.log4j.Logger
import com.axiomalaska.phenomena.Phenomenon

class NdbcSosObservationRetriever(private val stationQuery:StationQuery, 
    private val logger: Logger = Logger.getRootLogger()) 
	extends SosObservationRetriever(stationQuery, logger) {
  
  protected val serviceUrl = "http://sdf.ndbc.noaa.gov/sos/server.php"
  
}