package com.axiomalaska.sos.source

import org.apache.log4j.Logger
import com.axiomalaska.sos.ObservationUpdater
import com.axiomalaska.sos.source.observationretriever.RawsObservationRetriever
import com.axiomalaska.sos.source.observationretriever.NoaaNosCoOpsObservationRetriever
import com.axiomalaska.sos.source.observationretriever.HadsObservationRetriever
import com.axiomalaska.sos.source.observationretriever.NdbcObservationRetriever
import com.axiomalaska.sos.source.observationretriever.SnoTelObservationRetriever
import com.axiomalaska.sos.source.observationretriever.UsgsWaterObservationRetriever
import com.axiomalaska.sos.source.observationretriever.NoaaWeatherObservationRetriever
import com.axiomalaska.sos.source.data.SourceId
import com.axiomalaska.sos.data.PublisherInfo
import com.axiomalaska.sos.source.observationretriever.NerrsObservationRetriever
import com.axiomalaska.sos.source.observationretriever.NdbcSosObservationRetriever
import com.axiomalaska.sos.data.SosNetwork

class ObservationUpdaterFactory {

  /**
   * Build all the Source ObservationUpdaters
   */
  def buildAllSourceObservationUpdaters(sosUrl: String,
    stationQuery: StationQuery, publisherInfo:PublisherInfo, rootNetwork:SosNetwork,
    logger: Logger = Logger.getRootLogger()): List[ObservationUpdater] =
    List(
      buildRawsObservationUpdater(sosUrl, stationQuery, publisherInfo, rootNetwork, logger),
      buildNoaaNosCoOpsObservationUpdater(sosUrl, stationQuery, publisherInfo, rootNetwork, logger),
      buildHadsObservationUpdater(sosUrl, stationQuery, publisherInfo, rootNetwork, logger),
      buildNdbcSosObservationUpdater(sosUrl, stationQuery, publisherInfo, rootNetwork, logger),
//      buildNdbcFlatFileObservationUpdater(sosUrl, stationQuery, publisherInfo, rootNetwork, logger),
      buildSnotelObservationUpdater(sosUrl, stationQuery, publisherInfo, rootNetwork, logger),
      buildUsgsWaterObservationUpdater(sosUrl, stationQuery, publisherInfo, rootNetwork, logger),
      buildNerrsObservationUpdater(sosUrl, stationQuery, publisherInfo, rootNetwork, logger),
      buildNoaaWeatherObservationUpdater(sosUrl, stationQuery, publisherInfo, rootNetwork, logger))
  
  /**
   * Build a RAWS ObservationUpdater
   */
  def buildRawsObservationUpdater(sosUrl: String, 
      stationQuery:StationQuery, publisherInfo:PublisherInfo, rootNetwork:SosNetwork,
      logger: Logger= Logger.getRootLogger()): ObservationUpdater = {

    val stationRetriever = new SourceStationRetriever(stationQuery, SourceId.RAWS, rootNetwork, logger)
    val observationRetriever = new RawsObservationRetriever(stationQuery, logger)

    val retrieverAdapter = new ObservationRetrieverAdapter(observationRetriever, logger)
    val observationUpdater = new ObservationUpdater(sosUrl,
      logger, stationRetriever, publisherInfo, retrieverAdapter)
    
    return observationUpdater
  }
  
  /**
   * Build a NOAA NOS CO-OPS ObservationUpdater
   */
  def buildNoaaNosCoOpsObservationUpdater(sosUrl: String, 
      stationQuery:StationQuery, publisherInfo:PublisherInfo, rootNetwork:SosNetwork,
      logger: Logger= Logger.getRootLogger()): ObservationUpdater = {

    val stationRetriever = new SourceStationRetriever(stationQuery, SourceId.NOAA_NOS_CO_OPS, 
        rootNetwork, logger)
    val observationRetriever = new NoaaNosCoOpsObservationRetriever(stationQuery, logger)

    val retrieverAdapter = new ObservationRetrieverAdapter(observationRetriever, logger)
    val observationUpdater = new ObservationUpdater(sosUrl,
      logger, stationRetriever, publisherInfo, retrieverAdapter)
    
    return observationUpdater
  }
  
  /**
   * Build a NERRS ObservationUpdater
   */
  def buildNerrsObservationUpdater(sosUrl: String, 
      stationQuery:StationQuery, publisherInfo:PublisherInfo, rootNetwork:SosNetwork,
      logger: Logger = Logger.getRootLogger()): ObservationUpdater = {

    val stationRetriever = new SourceStationRetriever(stationQuery, SourceId.NERRS, 
        rootNetwork, logger)
    val observationRetriever = new NerrsObservationRetriever(stationQuery, logger)

    val retrieverAdapter = new ObservationRetrieverAdapter(observationRetriever, logger)
    val observationUpdater = new ObservationUpdater(sosUrl,
      logger, stationRetriever, publisherInfo, retrieverAdapter)
    
    return observationUpdater
  }
  
  /**
   * Build a HADS ObservationUpdater
   */
  def buildHadsObservationUpdater(sosUrl: String, 
      stationQuery:StationQuery, publisherInfo:PublisherInfo, rootNetwork:SosNetwork,
      logger: Logger= Logger.getRootLogger()): ObservationUpdater = {

    val stationRetriever = new SourceStationRetriever(stationQuery, SourceId.HADS, 
        rootNetwork, logger)
    val observationRetriever = new HadsObservationRetriever(stationQuery, logger)

    val retrieverAdapter = new ObservationRetrieverAdapter(observationRetriever, logger)
    val observationUpdater = new ObservationUpdater(sosUrl,
      logger, stationRetriever, publisherInfo, retrieverAdapter)
    
    return observationUpdater
  }
  
  /**
   * Build a NDBC ObservationUpdater
   */
  def buildNdbcFlatFileObservationUpdater(sosUrl: String, 
      stationQuery:StationQuery, publisherInfo:PublisherInfo, rootNetwork:SosNetwork,
      logger: Logger = Logger.getRootLogger()): ObservationUpdater = {

    val stationRetriever = new SourceStationRetriever(stationQuery, SourceId.NDBC, 
        rootNetwork, logger)
    val observationRetriever = new NdbcObservationRetriever(stationQuery, logger)

    val retrieverAdapter = new ObservationRetrieverAdapter(observationRetriever, logger)
    val observationUpdater = new ObservationUpdater(sosUrl,
      logger, stationRetriever, publisherInfo, retrieverAdapter)
    
    return observationUpdater
  }
  
  /**
   * Build a NDBC SOS ObservationUpdater
   */
  def buildNdbcSosObservationUpdater(sosUrl: String, 
      stationQuery:StationQuery, publisherInfo:PublisherInfo, rootNetwork:SosNetwork,
      logger: Logger = Logger.getRootLogger()): ObservationUpdater = {

    val stationRetriever = new SourceStationRetriever(stationQuery, SourceId.NDBC, 
        rootNetwork, logger)
    val observationRetriever = new NdbcSosObservationRetriever(stationQuery, logger)
                                   
    val retrieverAdapter = new ObservationRetrieverAdapter(observationRetriever, logger)
    val observationUpdater = new ObservationUpdater(sosUrl,
      logger, stationRetriever, publisherInfo, retrieverAdapter)
    
    return observationUpdater
  }
  
  /**
   * Build a SnoTel ObservationUpdater
   */
  def buildSnotelObservationUpdater(sosUrl: String, 
      stationQuery:StationQuery, publisherInfo:PublisherInfo, rootNetwork:SosNetwork,
      logger: Logger = Logger.getRootLogger()): ObservationUpdater = {

    val stationRetriever = new SourceStationRetriever(stationQuery, SourceId.SNOTEL, 
        rootNetwork, logger)
    val observationRetriever = new SnoTelObservationRetriever(stationQuery, logger)

    val retrieverAdapter = new ObservationRetrieverAdapter(observationRetriever, logger)
    val observationUpdater = new ObservationUpdater(sosUrl,
      logger, stationRetriever, publisherInfo, retrieverAdapter)
    
    return observationUpdater
  }
  
  /**
   * Build a USGS Water ObservationUpdater
   */
  def buildUsgsWaterObservationUpdater(sosUrl: String, 
      stationQuery:StationQuery, publisherInfo:PublisherInfo, rootNetwork:SosNetwork,
      logger: Logger = Logger.getRootLogger()): ObservationUpdater = {

    val stationRetriever = new SourceStationRetriever(stationQuery, SourceId.USGSWATER, 
        rootNetwork, logger)
    val observationRetriever = new UsgsWaterObservationRetriever(stationQuery, logger)

    val retrieverAdapter = new ObservationRetrieverAdapter(observationRetriever, logger)
    val observationUpdater = new ObservationUpdater(sosUrl,
      logger, stationRetriever, publisherInfo, retrieverAdapter)
    
    return observationUpdater
  }
  
  /**
   * Build a NOAA Weather ObservationUpdater
   */
  def buildNoaaWeatherObservationUpdater(sosUrl: String, 
      stationQuery:StationQuery, publisherInfo:PublisherInfo, rootNetwork:SosNetwork,
      logger: Logger = Logger.getRootLogger()): ObservationUpdater = {

    val stationRetriever = new SourceStationRetriever(stationQuery, SourceId.NOAA_WEATHER, 
        rootNetwork, logger)
    val observationRetriever = new NoaaWeatherObservationRetriever(stationQuery, logger)

    val retrieverAdapter = new ObservationRetrieverAdapter(observationRetriever, logger)
    val observationUpdater = new ObservationUpdater(sosUrl,
      logger, stationRetriever, publisherInfo, retrieverAdapter)
    
    return observationUpdater
  }
}