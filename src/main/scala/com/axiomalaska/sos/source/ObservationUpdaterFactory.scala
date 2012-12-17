package com.axiomalaska.sos.source

import org.apache.log4j.Logger
import com.axiomalaska.sos.ObservationUpdater
import com.axiomalaska.sos.source.observationretriever.RawsObservationRetriever
import com.axiomalaska.sos.source.observationretriever.NoaaNosCoOpsObservationRetriever
import com.axiomalaska.sos.source.observationretriever.GlosObservationRetriever
import com.axiomalaska.sos.source.observationretriever.HadsObservationRetriever
import com.axiomalaska.sos.source.observationretriever.NdbcObservationRetriever
import com.axiomalaska.sos.source.observationretriever.SnoTelObservationRetriever
import com.axiomalaska.sos.source.observationretriever.StoretObservationRetriever
import com.axiomalaska.sos.source.observationretriever.UsgsWaterObservationRetriever
import com.axiomalaska.sos.source.observationretriever.NoaaWeatherObservationRetriever
import com.axiomalaska.sos.data.SosNetworkImp
import com.axiomalaska.sos.source.data.SourceId
import com.axiomalaska.sos.data.PublisherInfo
import com.axiomalaska.sos.source.observationretriever.NerrsObservationRetriever
import com.axiomalaska.sos.source.observationretriever.NdbcSosObservationRetriever

class ObservationUpdaterFactory {

  /**
   * Build all the Source ObservationUpdaters
   */
  def buildAllSourceObservationUpdaters(sosUrl: String,
    stationQuery: StationQuery, publisherInfo:PublisherInfo, sources: String,
    logger: Logger = Logger.getRootLogger()): List[ObservationUpdater] = {
    var retval: List[ObservationUpdater] = List()
    if (sources.contains("all") || sources.contains("glos")) {
      retval = buildGlosObservationUpdater(sosUrl, stationQuery, publisherInfo, logger) :: retval
    }
    if (sources.contains("all") || sources.contains("hads")) {
      retval = buildHadsObservationUpdater(sosUrl, stationQuery, publisherInfo, logger) :: retval
    }
    if (sources.contains("all") || sources.contains("ndbc")) {
      retval = buildNdbcFlatFileObservationUpdater(sosUrl, stationQuery, publisherInfo, logger) :: retval
    }
    if (sources.contains("all") || sources.contains("nerrs")) {
      retval = buildNerrsObservationUpdater(sosUrl, stationQuery, publisherInfo, logger) :: retval
    }
    if (sources.contains("all") || sources.contains("noaanoscoops")) {
      retval = buildNoaaNosCoOpsObservationUpdater(sosUrl, stationQuery, publisherInfo, logger) :: retval
    }
    if (sources.contains("all") || sources.contains("noaaweather")) {
      retval = buildNoaaWeatherObservationUpdater(sosUrl, stationQuery, publisherInfo, logger) :: retval
    }
    if (sources.contains("all") || sources.contains("raws")) {
      retval = buildRawsObservationUpdater(sosUrl, stationQuery, publisherInfo, logger) :: retval
    }
    if (sources.contains("all") || sources.contains("snotel")) {
      retval = buildSnotelObservationUpdater(sosUrl, stationQuery, publisherInfo, logger) :: retval
    }
    if (sources.contains("all") || sources.contains("storet")) {
      retval = buildStoretObservationUpdater(sosUrl, stationQuery, publisherInfo, logger) :: retval
    }
    if (sources.contains("all") || sources.contains("usgswater")) {
      retval = buildUsgsWaterObservationUpdater(sosUrl, stationQuery, publisherInfo, logger) :: retval
    }
    
    for (item <- retval) {
      logger.info("Getting observations from item: " + item.toString())
    }
    
    return retval
  }
  
  /**
   * Build a RAWS ObservationUpdater
   */
  def buildRawsObservationUpdater(sosUrl: String, 
      stationQuery:StationQuery, publisherInfo:PublisherInfo, 
      logger: Logger= Logger.getRootLogger()): ObservationUpdater = {

    val stationRetriever = new SourceStationRetriever(stationQuery, SourceId.RAWS, logger)
    val observationRetriever = new RawsObservationRetriever(stationQuery, logger)
    
    addSourceNetworkToStations(stationRetriever, "network-raws", "raws", "raws network stations")

    val retrieverAdapter = new ObservationRetrieverAdapter(observationRetriever, logger)
    val observationUpdater = new ObservationUpdater(sosUrl,
      logger, stationRetriever, publisherInfo, retrieverAdapter)
    
    return observationUpdater
  }
  
  /**
   * Build a NOAA NOS CO-OPS ObservationUpdater
   */
  def buildNoaaNosCoOpsObservationUpdater(sosUrl: String, 
      stationQuery:StationQuery, publisherInfo:PublisherInfo, 
      logger: Logger= Logger.getRootLogger()): ObservationUpdater = {

    val stationRetriever = new SourceStationRetriever(stationQuery, SourceId.NOAA_NOS_CO_OPS, logger)
    val observationRetriever = new NoaaNosCoOpsObservationRetriever(stationQuery, logger)
    
    addSourceNetworkToStations(stationRetriever, "network-noaa", "noaa", "noaa network stations")

    val retrieverAdapter = new ObservationRetrieverAdapter(observationRetriever, logger)
    val observationUpdater = new ObservationUpdater(sosUrl,
      logger, stationRetriever, publisherInfo, retrieverAdapter)
    
    return observationUpdater
  }
  
  /**
   * Build a NERRS ObservationUpdater
   */
  def buildNerrsObservationUpdater(sosUrl: String, 
      stationQuery:StationQuery, publisherInfo:PublisherInfo, 
      logger: Logger = Logger.getRootLogger()): ObservationUpdater = {

    val stationRetriever = new SourceStationRetriever(stationQuery, SourceId.NERRS, logger)
    val observationRetriever = new NerrsObservationRetriever(stationQuery, logger)
    
    addSourceNetworkToStations(stationRetriever, "network-nerrs", "nerrs", "nerrs network stations")

    val retrieverAdapter = new ObservationRetrieverAdapter(observationRetriever, logger)
    val observationUpdater = new ObservationUpdater(sosUrl,
      logger, stationRetriever, publisherInfo, retrieverAdapter)
    
    return observationUpdater
  }
  
  /**
   * Build a HADS ObservationUpdater
   */
  def buildHadsObservationUpdater(sosUrl: String, 
      stationQuery:StationQuery, publisherInfo:PublisherInfo, 
      logger: Logger= Logger.getRootLogger()): ObservationUpdater = {

    val stationRetriever = new SourceStationRetriever(stationQuery, SourceId.HADS, logger)
    val observationRetriever = new HadsObservationRetriever(stationQuery, logger)
    
    addSourceNetworkToStations(stationRetriever, "network-hads", "hads", "hads network stations")

    val retrieverAdapter = new ObservationRetrieverAdapter(observationRetriever, logger)
    val observationUpdater = new ObservationUpdater(sosUrl,
      logger, stationRetriever, publisherInfo, retrieverAdapter)
    
    return observationUpdater
  }
  
  /**
   * Build a NDBC ObservationUpdater
   */
  def buildNdbcFlatFileObservationUpdater(sosUrl: String, 
      stationQuery:StationQuery, publisherInfo:PublisherInfo, 
      logger: Logger = Logger.getRootLogger()): ObservationUpdater = {

    val stationRetriever = new SourceStationRetriever(stationQuery, SourceId.NDBC, logger)
    val observationRetriever = new NdbcObservationRetriever(stationQuery, logger)
    
    addSourceNetworkToStations(stationRetriever, "network-ndbc", "ndbc", "ndbc network stations")

    val retrieverAdapter = new ObservationRetrieverAdapter(observationRetriever, logger)
    
    val observationUpdater = new ObservationUpdater(sosUrl,
      logger, stationRetriever, publisherInfo, retrieverAdapter)
    
    return observationUpdater
  }
  
  /**
   * Build a NDBC SOS ObservationUpdater
   */
  def buildNdbcSosObservationUpdater(sosUrl: String, 
      stationQuery:StationQuery, publisherInfo:PublisherInfo, 
      logger: Logger = Logger.getRootLogger()): ObservationUpdater = {

    val stationRetriever = new SourceStationRetriever(stationQuery, SourceId.NDBC, logger)
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
      stationQuery:StationQuery, publisherInfo:PublisherInfo, 
      logger: Logger = Logger.getRootLogger()): ObservationUpdater = {

    val stationRetriever = new SourceStationRetriever(stationQuery, SourceId.SNOTEL, logger)
    val observationRetriever = new SnoTelObservationRetriever(stationQuery, logger)
    
    addSourceNetworkToStations(stationRetriever, "network-snotel", "snotel", "snotel network stations")

    val retrieverAdapter = new ObservationRetrieverAdapter(observationRetriever, logger)
    val observationUpdater = new ObservationUpdater(sosUrl,
      logger, stationRetriever, publisherInfo, retrieverAdapter)
    
    return observationUpdater
  }
  
  /**
   * Build a USGS Water ObservationUpdater
   */
  def buildUsgsWaterObservationUpdater(sosUrl: String, 
      stationQuery:StationQuery, publisherInfo:PublisherInfo, 
      logger: Logger = Logger.getRootLogger()): ObservationUpdater = {

    val stationRetriever = new SourceStationRetriever(stationQuery, SourceId.USGSWATER, logger)
    val observationRetriever = new UsgsWaterObservationRetriever(stationQuery, logger)
    
    addSourceNetworkToStations(stationRetriever, "network-usgs", "usgs", "usgs network stations")

    val retrieverAdapter = new ObservationRetrieverAdapter(observationRetriever, logger)
    val observationUpdater = new ObservationUpdater(sosUrl,
      logger, stationRetriever, publisherInfo, retrieverAdapter)
    
    return observationUpdater
  }
  
  /**
   * Build a NOAA Weather ObservationUpdater
   */
  def buildNoaaWeatherObservationUpdater(sosUrl: String, 
      stationQuery:StationQuery, publisherInfo:PublisherInfo, 
      logger: Logger = Logger.getRootLogger()): ObservationUpdater = {

    val stationRetriever = new SourceStationRetriever(stationQuery, SourceId.NOAA_WEATHER, logger)
    val observationRetriever = new NoaaWeatherObservationRetriever(stationQuery, logger)
    
    addSourceNetworkToStations(stationRetriever, "network-noaa", "noaa", "noaa network stations")

    val retrieverAdapter = new ObservationRetrieverAdapter(observationRetriever, logger)
    val observationUpdater = new ObservationUpdater(sosUrl,
      logger, stationRetriever, publisherInfo, retrieverAdapter)
    
    return observationUpdater
  }
  
  def buildStoretObservationUpdater(sosUrl: String, 
      stationQuery:StationQuery, publisherInfo:PublisherInfo, 
      logger: Logger = Logger.getRootLogger()): ObservationUpdater = {
    val stationRetriever = new SourceStationRetriever(stationQuery, SourceId.STORET, logger)
    
    // attempt 2: iterate over the stations and add a storet network
    addSourceNetworkToStations(stationRetriever, "network-storet", "storet", "storet network stations")
    
    val observationRetriever = new StoretObservationRetriever(stationQuery, logger)

    val retrieverAdapter = new ObservationRetrieverAdapter(observationRetriever, logger)
    
    val observationUpdater = new ObservationUpdater(sosUrl,
      logger, stationRetriever, publisherInfo, retrieverAdapter)
    return observationUpdater
  }
  
  def buildGlosObservationUpdater(sosUrl: String, 
      stationQuery:StationQuery, publisherInfo:PublisherInfo, 
      logger: Logger = Logger.getRootLogger()): ObservationUpdater = {
    val stationRetriever = new SourceStationRetriever(stationQuery, SourceId.GLOS, logger)
    val observationRetriever = new GlosObservationRetriever(stationQuery, logger)
    
    // iterate over the stations and add a glos network
    addSourceNetworkToStations(stationRetriever, "network-glos", "glos", "glos network stations")
    
    val retrieverAdapter = new ObservationRetrieverAdapter(observationRetriever, logger)
    val observationUpdater = new ObservationUpdater(sosUrl,
      logger, stationRetriever, publisherInfo, retrieverAdapter)
    return observationUpdater
  }
  
  private def addSourceNetworkToStations(stationRetriever: SourceStationRetriever, id: String, sourceId: String, description: String) = {
    val network = new SosNetworkImp()
    network.setDescription(description)
    network.setId(id)
    network.setSourceId(sourceId)
    for (station <- stationRetriever.getLocalStations) {
      station.addNetwork(network)
    }
  }
  
}