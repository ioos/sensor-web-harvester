package com.axiomalaska.sos.harvester.app

import com.axiomalaska.sos.SosInjector
import com.axiomalaska.sos.data.PublisherInfo
import com.axiomalaska.sos.harvester.SourceStationRetriever
import com.axiomalaska.sos.harvester.StationQuery
import com.axiomalaska.sos.harvester.data.SourceId
import com.axiomalaska.sos.harvester.source.observationretriever._
import com.axiomalaska.sos.harvester.ObservationRetrieverAdapter

class SosInjectorFactory {

  /**
   * Build all the Source SosInjectors
   */
  def buildAllSourceSosInjectors(sosUrl: String,
    stationQuery: StationQuery, publisherInfo:PublisherInfo, 
    sources: String): List[SosInjector] = {
    var retval: List[SosInjector] = List()
    if (sources.contains("all") || sources.contains("glos")) {
      retval ::= buildGlosSosInjector(sosUrl, stationQuery, publisherInfo)
    }
    if (sources.contains("all") || sources.contains("hads")) {
      retval ::= buildHadsSosInjector(sosUrl, stationQuery, publisherInfo)
    }
    if (sources.contains("all") || sources.contains("ndbc")) {
      retval ::= buildNdbcFlatFileSosInjector(sosUrl, stationQuery, publisherInfo)
    }
    if (sources.contains("all") || sources.contains("nerrs")) {
      retval ::= buildNerrsSosInjector(sosUrl, stationQuery, publisherInfo)
    }
    if (sources.contains("all") || sources.contains("noaanoscoops")) {
      retval ::= buildNoaaNosCoOpsSosInjector(sosUrl, stationQuery, publisherInfo)
    }
    if (sources.contains("all") || sources.contains("noaaweather")) {
      retval ::= buildNoaaWeatherSosInjector(sosUrl, stationQuery, publisherInfo)
    }
    if (sources.contains("all") || sources.contains("raws")) {
      retval ::= buildRawsSosInjector(sosUrl, stationQuery, publisherInfo)
    }
    if (sources.contains("all") || sources.contains("snotel")) {
      retval ::= buildSnotelSosInjector(sosUrl, stationQuery, publisherInfo)
    }
    if (sources.contains("all") || sources.contains("storet")) {
      retval ::= buildStoretSosInjector(sosUrl, stationQuery, publisherInfo)
    }
    if (sources.contains("all") || sources.contains("usgswater")) {
      retval ::= buildUsgsWaterSosInjector(sosUrl, stationQuery, publisherInfo)
    }
    
    return retval
  }
 
  /**
   * Build a RAWS SosInjector
   */
  def buildRawsSosInjector(sosUrl: String, 
      stationQuery:StationQuery, publisherInfo:PublisherInfo): SosInjector = {

    val stationRetriever = new SourceStationRetriever(stationQuery, 
        SourceId.RAWS)
    val observationRetriever = new RawsObservationRetriever(stationQuery)
    
    val retrieverAdapter = new ObservationRetrieverAdapter(observationRetriever)
    val sosInjector = new SosInjector("RAWS SOS Injector", sosUrl, 
        publisherInfo, stationRetriever, retrieverAdapter, null)
    
    return sosInjector
  }
  
  /**
   * Build a NOAA NOS CO-OPS SosInjector
   */
  def buildNoaaNosCoOpsSosInjector(sosUrl: String, 
      stationQuery:StationQuery, publisherInfo:PublisherInfo): SosInjector = {

    val stationRetriever = new SourceStationRetriever(stationQuery, 
        SourceId.NOAA_NOS_CO_OPS)
    val observationRetriever = new NoaaNosCoOpsObservationRetriever(stationQuery)
    
    val retrieverAdapter = new ObservationRetrieverAdapter(observationRetriever)
    val sosInjector = new SosInjector("Noaa Nos Co-Ops SOS Injector", sosUrl, 
        publisherInfo, stationRetriever, retrieverAdapter, null)
    
    return sosInjector
  }
  
  /**
   * Build a NERRS SosInjector
   */
  def buildNerrsSosInjector(sosUrl: String, 
      stationQuery:StationQuery, publisherInfo:PublisherInfo): SosInjector = {

    val stationRetriever = new SourceStationRetriever(stationQuery, SourceId.NERRS)
    val observationRetriever = new NerrsObservationRetriever(stationQuery)
    
    val retrieverAdapter = new ObservationRetrieverAdapter(observationRetriever)
    val sosInjector = new SosInjector("Nerrs SOS Injector", sosUrl, 
        publisherInfo, stationRetriever, retrieverAdapter, null)
    
    return sosInjector
  }
  
  /**
   * Build a HADS SosInjector
   */
  def buildHadsSosInjector(sosUrl: String, 
      stationQuery:StationQuery, publisherInfo:PublisherInfo): SosInjector = {

    val stationRetriever = new SourceStationRetriever(stationQuery, SourceId.HADS)
    val observationRetriever = new HadsObservationRetriever(stationQuery)

    val retrieverAdapter = new ObservationRetrieverAdapter(observationRetriever)
    val sosInjector = new SosInjector("Hads SOS Injector", sosUrl, 
        publisherInfo, stationRetriever, retrieverAdapter, null)
    
    return sosInjector
  }
  
  /**
   * Build a NDBC SosInjector
   */
  def buildNdbcFlatFileSosInjector(sosUrl: String, 
      stationQuery:StationQuery, publisherInfo:PublisherInfo): SosInjector = {

    val stationRetriever = new SourceStationRetriever(stationQuery, SourceId.NDBC)
    val observationRetriever = new NdbcObservationRetriever(stationQuery)
    
    val retrieverAdapter = new ObservationRetrieverAdapter(observationRetriever)
    
    val sosInjector = new SosInjector("Ndbc Flat File SOS Injector", sosUrl, 
        publisherInfo, stationRetriever, retrieverAdapter, null)
    
    return sosInjector
  }
  
  /**
   * Build a NDBC SOS SosInjector
   */
  def buildNdbcSosSosInjector(sosUrl: String, 
      stationQuery:StationQuery, publisherInfo:PublisherInfo): SosInjector = {

    val stationRetriever = new SourceStationRetriever(stationQuery, SourceId.NDBC)
    val observationRetriever = new NdbcSosObservationRetriever(stationQuery)
                                   
    val retrieverAdapter = new ObservationRetrieverAdapter(observationRetriever)
    val sosInjector = new SosInjector("Ndbc SOS SOS Injector", sosUrl, 
        publisherInfo, stationRetriever, retrieverAdapter, null)
    
    return sosInjector
  }
  
  /**
   * Build a SnoTel SosInjector
   */
  def buildSnotelSosInjector(sosUrl: String, 
      stationQuery:StationQuery, publisherInfo:PublisherInfo): SosInjector = {

    val stationRetriever = new SourceStationRetriever(stationQuery, SourceId.SNOTEL)
    val observationRetriever = new SnoTelObservationRetriever(stationQuery)

    val retrieverAdapter = new ObservationRetrieverAdapter(observationRetriever)
    val sosInjector = new SosInjector("Snotel SOS Injector", sosUrl, 
        publisherInfo, stationRetriever, retrieverAdapter, null)
    
    return sosInjector
  }
  
  /**
   * Build a USGS Water SosInjector
   */
  def buildUsgsWaterSosInjector(sosUrl: String, 
      stationQuery:StationQuery, publisherInfo:PublisherInfo): SosInjector = {

    val stationRetriever = new SourceStationRetriever(stationQuery, SourceId.USGSWATER)
    val observationRetriever = new UsgsWaterObservationRetriever(stationQuery)

    val retrieverAdapter = new ObservationRetrieverAdapter(observationRetriever)
    val sosInjector = new SosInjector("USGS Water SOS Injector", sosUrl, 
        publisherInfo, stationRetriever, retrieverAdapter, null)
    
    return sosInjector
  }
  
  /**
   * Build a NOAA Weather SosInjector
   */
  def buildNoaaWeatherSosInjector(sosUrl: String, 
      stationQuery:StationQuery, publisherInfo:PublisherInfo): SosInjector = {

    val stationRetriever = new SourceStationRetriever(stationQuery, 
        SourceId.NOAA_WEATHER)
    val observationRetriever = new NoaaWeatherObservationRetriever(stationQuery)
    
    val retrieverAdapter = new ObservationRetrieverAdapter(observationRetriever)
    val sosInjector = new SosInjector("Noaa Weather SOS Injector", sosUrl, 
        publisherInfo, stationRetriever, retrieverAdapter, null)
    
    return sosInjector
  }
  
  def buildStoretSosInjector(sosUrl: String, 
      stationQuery:StationQuery, publisherInfo:PublisherInfo): SosInjector = {
    val stationRetriever = new SourceStationRetriever(stationQuery, 
        SourceId.STORET)
    
    val observationRetriever = new StoretObservationRetriever(stationQuery)

    val retrieverAdapter = new ObservationRetrieverAdapter(observationRetriever)
    
    val sosInjector = new SosInjector("Storet SOS Injector", sosUrl, 
        publisherInfo, stationRetriever, retrieverAdapter, null)
    
    return sosInjector
  }
  
  def buildGlosSosInjector(sosUrl: String, 
      stationQuery:StationQuery, publisherInfo:PublisherInfo): SosInjector = {
    val stationRetriever = new SourceStationRetriever(stationQuery, 
        SourceId.GLOS)
    val observationRetriever = new GlosObservationRetriever(stationQuery)
    
    val retrieverAdapter = new ObservationRetrieverAdapter(observationRetriever)
    
    val sosInjector = new SosInjector("Glos SOS Injector", sosUrl, 
        publisherInfo, stationRetriever, retrieverAdapter, null)
    
    return sosInjector
  }
}