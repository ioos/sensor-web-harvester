package com.axiomalaska.sos.source

import org.apache.log4j.Logger
import scala.util.Random
import com.axiomalaska.phenomena.Phenomena
import com.axiomalaska.sos.data.PublisherInfo
import com.axiomalaska.sos.data.SosNetwork
import com.axiomalaska.sos.source.data.LocalPhenomenon
import java.util.Calendar
import collection.JavaConversions._
import org.n52.sos.ioos.asset.AssetResolver
import org.n52.sos.ioos.asset.NetworkAsset

/**
 * This class manages updating the SOS with all the stations from the metadata database
 * 
 * @param databaseUrl - a database URL of the form <code>jdbc:<em>subprotocol</em>:<em>subname</em></code>
 * 	     example jdbc:h2:sensor-web-harvester
 * @param sosUrl - The URL of the SOS example http://localhost:8080/sos/sos
 */
class SosSourcesManager(
    private val databaseUrl:String, 
	private val sosUrl:String, 
	private val publisherInfo:PublisherInfo, 
    private val sources: String) {
  
  private val LOGGER = Logger.getLogger(getClass())
  private val random = new Random(Calendar.getInstance.getTime.getTime)
  
  def updateSos() {
    val factory = new SosInjectorFactory()
    val queryBuilder = new StationQueryBuilder(databaseUrl)

    queryBuilder.withStationQuery(stationQuery => {
      val sosInjectors = factory.buildAllSourceSosInjectors(
        sosUrl, stationQuery, publisherInfo, sources.toLowerCase)
      
      for (sosInjector <- random.shuffle(sosInjectors)) {
        sosInjector.update()
      }
    })
  }
}