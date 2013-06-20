package com.axiomalaska.sos.source

import com.axiomalaska.sos.source.data.LocalStation
import scala.collection.JavaConversions._
import org.apache.log4j.Logger
import com.axiomalaska.sos.StationRetriever
import com.axiomalaska.sos.data.SosNetwork
import com.axiomalaska.sos.data.SosStation
import com.axiomalaska.sos.source.data.LocalSource
import com.axiomalaska.sos.data.PublisherInfo

/**
 * A StationRetriever for all of the sources in the package. All one needs to
 * do is pass in the source id. From this source id all SosStations can be created.
 */
class SourceStationRetriever(
  private val stationQuery: StationQuery,
  val sourceId: Int) extends StationRetriever {

  private lazy val stationList: List[LocalStation] = populateStationList

  override def getStations(): java.util.List[SosStation] = stationList

  def getLocalStations = stationList

  private def populateStationList(): List[LocalStation] = {
    val source = stationQuery.getSource(sourceId)

    val sosSource = new LocalSource(source)

    val databaseStations = stationQuery.getActiveStations(source)

    val allStations = stationQuery.getAllStations(source)

    databaseStations.map(station =>
      new LocalStation(sosSource, station, stationQuery))
  }
}