package com.axiomalaska.sos.source

import com.axiomalaska.sos.source.stationupdater.AggregateStationUpdater
import org.apache.log4j.Logger

/**
 * This class manages updating the database with all the stations from all the 
 * sources for a selected region. 
 */
class MetadataDatabaseManager(    
    private val databaseUrl:String) {
  val queryBuilder = new StationQueryBuilder(databaseUrl)

  def init() {
    queryBuilder.withStationQuery(stationQuery => {
      stationQuery.init
    })
  }
  
  def update(boundingBox:BoundingBox, sources: String) {
    queryBuilder.withStationQuery(stationQuery => {
      val stationUpdater = new AggregateStationUpdater(stationQuery, 
          boundingBox, sources)
      stationUpdater.update()
    })
  }
}