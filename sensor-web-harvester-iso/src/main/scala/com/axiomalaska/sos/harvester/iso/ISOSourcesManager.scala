/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.axiomalaska.sos.harvester.iso

import com.axiomalaska.sos.data.PublisherInfo
import com.axiomalaska.sos.harvester.StationQueryBuilder;

class ISOSourcesManager(private val isoTemplate: String,
                        private val isoDirectory: String,
                        private val sources: String,
                        private val databaseUrl: String,
                        private val overwrite: Boolean,
                        private val publisherInfo: PublisherInfo) {
  
  def writeISOs() {
    val queryBuilder = new StationQueryBuilder(databaseUrl)
    queryBuilder.withStationQuery(stationQuery => {
        val isoWriters = new AggregateIsoWriter(stationQuery, isoTemplate, 
            isoDirectory, sources, overwrite, publisherInfo)
        isoWriters.writeISOs()
      })
  }
}
