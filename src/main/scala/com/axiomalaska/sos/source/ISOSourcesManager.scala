/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.axiomalaska.sos.source

import org.apache.log4j.Logger
import com.axiomalaska.sos.data.SosNetwork
import com.axiomalaska.sos.source.isowriter._
import com.axiomalaska.sos.data.PublisherInfo

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
