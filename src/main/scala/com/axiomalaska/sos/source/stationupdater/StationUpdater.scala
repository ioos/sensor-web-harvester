package com.axiomalaska.sos.source.stationupdater

import com.axiomalaska.sos.source.data.ObservedProperty
import com.axiomalaska.phenomena.Phenomenon
import org.apache.log4j.Logger
import com.axiomalaska.sos.source.data.DatabasePhenomenon
import com.axiomalaska.sos.source.StationQuery
import com.axiomalaska.sos.source.data.Source
import com.axiomalaska.ioos.sos.GeomHelper
import com.axiomalaska.sos.source.GeoTools
import com.axiomalaska.sos.source.BoundingBox

/**
 * This trait represents a object that updates a stations data with the call of
 * update()
 */
trait StationUpdater {
  def update()
  val name: String
}