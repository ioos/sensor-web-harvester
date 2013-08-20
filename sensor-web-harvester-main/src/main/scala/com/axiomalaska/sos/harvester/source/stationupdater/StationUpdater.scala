package com.axiomalaska.sos.harvester.source.stationupdater

/**
 * This trait represents a object that updates a stations data with the call of
 * update()
 */
trait StationUpdater {
  def update()
  val name: String
}