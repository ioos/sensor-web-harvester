package com.axiomalaska.sos.harvester.data

/**
 * THESE IDS MUST MATCH THE ID COLUMN IN source.csv!
 * Otherwise all kinds of madness will ensue!
 */
object SourceId {
  val RAWS = 1
  val NOAA_NOS_CO_OPS = 2
  val HADS = 3
  val NDBC = 4
  val SNOTEL = 5
  val USGSWATER = 6
  val NOAA_WEATHER = 7
  val NERRS = 8
  val STORET = 9
  val GLOS = 10
}