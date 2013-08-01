package com.axiomalaska.sos.source

object SourceUrls {

  val HADS_OBSERVATION_RETRIEVAL = "http://amazon.nws.noaa.gov/nexhads2/servlet/DecodedData"
  val HADS_COLLECTION_STATE_URLS = "http://amazon.nws.noaa.gov/hads/goog_earth/"
  val HADS_STATE_URL_TEMPLATE = "http://amazon.nws.noaa.gov/hads/charts/%s.html"
  val HADS_STATION_INFORMATION = "http://amazon.nws.noaa.gov/cgi-bin/hads/interactiveDisplays/displayMetaData.pl?table=dcp&nesdis_id="

  val NDBC_SOS = "http://sdf.ndbc.noaa.gov/sos/server.php"

  val NOAA_NOS_CO_OPS_SOS = "http://opendap.co-ops.nos.noaa.gov/ioos-dif-sos/SOS"

  val NOAA_WEATHER_OBSERVATION_RETRIEVAL = "http://www.nws.noaa.gov/data/obhistory/"
  val NOAA_WEATHER_COLLECTION_OF_STATIONS = "http://weather.noaa.gov/data/nsd_cccc.txt"

  val RAWS_OBSERVATION_RETRIEVAL = "http://www.raws.dri.edu/cgi-bin/wea_list2.pl"
  val RAWS_STATION_INFORMATION = "http://www.raws.dri.edu/cgi-bin/wea_info.pl?"
  val RAWS_STATE_URL_TEMPLATE = "http://www.raws.dri.edu/%slst.html"

  val SNOTEL_OBSERVATION_RETRIEVAL = "http://www.wcc.nrcs.usda.gov/nwcc/view"
  val SNOTEL_COLLECTION_SENSOR_INFO_FOR_STATION = "http://www.wcc.nrcs.usda.gov/nwcc/sensors"
  val SNOTEL_COLLECTION_OF_STATIONS = "http://www.wcc.nrcs.usda.gov/ftpref/data/water/wcs/earth/snotelwithoutlabels.kmz"

  val USGS_WATER_OBSERVATION_RETRIEVAL = "http://waterservices.usgs.gov/nwis/iv"
  val USGS_WATER_COLLECTION_OF_STATE_STATIONS = "http://waterservices.usgs.gov/nwis/iv?stateCd="
}