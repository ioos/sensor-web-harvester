/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.axiomalaska.sos.harvester.source.stationupdater

import com.axiomalaska.sos.harvester.BoundingBox;
import com.axiomalaska.sos.harvester.StationQuery;
import com.axiomalaska.sos.harvester.data.DatabasePhenomenon;
import com.axiomalaska.sos.harvester.data.DatabaseSensor;
import com.axiomalaska.sos.harvester.data.DatabaseStation;
import com.axiomalaska.sos.harvester.data.ObservedProperty;
import com.axiomalaska.sos.harvester.data.SourceId;
import com.axiomalaska.phenomena.Phenomena
import com.axiomalaska.phenomena.Phenomenon
import com.axiomalaska.sos.harvester.data.LocalPhenomenon
import com.axiomalaska.sos.tools.HttpSender
import com.typesafe.config.ConfigFactory
import org.apache.commons.net.ftp.FTPReply
import org.apache.log4j.Logger
import java.io.ByteArrayOutputStream
import java.io.ByteArrayInputStream
import java.io.File
import org.apache.commons.net.ftp.FTPClient
import org.apache.commons.net.ftp.FTPFile
import scala.collection.mutable
import com.axiomalaska.sos.harvester.data.PhenomenaFactory
import scala.util.control.Breaks._
import org.joda.time.format.ISODateTimeFormat
import java.sql.Timestamp

case class GLOSStation (stationName: String, stationId: String, stationDesc: String, platformType: String, lat: Double, lon: Double, time_begin: Timestamp, time_end: Timestamp)

class GlosStationUpdater (private val stationQuery: StationQuery,
  private val boundingBox: BoundingBox) extends StationUpdater {

  val name = "GLOS"
  
  private val LOGGER = Logger.getLogger(getClass())

  //ftp info
  private val config = ConfigFactory.parseFile(new File("glos.conf"))
  /*
  glos.conf file needed with ftp connection config in same dir as sensor-web-harvester jar, example:

  ftp {
    host = "glos.us"
    port = 21
    user = "someuser"
    pass = "somepassword"
  }
  */

  private val ftp_host = config.getString("ftp.host")
  private val ftp_port = config.getInt("ftp.port")
  private val ftp_user = config.getString("ftp.user")
  private val ftp_pass = config.getString("ftp.pass")

  private val glos_ftp: FTPClient = new FTPClient()
    
  private val temp_file = "platform.csv"
  private val glos_metadata_folder = "glosbuoy_metadata/"
    
  private val phenomenaFactory = new PhenomenaFactory()
  
  private val source = stationQuery.getSource(SourceId.GLOS)
  private val stationUpdater = new StationUpdateTool(stationQuery)
  private var stationList: List[String] = Nil
  
  private var phenomenaList = stationQuery.getPhenomena
  private val dateParser = ISODateTimeFormat.dateTime()

  def update() {
    LOGGER.info("Updating GLOS...")
    LOGGER.info("FTP host: " + ftp_host)
    LOGGER.info("FTP port: " + ftp_port)
    LOGGER.info("FTP user: " + ftp_user)

    val sourceStationSensors = getSourceStations()

    val databaseStations = stationQuery.getAllStations(source)

    stationUpdater.updateStations(sourceStationSensors, databaseStations)
    
    LOGGER.info("Finished updating GLOS")
    
  }
  
  private def getSourceStations() : List[(DatabaseStation, List[(DatabaseSensor, List[DatabasePhenomenon])])] = {
    // read ftp for data files for stations
    try {
      if (!glos_ftp.isConnected) {
        glos_ftp.connect(ftp_host, ftp_port)
        glos_ftp.login(ftp_user, ftp_pass)
        // check for succesful login
        if(!FTPReply.isPositiveCompletion(glos_ftp.getReplyCode)) {
          glos_ftp.disconnect
          LOGGER.error("FTP connection was refused.")
        } else {
          // set to passive mode
          glos_ftp.enterLocalPassiveMode
          // set timeouts to 1 min
          glos_ftp.setControlKeepAliveTimeout(60)
          glos_ftp.setDataTimeout(60000)
        }
      }
    } catch {
      case ex: Exception => {
          LOGGER.error(ex.toString)
          return Nil
      }
    }
    // read local ISOs for metadata
    var dir = new File(glos_metadata_folder)
    if (!dir.exists) {
      //try to load from classpath
      val glosMetadataDirResource = getClass.getClassLoader.getResource(glos_metadata_folder)
      if (glosMetadataDirResource != null) {
        dir = new File(glosMetadataDirResource.toURI)
      }
    }
    if (!dir.exists) {
      LOGGER.info("Directory " + dir.getAbsolutePath() + " doesn't exist")      
      return List[(DatabaseStation, List[(DatabaseSensor, List[DatabasePhenomenon])])]()
    }
    LOGGER.info(dir.listFiles.length + " files in directory")
    var path_list: Array[FTPFile] = glos_ftp.listFiles
    val finallist = for (file <- dir.listFiles; if file.getName.contains(".xml")) yield {
        val readin = scala.io.Source.fromFile(file)
        val xml = scala.xml.XML.loadString(readin.mkString)
        // read in the station data
        val station = readStationFromXML(xml)
        // get the obs data, need this for the depth values
        var dataXML: scala.xml.Elem = null
        val byteStream: ByteArrayOutputStream = new ByteArrayOutputStream()

        breakable {
          for (ftpfile <- path_list) {
            byteStream.reset()
            if (ftpfile.getName.contains(station.stationName)) {
              try {
                glos_ftp.retrieveFile(ftpfile.getName, byteStream)
                val inp: ByteArrayInputStream = new ByteArrayInputStream(byteStream.toByteArray)
                dataXML = scala.xml.XML.load(inp)
                break
              } catch {
                case ex: Exception => null // Don't do anything
              }
            }
          }
        }

        if (dataXML.ne(null)) {
          LOGGER.debug("reading xml file for station: " + station.stationName)
          val depths = readInDepths(dataXML)
          val stationDB = new DatabaseStation(station.stationName, source.tag + ":" + station.stationId,
              station.stationId, station.stationDesc, station.platformType, 
              source.id, station.lat, station.lon, station.time_begin, station.time_end)
          val sensors = readInSensors((xml \ "contentInfo"), stationDB, depths)
          if (sensors.nonEmpty)
            (stationDB, sensors)
          else
            (null, Nil)
        } else {
          (null, Nil)
        }
    }
    finallist.filter(p => p._2.nonEmpty).toList
  }

  private def readInDepths(data: scala.xml.Elem) : List[Double] = {
    var continue: Boolean = true
    var retval: List[Double] = Nil
    
    val wtmp = for (wt <- data \\ "wtmp1") yield { wt }
    if (wtmp.nonEmpty)
      retval = List(0d)
    else
      retval = List(-1d)
    
    var depthIndex: Integer = 1
    while (continue) {
      val xmlTag = if (depthIndex < 10) "dp00" + depthIndex else if (depthIndex < 100) "dp0" + depthIndex else "dp" + depthIndex
      val depth = for (elm <- data \\ xmlTag) yield {
        elm.text.trim
      }
      if (depth.isEmpty)
        continue = false
      else
        retval = depth.head.toDouble :: retval
      depthIndex += 1
    }
    
    return retval
  }
  
  private def readInSensors(stationxml : scala.xml.NodeSeq, 
      station : DatabaseStation, depths: List[Double]) : 
      List[(DatabaseSensor, List[(DatabasePhenomenon)])] = {
    val orderedDepths = depths.reverse
    val sensors = for {
      sensor <- (stationxml \\ "MD_Band")
      val sensorid = (sensor \\ "aName" \ "CharacterString").text.trim
      val sensordesc = (sensor \\ "descriptor" \ "CharacterString").text.trim
      val dbsensor = new DatabaseSensor(sensorid, sensordesc, station.id)
    } yield {
      var oprops: List[ObservedProperty] = Nil
      if (sensorid.equalsIgnoreCase("sea_water_temperature")) {
        for ((depth, index) <- orderedDepths.zipWithIndex) {LOGGER.info(index + " - " + depth)}
        val swtsen = for {
          (depth, index) <- orderedDepths.zipWithIndex
          if (depth >= 0)
        } yield {
          oprops = getObservedProperties(sensorid, sensordesc, depth, index)
          val dboprops = stationUpdater.updateObservedProperties(source, oprops)
          stationUpdater.getSourceSensors(station, dboprops)
        }
        swtsen.flatten
      } else {
        oprops = getObservedProperties(sensorid, sensordesc, 0d, 0)
        val dboprops = stationUpdater.updateObservedProperties(source, oprops)
        stationUpdater.getSourceSensors(station, dboprops)
      }
    }
    sensors.toList.flatten
  }



  private def findPhenomenon(tag: String) : Phenomenon = {
    // check the tag to list of known phenomena
    tag.toLowerCase match {
      case "air_pressure_at_sea_level" => return Phenomena.instance.AIR_PRESSURE_AT_SEA_LEVEL
      case "air_temperature" => return Phenomena.instance.AIR_TEMPERATURE
      case "dew_point_temperature" => return Phenomena.instance.DEW_POINT_TEMPERATURE
      case "relative_humidity" => return Phenomena.instance.RELATIVE_HUMIDITY
      case "sea_surface_wave_significant_height" => return Phenomena.instance.SEA_SURFACE_WAVE_SIGNIFICANT_HEIGHT
      case "sea_surface_wind_wave_period" => return Phenomena.instance.SEA_SURFACE_WIND_WAVE_PERIOD
      case "sea_water_temperature" => return Phenomena.instance.SEA_WATER_TEMPERATURE
      case "wave_height" => return Phenomena.instance.SEA_SURFACE_WAVE_SIGNIFICANT_HEIGHT
      case "wind_from_direction" => return Phenomena.instance.WIND_FROM_DIRECTION
      case "wind_speed" => return Phenomena.instance.WIND_SPEED
      case "wind_speed_of_gust" => return Phenomena.instance.WIND_SPEED_OF_GUST
      case "mass_concentration_of_chlorophyll_in_sea_water" => return Phenomena.instance.MASS_CONCENTRATION_OF_CHLOROPHYLL_IN_SEA_WATER
      case "mass_concentration_of_oxygen_in_sea_water" => return Phenomena.instance.MASS_CONCENTRATION_OF_OXYGEN_IN_SEA_WATER
      case "sea_water_ph_reported_on_total_scale" => return Phenomena.instance.SEA_WATER_PH_REPORTED_ON_TOTAL_SCALE
      case "sea_water_electrical_conductivity" => return Phenomena.instance.SEA_WATER_ELECTRICAL_CONDUCTIVITY
      case "northward_sea_water_velocity" => return Phenomena.instance.NORTHWARD_SEA_WATER_VELOCITY
      case "eastward_sea_water_velocity" => return Phenomena.instance.EASTWARD_SEA_WATER_VELOCITY
      case "sun_radiation" => return Phenomena.instance.SOLAR_RADIATION
      case _ => LOGGER.info("Unhandled case: " + tag)
    }
    
    return null
  }
  
  private def getForeignTagFromName(name: String, index: Integer) : String = {
    name.toLowerCase match {
      case "air_pressure_at_sea_level" => "baro1"
      case "air_temperature" => "atmp1"
      case "dew_point_temperature" => "dewpt1"
      case "relative_humidity" => "rh1"
      case "sea_surface_wave_significant_height" => "wvhgt"
      case "sea_surface_wind_wave_period" => "dompd"
      case "sea_water_temperature" => {
          if (index == 0)
            "wtmp1"
          else
            if (index < 10) "tp00" + index else if (index < 100) "tp0" + index else "tp" + index
      }
      case "wave_height" => "wvhgt"
      case "wind_from_direction" => "wdir1"
      case "wind_speed" => "wspd1"
      case "wind_speed_of_gust" => "gust1"
      case "sun_radiation" => "srad1"
      case _ => ""
    }
  }
  
  private def getObservedProperties(name: String, desc: String, 
      depths: List[Double]) : List[ObservedProperty] = {
    val properties = for {
      (dpth,index) <- depths.zipWithIndex
      phenom = findPhenomenon(name)
      foreignTag = getForeignTagFromName(name, index)
      if (phenom.ne(null) && !foreignTag.equals(""))
    } yield {
      stationUpdater.getObservedProperty(phenom, foreignTag, phenom.getUnit().toString(), dpth, source)
    }
    properties
  }
  
  private def getObservedProperties(name: String, desc: String, 
      depth: Double, index: Integer) : List[ObservedProperty] = {
     val phenom = findPhenomenon(name)
     val foreignTag = getForeignTagFromName(name, index)
     if (phenom != null && !foreignTag.equals(""))
       List(stationUpdater.getObservedProperty(phenom, 
           foreignTag, phenom.getUnit().toString(), depth, source))
     else
       Nil
  }

  private def readStationFromXML(xml : scala.xml.Node) : GLOSStation = {
    //val metadata = (xml \ "gmi:MI_Metadata")
    val name = (xml \ "fileIdentifier" \ "CharacterString").text.trim
    val id = name
    var desc = (xml \ "identificationInfo" \\ "abstract" \ "CharacterString").text.trim
    val lat = for (north <- xml \\ "northBoundLatitude" \ "Decimal") yield { north.text.trim }
    val lon = for (west <- xml \\ "westBoundLongitude" \ "Decimal") yield { west.text.trim }
    val platformType = "BUOY"
    if (desc.length > 254) 
      desc = desc.slice(0, 252) + "..."

    val tpelem = xml \ "identificationInfo" \ "MD_DataIdentification" \ "extent" \\ "TimePeriod"

    val beginval = (tpelem \ "beginPosition").text.trim
    val time_begin = if (beginval != "") new Timestamp(dateParser.parseDateTime(beginval).getMillis) else null
    val endval = (tpelem \ "endPosition").text.trim
    val time_end = if (endval != "") new Timestamp(dateParser.parseDateTime(endval).getMillis) else null

    LOGGER.info("read in station: " + name)
    new GLOSStation(name, id, desc, platformType, lat.head.toDouble, lon.head.toDouble, time_begin, time_end)
  }

}
