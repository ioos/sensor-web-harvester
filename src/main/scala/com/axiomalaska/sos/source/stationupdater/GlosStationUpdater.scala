/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.axiomalaska.sos.source.stationupdater

import com.axiomalaska.sos.source.StationQuery
import com.axiomalaska.phenomena.Phenomena
import com.axiomalaska.phenomena.Phenomenon
import com.axiomalaska.sos.source.BoundingBox
import com.axiomalaska.sos.source.data.SourceId
import com.axiomalaska.sos.source.data.DatabaseStation
import com.axiomalaska.sos.source.data.DatabaseSensor
import com.axiomalaska.sos.source.data.DatabasePhenomenon
import com.axiomalaska.sos.source.data.LocalPhenomenon
import com.axiomalaska.sos.source.data.ObservedProperty
import com.axiomalaska.sos.tools.HttpSender
import org.apache.commons.net.ftp.FTPReply
import org.apache.log4j.Logger
import java.io.ByteArrayOutputStream
import java.io.File
import org.apache.commons.net.ftp.FTPClient
import scala.collection.mutable
import com.axiomalaska.sos.source.data.PhenomenaFactory

//case class GlosStation (stationName: String, stationId: String, stationDesc: String, lat: Double, lon: Double)

case class GLOSStation (stationName: String, stationId: String, stationDesc: String, platformType: String, lat: Double, lon: Double)

class GlosStationUpdater (private val stationQuery: StationQuery,
  private val boundingBox: BoundingBox) extends StationUpdater {

  val name = "GLOS"
  
  //ftp info
  private val ftp_host = "glos.us"
  private val ftp_port = 21
  private val ftp_user = "asa"
  private val ftp_pass = "AGSLaos001"
  private val glos_ftp: FTPClient = new FTPClient()
  private val LOGGER = Logger.getLogger(getClass())
    
  private val temp_file = "platform.csv"
  private val glos_metadata_folder = "glosbuoy_metadata/"
    
  private val phenomenaFactory = new PhenomenaFactory()
  
  private val source = stationQuery.getSource(SourceId.GLOS)
  private val stationUpdater = new StationUpdateTool(stationQuery)
  private var stationList: List[String] = Nil
  
  private var phenomenaList = stationQuery.getPhenomena
  
  // default mmisw url
  private val MAX_FILE_LIMIT = 2500
  private var filesToMove: List[String] = List()
  private var filesInMemory: List[scala.xml.Elem] = List()
  
  ////////// DEBUG VALs //////////////////////////////////////////
  private val DEBUG: Boolean = false  // enable to run on local debug test files
  private val DEBUG_DIR: String = "C:/Users/scowan/Desktop/Temp"
  ////////////////////////////////////////////////////////////////
  
  def update() {
    
    LOGGER.info("Updating GLOS...")
    
    val sourceStationSensors = getSourceStations()

    val databaseStations = stationQuery.getAllStations(source)

    stationUpdater.updateStations(sourceStationSensors, databaseStations)
    
    LOGGER.info("Finished updating GLOS")
    
  }
  
  private def getSourceStations() : List[(DatabaseStation, List[(DatabaseSensor, List[DatabasePhenomenon])])] = {
    // read ftp for data files for stations
    if (!DEBUG) {
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
    }
    // read local ISOs for metadata
    val dir = new File(glos_metadata_folder)
    LOGGER.info(dir.listFiles.length + " files in directory")
    val finallist = for (file <- dir.listFiles; if file.getName.contains(".xml")) yield {
        val readin = scala.io.Source.fromFile(file)
        val xml = scala.xml.XML.loadString(readin.mkString)
//        LOGGER.info("read in xml:\n" + xml)
        // read in the station data
        val station = readStationFromXML(xml)
        // get the obs data, need this for the depth values
        if (DEBUG) readInDebugFilesIntoMemory(station.stationName)
        LOGGER.info(filesInMemory.size + " files for station")
        val dataXML = if (!DEBUG) readInData(station.stationName) else readInDataDebug()
        if (dataXML.ne(null)) {
          LOGGER.info("reading xml file for station: " + station.stationName)
          val depths = readInDepths(dataXML)
          val stationDB = new DatabaseStation(station.stationName, station.stationId, 
              station.stationId, station.stationDesc, station.platformType, 
              SourceId.GLOS, station.lat, station.lon)
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
  
  private def readInData(stationid: String) : scala.xml.Elem = {
    val byteStream: ByteArrayOutputStream = new ByteArrayOutputStream()
    var retval: scala.xml.Elem = null
    try {
      for (file <- glos_ftp.listFiles) {
        if (file.getName.contains(stationid)) {
          glos_ftp.retrieveFile(file.getName, byteStream) match {
            case true => {
                try {
                  retval = scala.xml.XML.loadString(byteStream.toString("UTF-8").trim)
                  return retval
                } catch {
                  case ex: Exception => 
                    LOGGER.error("Could not open file: " + file.getName + "\n" + ex.toString)
                }
                retval = null
            }
            case _ => {
                retval = null
            }
          }
        }
      }
    } catch {
      case ex: Exception => LOGGER.error(ex.toString)
    }
    LOGGER.info("could not find a data xml for " + stationid)
    return retval
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
    LOGGER.info("looking for tag: " + tag)
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
      case "sun_radiation" => {
        val url = Phenomena.GLOS_FAKE_MMI_URL_PREFIX + "rads"
        return phenomenaFactory.findCustomPhenomenon(url)
      }
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
      val phenom = findPhenomenon(name)
      val foreignTag = getForeignTagFromName(name, index)
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
//    val metadata = (xml \ "gmi:MI_Metadata")
    val name = (xml \ "fileIdentifier" \ "CharacterString").text.trim
    val id = name
    var desc = (xml \ "identificationInfo" \\ "abstract" \ "CharacterString").text.trim
    val lat = for (north <- xml \\ "northBoundLatitude" \ "Decimal") yield { north.text.trim }
    val lon = for (west <- xml \\ "westBoundLongitude" \ "Decimal") yield { west.text.trim }
    val platformType = "BUOY"
    if (desc.length > 254) 
      desc = desc.slice(0, 252) + "..."
    LOGGER.info("read in station: " + name)
    new GLOSStation(name, id, desc, platformType, lat.head.toDouble, lon.head.toDouble)
  }
  
   ///////////////////////////////////////////////////////////////
   ////////               Debug                     //////////////
   ///////////////////////////////////////////////////////////////
   
  private def readInDebugFilesIntoMemory(stationid: String) {
    val stid: String = if (stationid.contains(":")) {
      stationid.substring(stationid.lastIndexOf(":")+1)
    } else {
      stationid
    }
    try {
      val dir = new File(DEBUG_DIR)
      var fileCount = 0
      val fileList = for {
        file <- dir.listFiles()
        val currentCount = fileCount
        if (file.getName.contains(".xml") && 
            file.getName.toLowerCase.contains(stid.toLowerCase) && currentCount < MAX_FILE_LIMIT)
      } yield {
        filesToMove = file.getAbsolutePath :: filesToMove
        fileCount += 1
        loadFileDebug(file)
      }
      filesInMemory = fileList.filter(_.isDefined).map(_.get).toList
    } catch {
      case ex: Exception => { ex.printStackTrace() }
    }
  }

  private def loadFileDebug(file: java.io.File) : Option[scala.xml.Elem] = {
    try {
      new Some[scala.xml.Elem](scala.xml.XML.loadFile(file))
    } catch {
      case ex: Exception => { None }
    }
  }
  
  private def readInDataDebug() : scala.xml.Elem = {
    val byteStream: ByteArrayOutputStream = new ByteArrayOutputStream()
    var retval: scala.xml.Elem = null
    try {
      for (file <- filesInMemory) {
        return file
      }
    } catch {
      case ex: Exception => LOGGER.error(ex.toString)
    }
    return retval
  }
}