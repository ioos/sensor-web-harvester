package com.axiomalaska.sos.source.stationupdater

import org.apache.log4j.Logger

import com.axiomalaska.sos.tools.HttpSender
import com.axiomalaska.sos.source.BoundingBox
import com.axiomalaska.sos.source.StationQuery
import com.axiomalaska.sos.source.data.SourceId
import com.axiomalaska.sos.source.GeoTools
import com.axiomalaska.sos.source.observationretriever.SosRawDataRetriever
import com.axiomalaska.sos.source.data.Source
import com.axiomalaska.sos.source.data.DatabaseStation
import com.axiomalaska.sos.source.data.DatabaseSensor
import com.axiomalaska.sos.source.data.DatabasePhenomenon
import com.axiomalaska.sos.source.data.LocalPhenomenon
import com.axiomalaska.sos.source.data.ObservedProperty
import com.axiomalaska.phenomena.Phenomena
import com.axiomalaska.phenomena.Phenomenon
import com.axiomalaska.sos.data.Location

import net.opengis.sos.x10.ObservationOfferingType
import net.opengis.sos.x10.CapabilitiesDocument
import net.opengis.sos.x10.GetCapabilitiesDocument
import net.opengis.om.x10.CompositeObservationDocument
import net.opengis.gml.x32.ValueArrayPropertyType

import gov.noaa.ioos.x061.NamedQuantityType
import gov.noaa.ioos.x061.CompositePropertyType
import gov.noaa.ioos.x061.CompositeValueType
import gov.noaa.ioos.x061.ValueArrayType

class NoaaNosCoOpsStationUpdater(private val stationQuery: StationQuery,
  private val boundingBox: BoundingBox, 
  private val logger: Logger = Logger.getRootLogger()) extends StationUpdater {

  // ---------------------------------------------------------------------------
  // Private Data
  // ---------------------------------------------------------------------------
  
  private val stationUpdater = new StationUpdateTool(stationQuery, logger)
  private val httpSender = new HttpSender()
  private val geoTools = new GeoTools()
  private val stationIdParser = """station-(\w+)""".r
  private val observedPropteryRefParser = """.*/(\w+)""".r
  private val rawDataRetriever = new SosRawDataRetriever()
  private val serviceUrl = "http://opendap.co-ops.nos.noaa.gov/ioos-dif-sos/SOS"
  private val offeringTag = "urn:x-noaa:def:station:NOAA.NOS.CO-OPS::"
  private val observedPropertyTag = "http://mmisw.org/ont/cf/parameter/"
  private val source = stationQuery.getSource(SourceId.NOAA_NOS_CO_OPS)

  // ---------------------------------------------------------------------------
  // StationUpdater Members
  // ---------------------------------------------------------------------------
  
  def update() {
    logger.info("Getting source stations")
    val sourceStationSensors = getSourceStations(source)
    logger.info("Getting db stations")
    val databaseStations = stationQuery.getAllStations(source)
    logger.info("Updating db with new stations")
    stationUpdater.updateStations(sourceStationSensors, databaseStations)
  }
  
  val name = "NOAA NOS CO-OPS"
  
  // ---------------------------------------------------------------------------
  // Private Members
  // ---------------------------------------------------------------------------
  
  private def getSourceStations(source: Source): List[(DatabaseStation, List[(DatabaseSensor, List[DatabasePhenomenon])])] = {
    val observationOfferings = getObservationOfferingTypes()
    val size = observationOfferings.length - 1
    logger.info("Total number of stations not filtered: " + (size + 1))
    
    val stationSensorsCollection = for {
      (observationOfferingType, index) <- observationOfferings.zipWithIndex
      val station = createSouceStation(observationOfferingType, source)
      if (withInBoundingBox(station))
      val sourceObservedProperties = getSourceObservedProperties(
          observationOfferingType, station, source)
      val databaseObservedProperties = stationUpdater.updateObservedProperties(
          source, sourceObservedProperties)
      val sensors = stationUpdater.getSourceSensors(station, databaseObservedProperties)
      if(sensors.nonEmpty)
    } yield {
      logger.debug("[" + index + " of " + size + "] station: " + station.name)
      (station, sensors)
    }
    logger.info("finished with stations")
    
    return stationSensorsCollection
  }

  private def withInBoundingBox(station: DatabaseStation): Boolean = {
    val stationLocation = new Location(station.latitude, station.longitude)
    return geoTools.isStationWithinRegion(stationLocation, boundingBox)
  }

  private def getObservationOfferingTypes(): List[ObservationOfferingType] = {
    getCapabilitiesDocument() match {
      case Some(capabilitiesDocument) => {
        capabilitiesDocument.getCapabilities().getContents().getObservationOfferingList().getObservationOfferingArray().
          filter(observationOffering =>
            stationIdParser.findFirstIn(observationOffering.getId) != None).toList
      }
      case None => {
        Nil
      }
    }
  }
  
  private def getCapabilitiesDocument(): Option[CapabilitiesDocument] = {
    val getCapabilitiesDocument = GetCapabilitiesDocument.Factory.newInstance

    val getCapabilities = getCapabilitiesDocument.addNewGetCapabilities
    getCapabilities.setService("SOS")

    val httpSender = new HttpSender()
    val results = httpSender.sendPostMessage(
        serviceUrl, getCapabilitiesDocument.xmlText());

    if(results != null){
    	Some(CapabilitiesDocument.Factory.parse(results))
    }
    else{
      None
    }
  }

  private def getSourceObservedProperties(observationOfferingType: ObservationOfferingType, 
      station: DatabaseStation, source: Source): List[ObservedProperty] = {
    val namedQuantityTypes = getNamedQuantityTypes(observationOfferingType, station)

    val sourceObservedProperty = namedQuantityTypes.flatMap(namedQuantity => 
      matchObservedProperty(namedQuantity, source))

    return sourceObservedProperty
  }
  
  private def matchObservedProperty(namedQuantity: NamedQuantityType, 
      source: Source): Option[ObservedProperty] = {
    namedQuantity.getName match {
      case "WaterTemperature" => {
        getObservedProperty(Phenomena.instance.SEA_WATER_TEMPERATURE, namedQuantity.getName)
      }
      case "SignificantWaveHeight" => {
        getObservedProperty(Phenomena.instance.SEA_SURFACE_WAVE_SIGNIFICANT_HEIGHT, namedQuantity.getName)
      }
      case "DominantWavePeriod" => {
        getObservedProperty(Phenomena.instance.DOMINANT_WAVE_PERIOD, namedQuantity.getName)
      }
      case "AverageWavePeriod" => {
        getObservedProperty(Phenomena.instance.SEA_SURFACE_WAVE_MEAN_PERIOD, namedQuantity.getName)
      }
      case "SwellHeight" => {
        getObservedProperty(Phenomena.instance.SEA_SURFACE_SWELL_WAVE_SIGNIFICANT_HEIGHT, namedQuantity.getName)
      }
      case "SwellPeriod" => {
        getObservedProperty(Phenomena.instance.SEA_SURFACE_SWELL_WAVE_PERIOD, namedQuantity.getName)
      }
      case "WindWaveHeight" => {
        getObservedProperty(Phenomena.instance.SEA_SURFACE_WIND_WAVE_SIGNIFICANT_HEIGHT, namedQuantity.getName)
      }
      case "WindWavePeriod" => {
        getObservedProperty(Phenomena.instance.SEA_SURFACE_WIND_WAVE_PERIOD, namedQuantity.getName)
      }
      case "SwellWaveDirection" => {
        getObservedProperty(Phenomena.instance.SEA_SURFACE_SWELL_WAVE_TO_DIRECTION, namedQuantity.getName)
      }
      case "WindWaveDirection" => {
        getObservedProperty(Phenomena.instance.SEA_SURFACE_WIND_WAVE_TO_DIRECTION, namedQuantity.getName)
      }
      case "WindSpeed" => {
        getObservedProperty(Phenomena.instance.WIND_SPEED, namedQuantity.getName)
      }
      case "WindDirection" => {
        getObservedProperty(Phenomena.instance.WIND_FROM_DIRECTION, namedQuantity.getName)
      }
      case "WindVerticalVelocity" => {
        getObservedProperty(Phenomena.instance.WIND_VERTICAL_VELOCITY, namedQuantity.getName)
      }
      case "WindGust" => {
        getObservedProperty(Phenomena.instance.WIND_SPEED_OF_GUST, namedQuantity.getName)
      }
      case "WaterLevel" => {
        getObservedProperty(Phenomena.instance.createHomelessParameter("water_level", namedQuantity.getUom), namedQuantity.getName)
      }
      case "Salinity" => {
        getObservedProperty(Phenomena.instance.SALINITY, namedQuantity.getName)
      }
      case "Depth" => {
        getObservedProperty(Phenomena.instance.SEA_FLOOR_DEPTH_BELOW_SEA_SURFACE, namedQuantity.getName)
      }
      case "CurrentDirection" => {
        getObservedProperty(Phenomena.instance.CURRENT_DIRECTION, namedQuantity.getName)
      }
      case "CurrentSpeed" => {
        getObservedProperty(Phenomena.instance.CURRENT_SPEED, namedQuantity.getName)
      }
      case "AirTemperature" => {
        getObservedProperty(Phenomena.instance.AIR_TEMPERATURE, namedQuantity.getName)
      }
      case "BarometricPressure" => {
        getObservedProperty(Phenomena.instance.AIR_PRESSURE, namedQuantity.getName)
      }
      case "SamplingRate" => None
      case "WaveDuration" => None
      case "MeanWaveDirectionPeakPeriod" => None
      case _ =>{
        logger.debug(" observed property: " + namedQuantity.getName +
          " is not processed correctly.")
        return None
      }
    }
  }
  
  private def getObservedProperty(phenomenon: Phenomenon, foreignTag: String) : Option[ObservedProperty] = {
    try {
      var localPhenom: LocalPhenomenon = new LocalPhenomenon(new DatabasePhenomenon(phenomenon.getId))
      var units: String = if (phenomenon.getUnit == null || phenomenon.getUnit.getSymbol == null) "none" else phenomenon.getUnit.getSymbol
      if (localPhenom.databasePhenomenon.id < 0) {
        localPhenom = new LocalPhenomenon(insertPhenomenon(localPhenom.databasePhenomenon, units, phenomenon.getId, phenomenon.getName))
      }
      return new Some[ObservedProperty](stationUpdater.createObservedProperty(foreignTag, source, localPhenom.getUnit.getSymbol, localPhenom.databasePhenomenon.id))
    } catch {
      case ex: Exception => {}
    }
    None
}

  private def insertPhenomenon(dbPhenom: DatabasePhenomenon, units: String, description: String, name: String) : DatabasePhenomenon = {
    dbPhenom.units = units
    dbPhenom.description = description
    dbPhenom.name = name
    stationQuery.createPhenomenon(dbPhenom)
  }
  
  private def convertUnits(originalUnits:String)=
    originalUnits match{
      case "deg" => "degrees"
      case x => x
    }

  private def createSouceStation(observationOfferingType: ObservationOfferingType, 
      source: Source): DatabaseStation = {
    val stationIdParser(foreignId) = observationOfferingType.getId
    val label = observationOfferingType.getDescription().getStringValue
    val (lat, lon) = getLatLon(observationOfferingType)

    logger.info("Processing station: " + label)
    return new DatabaseStation(label, foreignId, foreignId, "", "BUOY", source.id, lat, lon)
  }

  private def getLatLon(observationOfferingType: ObservationOfferingType): (Double, Double) = {
    val latLon = observationOfferingType.getBoundedBy().getEnvelope().getLowerCorner().getListValue
    val lat = latLon.get(0).toString().toDouble
    val lon = latLon.get(1).toString().toDouble
    
    return (lat,lon)
  }

  private def getNamedQuantityTypes(observationOfferingType: ObservationOfferingType, 
      station: DatabaseStation): List[NamedQuantityType] = {
    val sensorForeignIds = getSensorForeignIds(observationOfferingType)
      
    val namedQuantityTypes = for (sensorForeignId <- sensorForeignIds) yield {
      val rawData = rawDataRetriever.getRawDataLatest(serviceUrl, offeringTag, observedPropertyTag,
        station.foreign_tag, sensorForeignId)

        if(rawData == null){
          return Nil
        }
      createCompositeObservationDocument(rawData) match {
        case Some(document) => {
          try {
            val compositePropertyType: CompositePropertyType =
              document.getObservation().getResult().asInstanceOf[CompositePropertyType]

            val valueArrayType: ValueArrayType =
              compositePropertyType.getComposite().getValueComponents().getAbstractValueArray(1).asInstanceOf[ValueArrayType]

            val compositeValueType: CompositeValueType =
              valueArrayType.getValueComponents().getAbstractValueArray(0).asInstanceOf[CompositeValueType]

            val valueArrayType1: ValueArrayType =
              compositeValueType.getValueComponents().getAbstractValueArray(1).asInstanceOf[ValueArrayType]

            val propertyNames = for {
              xmlObject <- valueArrayType1.getValueComponents().getAbstractValueArray();
              val compositeValue = xmlObject.asInstanceOf[CompositeValueType]
              namedQuantityType <- getValues(compositeValue.getValueComponents())
            } yield {
              namedQuantityType
            }

            propertyNames.toList.distinct
          } catch {
            case e:Exception => {
              logger.error(" station ID: " + station.foreign_tag + " sensorForeignId: " + sensorForeignId + " error: parsing data ")
              Nil
            }
          }
        }
        case None => {
          Nil
        }
      }
    }
    
    return namedQuantityTypes.flatten
  }

  private def createCompositeObservationDocument(data: String): 
      Option[CompositeObservationDocument] = {
    if (!data.contains("ExceptionReport")) {
      try {
        val compositeObservationDocument =
          CompositeObservationDocument.Factory.parse(data)

        return Some[CompositeObservationDocument](compositeObservationDocument)
      } catch {
        case e: Exception => //println("--------------\n" + data) //do nothing
      }
    }

    return None
  }

  private def getValues(valuePropertyType: ValueArrayPropertyType): List[NamedQuantityType] = {
    valuePropertyType.getAbstractValueArray(1) match {
      case compositeValueType: CompositeValueType => {
        val values = compositeValueType.getValueComponents().getAbstractValueArray().collect {
          case namedQuantity: NamedQuantityType => namedQuantity
        }

        values.toList
      }
      case _ => Nil
    }
  }
  
  private def sensorForeignNotUsed = List("harmonic_constituents", "datums")

  private def getSensorForeignIds(observationOfferingType: ObservationOfferingType): List[String] = {
    val sensorForeignIds = observationOfferingType.getObservedPropertyArray().map(o => {
      val observedPropteryRefParser(property) = o.getHref
      property
    })
    
    val filteredSensorForeignIds = sensorForeignIds.filter(sensorForeignId => !sensorForeignNotUsed.contains(sensorForeignId))

    return filteredSensorForeignIds.toList
  }
}