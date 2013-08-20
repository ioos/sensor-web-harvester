package com.axiomalaska.sos.harvester
import io.Source._
import org.apache.log4j.Logger
import org.squeryl.PrimitiveTypeMode._
import org.squeryl.SessionFactory
import org.squeryl.Session
import com.axiomalaska.sos.harvester.data.Network
import org.h2.tools.RunScript
import com.axiomalaska.sos.harvester.data.DatabaseStation
import com.axiomalaska.sos.harvester.data.Source
import com.axiomalaska.sos.harvester.data.DatabaseSensor
import com.axiomalaska.sos.harvester.data.DatabasePhenomenon
import com.axiomalaska.sos.harvester.data.ObservedProperty
import com.axiomalaska.sos.harvester.data.StationDatabase
import org.squeryl.adapters.H2Adapter

/**
 * The StationQueryBuilder builds the StationQuery object that is used to 
 * interact with the database. The StationQueryBuilder is the only way to build
 * a StationQuery, because this prevents a user from not closing the database connection
 * once they are done with it. The StationQueryBuilder should be used like below
 * 
    val queryBuilder = new StationQueryBuilder()

    queryBuilder.withStationQuery(stationQuery => {
      // perform code with the stationQuery object. 
    })
 */
class StationQueryBuilder(url:String) {  
  def withStationQuery[A](op: StationQuery => A): A = {
    val stationQuery = new StationQueryImp(url)
    try {
      op(stationQuery)
    } finally {
      stationQuery.close()
    }
  }
}

/**
 * The StationQuery is the object used to interact with the database
 */
trait StationQuery{
  def init
  def getAllStations(source: Source): List[DatabaseStation]
  def getActiveStations(source: Source): List[DatabaseStation]
  def getAllSource():List[Source]
  def getAllSensors(station:DatabaseStation):List[DatabaseSensor]
  def getActiveSensors(station:DatabaseStation):List[DatabaseSensor]
  def createSource(name: String, tag:String): Source
  def getSource(id:Long):Source
  def getStation(id:Long): DatabaseStation
  def getStation(foreign_tag:String): Option[DatabaseStation]
  def getSource(station:DatabaseStation):Source
  def createStation(station: DatabaseStation): DatabaseStation
  def getAllPhenomena():List[DatabasePhenomenon]
  def getPhenomena(sensor:DatabaseSensor):List[DatabasePhenomenon]
  def getPhenomena():List[DatabasePhenomenon]
  def getObservedProperties(station: DatabaseStation,
    sensor: DatabaseSensor, phenomenon:DatabasePhenomenon):List[ObservedProperty]
  def updateStation(originalStation: DatabaseStation, newStation: DatabaseStation)
  def associateSensorToStation(station: DatabaseStation, sensor: DatabaseSensor)
  def getObservedProperty(foreignTag: String, depth:Double, phenomenon_id:Long, 
      source: Source): Option[ObservedProperty]
  def updateObservedProperty(databaseObservedProperty: ObservedProperty,
    newObservedProperty: ObservedProperty)
  def createObservedProperty(observedProperty: ObservedProperty): ObservedProperty
  def getPhenomenon(id:Long):DatabasePhenomenon 
  def getPhenomenon(tag:String):Option[DatabasePhenomenon]
  def createPhenomenon(phenomenon:DatabasePhenomenon):DatabasePhenomenon
  def createSensor(databaseStation: DatabaseStation, sensor:DatabaseSensor):DatabaseSensor
  def associatePhenomonenToSensor(sensor: DatabaseSensor, phenonomen: DatabasePhenomenon)
  def getObservedProperty(source: Source): List[ObservedProperty]
  
  def getNetworks(source:Source):List[Network]
  
  def getNetworks(station:DatabaseStation):List[Network]
}

private class StationQueryImp(url:String) extends StationQuery {
  Class.forName("org.h2.Driver")
  
  private val session = createSession()
  
  private val logger = Logger.getRootLogger()

  SessionFactory.concreteFactory = Some(() => session)
  
  // ---------------------------------------------------------------------------
  // StationQuery Members
  // ---------------------------------------------------------------------------

  def init ={
    using(session){
      val inputStreamReader = fromInputStream(getClass.getClassLoader.getResourceAsStream("initdb.sql")).reader
      RunScript.execute(session.connection, inputStreamReader)
    }
  }
  
  def getObservedProperty(source: Source): List[ObservedProperty] ={
    using(session){
      source.observedProperties.toList
    }
  }
  
  def createPhenomenon(phenomenon:DatabasePhenomenon):DatabasePhenomenon ={
    using(session){
      // only insert if it doesn't exist... ??
      val inPhenomenaList = StationDatabase.phenomena.toList.filter(p => p.tag.equalsIgnoreCase(phenomenon.tag))
      if (inPhenomenaList.isEmpty)
        StationDatabase.phenomena.insert(phenomenon)
      else
        inPhenomenaList.head
    }
  }
  def createSensor(databaseStation: DatabaseStation, sensor:DatabaseSensor):DatabaseSensor ={
    using(session){
      val vaildSensor = new DatabaseSensor(sensor.tag, sensor.description, 
          databaseStation.id)
      StationDatabase.sensors.insert(vaildSensor)
    }
  }
  
  def associatePhenomonenToSensor(sensor: DatabaseSensor, phenonomen: DatabasePhenomenon) {
    try {
      using(session) {
        sensor.phenomena.associate(phenonomen)
      }
    } catch {
      case ex: Exception => { logger.warn(ex.toString) }
    }
  }
  
  def createObservedProperty(observedProperty: ObservedProperty): ObservedProperty = {
    val obsProp = if (observedProperty.foreign_units != null) {
      observedProperty 
    } else {
        new ObservedProperty(observedProperty.foreign_tag, 
            observedProperty.source_id, "none", 
            observedProperty.phenomenon_id, observedProperty.depth)
    }
    using(session) {
      StationDatabase.observedProperties.insert(obsProp)
    }
  }
  
  def updateObservedProperty(databaseObservedProperty: ObservedProperty,
    newObservedProperty: ObservedProperty) {
    val foreign_units = if (newObservedProperty.foreign_units != null) {
      newObservedProperty.foreign_units
    } else "none"
        
    using(session) {
      update(StationDatabase.observedProperties)(s =>
        where(s.foreign_tag === databaseObservedProperty.foreign_tag and
            s.source_id === databaseObservedProperty.source_id and 
            s.depth === databaseObservedProperty.depth and 
            s.phenomenon_id === newObservedProperty.phenomenon_id)
          set (s.foreign_units := newObservedProperty.foreign_units))
    }
  }

  def getObservedProperty(foreignTag: String, depth:Double, phenomenon_id:Long, source: Source): 
	  Option[ObservedProperty] ={
    using(session) {
      val observedProperties = source.observedProperties.where(observedProperty =>
        observedProperty.foreign_tag === foreignTag and observedProperty.depth === depth and 
        observedProperty.phenomenon_id === phenomenon_id)

      return observedProperties.headOption
    }
  }

  def updateStation(originalStation: DatabaseStation, newStation: DatabaseStation) {
    using(session) {
      update(StationDatabase.stations)(s =>
        where(s.foreign_tag === originalStation.foreign_tag)
          set (s.name := newStation.name,
              s.tag := newStation.tag,
            s.latitude := newStation.latitude, 
            s.longitude := newStation.longitude,
            s.timeBegin := newStation.timeBegin,
            s.timeEnd := newStation.timeEnd))
    }
  }
  
  def getObservedProperties(station: DatabaseStation,
    sensor: DatabaseSensor, phenomenon:DatabasePhenomenon):List[ObservedProperty] ={
    using(session) {
      val source = station.source.head
      
      source.observedProperties.where(op => 
        op.phenomenon_id === phenomenon.id).toList
    }
  }
  
  def getSource(id:Long):Source ={
    using(session) {
      return StationDatabase.sources.lookup(id).get
    }
  }
  
  def getSource(station:DatabaseStation):Source ={
    using(session) {
      return station.source.head
    }
  }
  
  def getNetworks(source:Source):List[Network] ={
    using(session){
      return source.networks.toList
    }
  }
  
  def getNetworks(station:DatabaseStation):List[Network] ={
    using(session){
      return station.networks.toList
    }
  }
  
  def getAllStations(source: Source): List[DatabaseStation] = {
    using(session) {
      return source.stations.toList
    }
  }
  
  def getActiveStations(source: Source): List[DatabaseStation] = {
    using(session) {
      return source.stations.where(station => station.active === true).toList
    }
  }
  
  def getStation(id:Long): DatabaseStation = {
    using(session) {
      return StationDatabase.stations.lookup(id).get
    }
  }
  
  def getStation(foreign_tag: String): Option[DatabaseStation] = {
    using(session) {
      return StationDatabase.stations.where(station => 
        station.foreign_tag === foreign_tag).headOption
    }
  }
  
  def createStation(station: DatabaseStation): DatabaseStation = {
    using(session) {
      return StationDatabase.stations.insert(station)
    }
  }
  
  def getAllSource():List[Source] ={
    using(session) {
      return StationDatabase.sources.toList
    }
  }
  
  def createSource(name: String, tag:String): Source = {
    using(session) {
    	return StationDatabase.sources.insert(new Source(name, tag, 
    	    "", "", "", "", "", "", "", ""))
    }
  }
  
  def getAllSensors(station:DatabaseStation):List[DatabaseSensor] ={
    using(session) {
      station.sensors.toList
    }
  }
  
  def getActiveSensors(station:DatabaseStation):List[DatabaseSensor] ={
    using(session) {
      station.sensors.where(sensor => sensor.active === true).toList
    }
  }
  
  def getPhenomena(sensor:DatabaseSensor):List[DatabasePhenomenon] ={
    using(session) {
      sensor.phenomena.toList
    }
  }
  
  def getAllPhenomena():List[DatabasePhenomenon] ={
    using(session) {
      StationDatabase.phenomena.toList
    }
  }
  
  def getPhenomenon(id:Long):DatabasePhenomenon ={
    using(session) {
      StationDatabase.phenomena.lookup(id).get
    }
  }
  
  def getPhenomenon(tag:String):Option[DatabasePhenomenon] = {
    using (session) {
      StationDatabase.phenomena.where(phen => phen.tag === tag).headOption
    }
  }
  
  def getPhenomena():List[DatabasePhenomenon] = {
    using(session) {
      StationDatabase.phenomena.toList
    }
  }
  
  def associateSensorToStation(station: DatabaseStation, sensor: DatabaseSensor) {
    using(session) {
      station.sensors.associate(sensor)
    }
  }
  
  // ---------------------------------------------------------------------------
  // Public Members
  // ---------------------------------------------------------------------------
  
  def close(){
    session.close
  }
  
  // ---------------------------------------------------------------------------
  // Private Members
  // ---------------------------------------------------------------------------
  
  private def createSession(): org.squeryl.Session = {
    Session.create(
      java.sql.DriverManager.getConnection(url),new H2Adapter)
  }
}