package com.axiomalaska.sos.source

import org.squeryl.PrimitiveTypeMode._
import org.squeryl.SessionFactory
import org.squeryl.Session
import org.squeryl.adapters.PostgreSqlAdapter
import com.axiomalaska.sos.source.data.DatabaseStation
import com.axiomalaska.sos.source.data.DatabaseSensor
import com.axiomalaska.sos.source.data.Source
import com.axiomalaska.sos.source.data.DatabasePhenomenon
import com.axiomalaska.sos.source.data.ObservedProperty
import com.axiomalaska.sos.source.data.StationDatabase

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
class StationQueryBuilder(url:String, 
	user:String, password:String) {

  def withStationQuery[A](op: StationQuery => A): A = {
    val stationQuery = new StationQueryImp(url, user, password)
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
  def getAllStations(source: Source): List[DatabaseStation]
  def getActiveStations(source: Source): List[DatabaseStation]
  def getAllSource():List[Source]
  def getAllSensors(station:DatabaseStation):List[DatabaseSensor]
  def getActiveSensors(station:DatabaseStation):List[DatabaseSensor]
  def createSource(name: String, tag:String): Source
  def getSource(id:Long):Source
  def getStation(id:Long): DatabaseStation
  def getSource(station:DatabaseStation):Source
  def createStation(station: DatabaseStation): DatabaseStation
  def getAllPhenomena():List[DatabasePhenomenon]
  def getPhenomena(sensor:DatabaseSensor):List[DatabasePhenomenon]
  def getObservedProperties(station: DatabaseStation,
    sensor: DatabaseSensor, phenomenon:DatabasePhenomenon):List[ObservedProperty]
  def updateStation(originalStation: DatabaseStation, newStation: DatabaseStation)
  def associateSensorToStation(station: DatabaseStation, sensor: DatabaseSensor)
  def getObservedProperty(foreignTag: String, depth:Double, source: Source): Option[ObservedProperty]
  def updateObservedProperty(databaseObservedProperty: ObservedProperty,
    newObservedProperty: ObservedProperty)
  def createObservedProperty(observedProperty: ObservedProperty): ObservedProperty
  def getPhenomenon(id:Long):DatabasePhenomenon 
  def createPhenomenon(phenomenon:DatabasePhenomenon):DatabasePhenomenon
  def createSensor(databaseStation: DatabaseStation, sensor:DatabaseSensor):DatabaseSensor
  def associatePhenomonenToSensor(sensor: DatabaseSensor, phenonomen: DatabasePhenomenon)
  def getObservedProperty(source: Source): List[ObservedProperty]
}

private class StationQueryImp(url:String, 
	user:String, password:String) extends StationQuery {
  Class.forName("org.postgresql.Driver")
  
  private val session = createSession()

  SessionFactory.concreteFactory = Some(() => session)
  
  // ---------------------------------------------------------------------------
  // StationQuery Members
  // ---------------------------------------------------------------------------
  
  def getObservedProperty(source: Source): List[ObservedProperty] ={
    using(session){
      source.observedProperties.toList
    }
  }
  
  def createPhenomenon(phenomenon:DatabasePhenomenon):DatabasePhenomenon ={
    using(session){
      StationDatabase.phenomena.insert(phenomenon)
    }
  }
  def createSensor(databaseStation: DatabaseStation, sensor:DatabaseSensor):DatabaseSensor ={
    using(session){
      val vaildSensor = new DatabaseSensor(sensor.tag, sensor.description, 
          databaseStation.id, sensor.depth)
      StationDatabase.sensors.insert(vaildSensor)
    }
  }
  
  def associatePhenomonenToSensor(sensor: DatabaseSensor, phenonomen: DatabasePhenomenon) {
    using(session) {
      sensor.phenomena.associate(phenonomen)
    }
  }
  
  def createObservedProperty(observedProperty: ObservedProperty): ObservedProperty = {
    using(session) {
      StationDatabase.observedProperties.insert(observedProperty)
    }
  }
  
  def updateObservedProperty(databaseObservedProperty: ObservedProperty,
    newObservedProperty: ObservedProperty) {
    using(session) {
      update(StationDatabase.observedProperties)(s =>
        where(s.foreign_tag === databaseObservedProperty.foreign_tag and
            s.source_id === databaseObservedProperty.source_id and 
            s.depth === databaseObservedProperty.depth)
          set (s.foreign_units := newObservedProperty.foreign_units,
            s.phenomenon_id := newObservedProperty.phenomenon_id))
    }
  }

  def getObservedProperty(foreignTag: String, depth:Double, source: Source): 
	  Option[ObservedProperty] ={
    using(session) {
      val observedProperties = source.observedProperties.where(observedProperty =>
        observedProperty.foreign_tag === foreignTag and observedProperty.depth === depth)

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
            s.longitude := newStation.longitude))
    }
  }
  
  def getObservedProperties(station: DatabaseStation,
    sensor: DatabaseSensor, phenomenon:DatabasePhenomenon):List[ObservedProperty] ={
    using(session) {
      val source = station.source.head
      
      source.observedProperties.where(op => 
        op.phenomenon_id === phenomenon.id and op.depth === sensor.depth).toList
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
      java.sql.DriverManager.getConnection(url, user, password),
      new PostgreSqlAdapter)
  }
}