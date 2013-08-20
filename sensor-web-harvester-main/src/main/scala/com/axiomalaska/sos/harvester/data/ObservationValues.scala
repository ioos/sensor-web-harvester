package com.axiomalaska.sos.harvester.data

import java.util.Calendar
import scala.collection.mutable
import com.axiomalaska.sos.data.SosSensor
import com.axiomalaska.phenomena.Phenomenon
import org.joda.time.DateTime

class ObservationValues(val observedProperty: ObservedProperty, 
    val sensor:SosSensor, val phenomenon:Phenomenon, val units:String){
  private val valueCollection = new mutable.ListBuffer[java.lang.Double]()
  private val dateCollection = new mutable.ListBuffer[DateTime]()
  
  def getValues(): List[java.lang.Double] = {
    return valueCollection.toList
  }

  def getDates(): List[DateTime] = {
    return dateCollection.toList
  }
  
  def getDatesAndValues(): List[(java.lang.Double, DateTime)] ={
    return valueCollection.zip(dateCollection).toList
  }

  def get(index: Int): (java.lang.Double, DateTime) = {
    (valueCollection(index), dateCollection(index))
  }
  
  def containsDate(date:DateTime):Boolean = 
    dateCollection.exists(_.equals(date))

  def addValue(value: java.lang.Double, date: DateTime) {
    valueCollection += value
    dateCollection += date
  }
}