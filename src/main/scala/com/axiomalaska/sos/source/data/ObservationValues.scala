package com.axiomalaska.sos.source.data

import java.util.Calendar
import scala.collection.mutable
import com.axiomalaska.sos.data.SosSensor
import com.axiomalaska.phenomena.Phenomenon

class ObservationValues(val observedProperty: ObservedProperty, 
    val sensor:SosSensor, val phenomenon:Phenomenon, val units:String){
  private val valueCollection = new mutable.ListBuffer[java.lang.Double]()
  private val dateCollection = new mutable.ListBuffer[Calendar]()
  
  def getValues(): List[java.lang.Double] = {
    return valueCollection.toList
  }

  def getDates(): List[Calendar] = {
    return dateCollection.toList
  }
  
  def getDatesAndValues(): List[(java.lang.Double, Calendar)] ={
    return valueCollection.zip(dateCollection).toList
  }

  def get(index: Int): (java.lang.Double, Calendar) = {
    (valueCollection(index), dateCollection(index))
  }
  
  def containsDate(date:Calendar):Boolean = 
    dateCollection.exists(_.equals(date))

  def addValue(value: java.lang.Double, date: Calendar) {
    valueCollection += value
    dateCollection += date
  }
}