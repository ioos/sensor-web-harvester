package com.axiomalaska.sos.source

import javax.measure.unit.SI
import javax.measure.unit.NonSI
import javax.measure.unit.Unit
import javax.measure.Measure
import com.axiomalaska.sos.source.data.ObservationValues

abstract class UnitsConverter {
  def convert(observationValues: ObservationValues): ObservationValues
}

object UnitsConverter {
  def getConverter(observationValues: ObservationValues): UnitsConverter = {
    if(observationValues.units.toUpperCase() == 
    		observationValues.phenomenon.getUnits.toUpperCase()){
      new NullUnitsConverter()
    } else {
      (observationValues.observedProperty.foreign_units.toUpperCase(), 
          observationValues.phenomenon.getUnits.toUpperCase()) match {
        case ("F", "C") => new FahrenheitToCelsiusUnitsConverter()
        case ("FEET", "M") => new FeetToMUnitsConverter()
        case ("MBAR", "PA") => new MbarToPaConverter()
        case ("MB", "PA") => new MbarToPaConverter()
        case ("MM/HG", "PA") => new MMOfMercuryToPaConverter()
        case ("INCHES OF MERCURY", "PA") => new InchesOfMercuryToPaConverter()
        case ("MPH", "CM/S") => new MilesPerHourToCmPerSecondUnitsConverter()
        case ("INCHES", "M") => new InchesToMUnitsConverter()
        case ("IN", "M") => new InchesToMUnitsConverter()
        case ("CM", "M") => new CmToMUnitsConverter()
        case ("MM", "M") => new MmToMUnitsConverter()
        case ("M/S", "CM/S") => new MetersPerSecondToCmPerSecondConverter()
        case ("KNOTS", "CM/S") => new KnotsToCmPerSecondConverter()
        case ("HPA", "PA") => new HpaToPaConverter()
        case ("DEGREES", "DEGREE") => new NameConverter("degree")
        case ("DEG", "DEGREE") => new NameConverter("degree")
        case ("STD UNITS", "PH") => new NameConverter("pH")
        case ("A (HOURLY AVERAGE)", "A/HOUR") => new NameConverter("A/hour")
        case ("% SATURATION", "%") => new NameConverter("%")
        case ("PPM", "MG/L") =>  new NameConverter("mg/l")
        case ("PPT", "PSU") =>  new NameConverter("psu")
        case ("UMOL/MOL", "MG/L") =>  new NameConverter("MG/L")
        case ("UMHOS/CM", "MS/M") => new UmhosPerCmToMsPerMUnitsConverter()
        case ("VOLT","V") =>  new NameConverter("V")
        case ("WATT","W/M2") =>  new NameConverter("W/M2")
        case _ => new NullUnitsConverter()
      }
    }
  }
}

// reference http://books.google.com/books?id=qHoWToUx1uYC&pg=PA201&lpg=PA201&dq=convert+UMHOS/CM+to+MS/M&source=bl&ots=JCJq_Paf7f&sig=x-YZ0-iVWufPVbs3cf7sBvhOXTA&hl=en&sa=X&ei=mEQjT-X6A4egiQLqydHuBw&sqi=2&ved=0CEIQ6AEwBA#v=onepage&q=convert%20UMHOS%2FCM%20to%20MS%2FM&f=false
private class UmhosPerCmToMsPerMUnitsConverter extends UnitsConverter {
  def convert(observationValues: ObservationValues): ObservationValues = {

    val copySensorObservationValues = new ObservationValues(observationValues.observedProperty, 
        observationValues.sensor, observationValues.phenomenon, "mS/m")
    
    for ((value, date) <- observationValues.getDatesAndValues()) {
      copySensorObservationValues.addValue(value/10, date)
    }

    return copySensorObservationValues
  }
}

private class MilesPerHourToCmPerSecondUnitsConverter extends UnitsConverter {
  def convert(observationValues: ObservationValues): ObservationValues = {

    val copySensorObservationValues = new ObservationValues(observationValues.observedProperty, 
        observationValues.sensor, observationValues.phenomenon, "cm/s")

    for ((value, date) <- observationValues.getDatesAndValues()) {
      val convertedValue = Measure.valueOf(value, NonSI.MILES_PER_HOUR).doubleValue(SI.METERS_PER_SECOND)

      copySensorObservationValues.addValue(convertedValue*100, date)
    }

    return copySensorObservationValues
  }
}

private class NameConverter(newUnits:String) extends UnitsConverter {
  def convert(observationValues: ObservationValues): ObservationValues = {

    val copySensorObservationValues = new ObservationValues(observationValues.observedProperty, 
        observationValues.sensor, observationValues.phenomenon, newUnits)

    for ((value, date) <- observationValues.getDatesAndValues()) {
      copySensorObservationValues.addValue(value, date)
    }

    copySensorObservationValues
  }
}

//1 mbar = 1hPa http://en.wikipedia.org/wiki/Pascal_(unit)
private class MbarToPaConverter extends UnitsConverter {
  def convert(observationValues: ObservationValues): ObservationValues = {

    val copySensorObservationValues = new ObservationValues(observationValues.observedProperty, 
        observationValues.sensor, observationValues.phenomenon, "Pa")

    for ((value, date) <- observationValues.getDatesAndValues()) {
      copySensorObservationValues.addValue(value*100, date)
    }

    copySensorObservationValues
  }
}

private class HpaToPaConverter extends UnitsConverter {
  def convert(observationValues: ObservationValues): ObservationValues = {

    val copySensorObservationValues = new ObservationValues(observationValues.observedProperty, 
        observationValues.sensor, observationValues.phenomenon, "Pa")

    for ((value, date) <- observationValues.getDatesAndValues()) {
      copySensorObservationValues.addValue(value*100, date)
    }

    copySensorObservationValues
  }
}

private class MetersPerSecondToCmPerSecondConverter extends UnitsConverter {
  def convert(observationValues: ObservationValues): ObservationValues = {

    val copySensorObservationValues = new ObservationValues(observationValues.observedProperty, 
        observationValues.sensor, observationValues.phenomenon, "cm/s")

    for ((value, date) <- observationValues.getDatesAndValues()) {
      copySensorObservationValues.addValue(value*100, date)
    }

    copySensorObservationValues
  }
}

private class KnotsToCmPerSecondConverter extends UnitsConverter {
  def convert(observationValues: ObservationValues): ObservationValues = {

    val copySensorObservationValues = new ObservationValues(observationValues.observedProperty, 
        observationValues.sensor, observationValues.phenomenon, "cm/s")

    for ((value, date) <- observationValues.getDatesAndValues()) {
      val convertedValue = Measure.valueOf(value,
        NonSI.KNOT).doubleValue(SI.METERS_PER_SECOND)

      copySensorObservationValues.addValue(convertedValue*100, date)
    }

    copySensorObservationValues
  }
}

private class MMOfMercuryToPaConverter extends UnitsConverter {
  def convert(observationValues: ObservationValues): ObservationValues = {

    val copySensorObservationValues = new ObservationValues(observationValues.observedProperty, 
        observationValues.sensor, observationValues.phenomenon, "Pa")

    for ((value, date) <- observationValues.getDatesAndValues()) {
      val convertedValue = Measure.valueOf(value,
        NonSI.MILLIMETER_OF_MERCURY).doubleValue(SI.PASCAL)

      copySensorObservationValues.addValue(convertedValue, date)
    }

    copySensorObservationValues
  }
}

private class InchesOfMercuryToPaConverter extends UnitsConverter {
  def convert(observationValues: ObservationValues): ObservationValues = {

    val copySensorObservationValues = new ObservationValues(observationValues.observedProperty, 
        observationValues.sensor, observationValues.phenomenon, "Pa")

    for ((value, date) <- observationValues.getDatesAndValues()) {
      val convertedValue = Measure.valueOf(value,
        NonSI.INCH_OF_MERCURY).doubleValue(SI.PASCAL)

      copySensorObservationValues.addValue(convertedValue, date)
    }

    copySensorObservationValues
  }
}

private class FeetToMUnitsConverter extends UnitsConverter {
  def convert(observationValues: ObservationValues): ObservationValues = {

    val copySensorObservationValues = new ObservationValues(observationValues.observedProperty, 
        observationValues.sensor, observationValues.phenomenon, "m")

    for ((value, date) <- observationValues.getDatesAndValues()) {
      val convertedValue = Measure.valueOf(value,
        NonSI.FOOT).doubleValue(SI.METER)

      copySensorObservationValues.addValue(convertedValue, date)
    }

    copySensorObservationValues
  }
}

private class CmToMUnitsConverter extends UnitsConverter {
  def convert(observationValues: ObservationValues): ObservationValues = {

    val copySensorObservationValues = new ObservationValues(observationValues.observedProperty, 
        observationValues.sensor, observationValues.phenomenon, "m")

    for ((value, date) <- observationValues.getDatesAndValues()) {
      val convertedValue = Measure.valueOf(value,
        SI.CENTIMETER).doubleValue(SI.METER)

      copySensorObservationValues.addValue(convertedValue, date)
    }

    copySensorObservationValues
  }
}

private class MmToMUnitsConverter extends UnitsConverter {
  def convert(observationValues: ObservationValues): ObservationValues = {

    val copySensorObservationValues = new ObservationValues(observationValues.observedProperty, 
        observationValues.sensor, observationValues.phenomenon, "m")

    for ((value, date) <- observationValues.getDatesAndValues()) {
      val convertedValue = Measure.valueOf(value,
        SI.MILLIMETER).doubleValue(SI.METER)

      copySensorObservationValues.addValue(convertedValue, date)
    }

    copySensorObservationValues
  }
}

private class InchesToMUnitsConverter extends UnitsConverter {
  def convert(observationValues: ObservationValues): ObservationValues = {

    val copySensorObservationValues = new ObservationValues(observationValues.observedProperty, 
        observationValues.sensor, observationValues.phenomenon, "m")

    for ((value, date) <- observationValues.getDatesAndValues()) {
      val convertedValue = Measure.valueOf(value,
        NonSI.INCH).doubleValue(SI.METER)

      copySensorObservationValues.addValue(convertedValue, date)
    }

    copySensorObservationValues
  }
}

private class NullUnitsConverter extends UnitsConverter {
  def convert(observationValues: ObservationValues): ObservationValues = {
    return observationValues
  }
}

private class FahrenheitToCelsiusUnitsConverter extends UnitsConverter {
  def convert(observationValues: ObservationValues): ObservationValues = {

    val copySensorObservationValues = new ObservationValues(observationValues.observedProperty, 
        observationValues.sensor, observationValues.phenomenon, "C")

    for ((value, date) <- observationValues.getDatesAndValues()) {
      val convertedValue = Measure.valueOf(value,
        NonSI.FAHRENHEIT).doubleValue(SI.CELSIUS)

      copySensorObservationValues.addValue(convertedValue, date)
    }

    copySensorObservationValues
  }
}