package com.axiomalaska.sos.harvester

import javax.measure.unit.SI
import javax.measure.unit.NonSI
import javax.measure.unit.Unit
import javax.measure.Measure
import com.axiomalaska.sos.harvester.data.ObservationValues
import com.axiomalaska.phenomena.Phenomenon

abstract class UnitsConverter {
  def convert(observationValues: ObservationValues): ObservationValues
}

object UnitsConverter {
  def getConverter(observationValues: ObservationValues): UnitsConverter = {
    val tag = headOfPhenomenonTag(observationValues.phenomenon)
    (observationValues.observedProperty.foreign_units.toUpperCase(),
      observationValues.phenomenon.getUnit().toString().toUpperCase()) match {
      case (a, b) if (a == b) => new NullUnitsConverter()
      case ("F", "CEL") => new FahrenheitToCelsiusUnitsConverter()
      case ("FEET", "M") => new FeetToMUnitsConverter()
      case ("MBAR", "PA") => new MbarToPaConverter()
      case ("MB", "PA") => new MbarToPaConverter()
      case ("MM/HG", "PA") => new MMOfMercuryToPaConverter()
      case ("INCHES OF MERCURY", "PA") => new InchesOfMercuryToPaConverter()
      case ("INCHES", "M") => new InchesToMUnitsConverter()
      case ("IN", "M") => new InchesToMUnitsConverter()
      case ("CM", "M") => new CmToMUnitsConverter()
      case ("MM", "M") => new MmToMUnitsConverter()
      case ("HPA", "PA") => new HpaToPaConverter()
      case ("DEGREES", "DEG") => new NameConverter("Deg")
      case ("A (HOURLY AVERAGE)", "A.H-1") => new NameConverter("A.H-1")
      case ("IN", "MM") => new InchesToMMUnitsConverter()
      case ("INCHES", "MM") => new InchesToMMUnitsConverter()
      case ("MPH", "M.S-1") => new MilesPerHourToMPerSecondUnitsConverter()
      case ("KNOTS", "M.S-1") => new KnotsToMPerSecondConverter()
      case ("C", "CEL") => new NameConverter("Cel")
      case ("W/M2", "W.M-2") => new NameConverter("W.M-2")
      case ("% SATURATION", "%") => new NameConverter("%")
      case ("STD UNITS", "1") 
      	if(tag == 
        "sea_water_ph_reported_on_total_scale") => new NameConverter("1")
      case ("UNIT", "%") 
      	if(tag == "relative_permittivity") => new NameConverter("%")
      case ("PCT", "%") =>  new NameConverter("%")
      case ("DEGR", "DEG") => new NameConverter("Deg")
      case ("M/S", "M.S-1") => new NameConverter("M.S-1")
      case ("GRAM", "PPT") => new PPThousandsToPPTrillionConverter()
      case ("CFS", "M3.S-1") => new CubicFootPerSecondToM3PerSecondConverter()
      case ("MG/L", "KG.M-3") => new MGramPerLiterToKilogramsPerMeterCubiedConverter()
      case ("PPM", "KG.M-3") => new PPMillionToKilogramsPerMeterCubiedConverter()
      case ("UMHOS/CM", "S.M-1") => new UmhosPerCmToSPerMUnitsConverter()
      case ("UMOL/MOL", "KG.M-3") => new UmolPerMolToKgramPerMeterCubieConverter()
      case ("WATT", "W.M-2") => new NameConverter("W.M-2")
      case ("feet3 s-1", "m3 s-1") => new CubicFeetPerSecondToCubicMeterPerSecondConverter()
      case _ => new NullUnitsConverter()
      }
  }
  
  def headOfPhenomenonTag(phenomenon: Phenomenon): String = {
    val index = phenomenon.getId().lastIndexOf("/") + 1
    val tag = phenomenon.getId().substring(index)
    tag
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

private class CubicFeetPerSecondToCubicMeterPerSecondConverter extends UnitsConverter {
  def convert(observationValues: ObservationValues): ObservationValues = {

    val copySensorObservationValues =  new ObservationValues(observationValues.observedProperty, 
        observationValues.sensor, observationValues.phenomenon, "m3 s-1")

    for ((value, date) <- observationValues.getDatesAndValues()) {
      val convertedValue = value * 0.0283168466
      copySensorObservationValues.addValue(convertedValue, date)
    }

    return copySensorObservationValues
  }
}

private class UmolPerMolToKgramPerMeterCubieConverter extends UnitsConverter {
  def convert(observationValues: ObservationValues): ObservationValues = {

    val copySensorObservationValues =  new ObservationValues(observationValues.observedProperty, 
        observationValues.sensor, observationValues.phenomenon, "KG.M-3")

    for ((value, date) <- observationValues.getDatesAndValues()) {
      val convertedValue = value/1000
      copySensorObservationValues.addValue(convertedValue, date)
    }

    return copySensorObservationValues
  }
}

// reference http://books.google.com/books?id=qHoWToUx1uYC&pg=PA201&lpg=PA201&dq=convert+UMHOS/CM+to+MS/M&source=bl&ots=JCJq_Paf7f&sig=x-YZ0-iVWufPVbs3cf7sBvhOXTA&hl=en&sa=X&ei=mEQjT-X6A4egiQLqydHuBw&sqi=2&ved=0CEIQ6AEwBA#v=onepage&q=convert%20UMHOS%2FCM%20to%20MS%2FM&f=false
private class UmhosPerCmToSPerMUnitsConverter extends UnitsConverter {
  def convert(observationValues: ObservationValues): ObservationValues = {

    val copySensorObservationValues = new ObservationValues(observationValues.observedProperty, 
        observationValues.sensor, observationValues.phenomenon, "S/m")

    for ((value, date) <- observationValues.getDatesAndValues()) {
      copySensorObservationValues.addValue(value/10000, date)
    }

    return copySensorObservationValues
  }
}

private class PPMillionToKilogramsPerMeterCubiedConverter extends UnitsConverter {
  def convert(observationValues: ObservationValues): ObservationValues = {

    val copySensorObservationValues = new ObservationValues(observationValues.observedProperty, 
        observationValues.sensor, observationValues.phenomenon, "KG.M-3")

    for ((value, date) <- observationValues.getDatesAndValues()) {
      val convertedValue = 1000 * value
      copySensorObservationValues.addValue(convertedValue, date)
    }

    return copySensorObservationValues
  }
}

private class MGramPerLiterToKilogramsPerMeterCubiedConverter extends UnitsConverter {
  def convert(observationValues: ObservationValues): ObservationValues = {

    val copySensorObservationValues =  new ObservationValues(observationValues.observedProperty, 
        observationValues.sensor, observationValues.phenomenon, "KG.M-3")

    for ((value, date) <- observationValues.getDatesAndValues()) {
      val convertedValue = 0.001 * value
      copySensorObservationValues.addValue(convertedValue, date)
    }

    return copySensorObservationValues
  }
}

private class CubicFootPerSecondToM3PerSecondConverter extends UnitsConverter {
  def convert(observationValues: ObservationValues): ObservationValues = {

    val copySensorObservationValues = new ObservationValues(observationValues.observedProperty, 
        observationValues.sensor, observationValues.phenomenon, "M3.S-1")

    for ((value, date) <- observationValues.getDatesAndValues()) {
      val convertedValue = 0.0283168466 * value
      copySensorObservationValues.addValue(convertedValue, date)
    }

    return copySensorObservationValues
  }
}

private class PPThousandsToPPTrillionConverter extends UnitsConverter {
  def convert(observationValues: ObservationValues): ObservationValues = {

    val copySensorObservationValues = new ObservationValues(observationValues.observedProperty, 
        observationValues.sensor, observationValues.phenomenon, "ppt")

    for ((value, date) <- observationValues.getDatesAndValues()) {
      val convertedValue = 1000000000 * value
      copySensorObservationValues.addValue(convertedValue, date)
    }

    return copySensorObservationValues
  }
}

private class KnotsToMPerSecondConverter extends UnitsConverter {
  def convert(observationValues: ObservationValues): ObservationValues = {

    val copySensorObservationValues =  new ObservationValues(observationValues.observedProperty, 
        observationValues.sensor, observationValues.phenomenon, "m.s-1")

    for ((value, date) <- observationValues.getDatesAndValues()) {
      val convertedValue = Measure.valueOf(value,
        NonSI.KNOT).doubleValue(SI.METERS_PER_SECOND)

      copySensorObservationValues.addValue(convertedValue, date)
    }

    copySensorObservationValues
  }
}

private class MilesPerHourToMPerSecondUnitsConverter extends UnitsConverter {
  def convert(observationValues: ObservationValues): ObservationValues = {

    val copySensorObservationValues = new ObservationValues(observationValues.observedProperty, 
        observationValues.sensor, observationValues.phenomenon, "M.S-1")

    for ((value, date) <- observationValues.getDatesAndValues()) {
      val convertedValue = Measure.valueOf(value, NonSI.MILES_PER_HOUR).doubleValue(SI.METERS_PER_SECOND)

      copySensorObservationValues.addValue(convertedValue, date)
    }

    return copySensorObservationValues
  }
}

private class InchesToMMUnitsConverter extends UnitsConverter {
  def convert(observationValues: ObservationValues): ObservationValues = {

    val copySensorObservationValues = new ObservationValues(observationValues.observedProperty, 
        observationValues.sensor, observationValues.phenomenon, "mm")

    for ((value, date) <- observationValues.getDatesAndValues()) {
      val convertedValue = Measure.valueOf(value,
        NonSI.INCH).doubleValue(SI.MILLIMETER)

      copySensorObservationValues.addValue(convertedValue, date)
    }

    copySensorObservationValues
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