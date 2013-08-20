package com.axiomalaska.sos.harvester.data

import org.joda.time.DateTime

case class RawValues(phenomenonForeignTag:String, 
    values:List[(DateTime, Double)])