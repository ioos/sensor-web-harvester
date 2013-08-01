package com.axiomalaska.sos.source.data

import org.joda.time.DateTime

case class RawValues(phenomenonForeignTag:String, 
    values:List[(DateTime, Double)])