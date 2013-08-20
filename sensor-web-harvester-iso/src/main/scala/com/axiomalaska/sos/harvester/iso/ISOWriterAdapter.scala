/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.axiomalaska.sos.harvester.iso

import com.axiomalaska.sos.ISOFileWriter
import com.axiomalaska.sos.data.SosStation
import com.axiomalaska.sos.harvester.data.LocalStation

class ISOWriterAdapter(private val writer: ISOWriter) extends ISOFileWriter {

  override def writeISOFileForStation(station: SosStation) = {
    station match {
      case localStation: LocalStation => writer.writeISOFile(localStation)
    }
  }
}
