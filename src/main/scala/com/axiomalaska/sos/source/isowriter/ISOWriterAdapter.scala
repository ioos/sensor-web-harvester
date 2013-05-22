/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.axiomalaska.sos.source.isowriter

import com.axiomalaska.sos.ISOFileWriter
import com.axiomalaska.sos.data.SosStation
import com.axiomalaska.sos.source.data.LocalStation

class ISOWriterAdapter(private val writer: ISOWriter) extends ISOFileWriter {

  override def writeISOFileForStation(station: SosStation) = {
    station match {
      case localStation: LocalStation => writer.writeISOFile(localStation)
    }
  }
}
