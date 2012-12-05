/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.axiomalaska.sos.source.isowriter

import com.axiomalaska.sos.source.data.LocalStation

trait ISOWriter {
  def writeISOFile(station: LocalStation, sosURL: String)
}

