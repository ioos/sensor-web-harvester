package com.axiomalaska.sos.source.data

import com.axiomalaska.sos.data.SosSource

class LocalSource(val source:Source) extends SosSource {

  	def getId() = source.tag

	def getName() = source.name

	def getCountry() = source.country

	def getEmail() = source.email

	def getWebAddress() = source.webAddress

	def getOperatorSector() = source.operatorSector
}