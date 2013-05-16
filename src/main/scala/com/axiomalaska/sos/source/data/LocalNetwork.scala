package com.axiomalaska.sos.source.data

import com.axiomalaska.sos.data.SosNetwork

class LocalNetwork(val databaseNetwork:Network) extends SosNetwork{
  
	def getId() = databaseNetwork.tag
	
	def getDescription() = databaseNetwork.description
	
	def getSourceId() = databaseNetwork.sourceTag
	
	def getLongName() = databaseNetwork.longName
	
	def getShortName() = databaseNetwork.shortName
}