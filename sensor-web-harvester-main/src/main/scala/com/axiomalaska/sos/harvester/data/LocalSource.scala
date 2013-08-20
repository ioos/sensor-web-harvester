package com.axiomalaska.sos.harvester.data

import com.axiomalaska.sos.data.SosSource

class LocalSource(val source:Source) extends SosSource {

    setId(source.tag)
    setName(source.name)
    setCountry(source.country)
    setEmail(source.email)
    setWebAddress(source.webAddress)
    setOperatorSector(source.operatorSector)
    setAddress(source.address)
    setCity(source.city)
    setState(source.state)
    setZipcode(source.zipcode)
}