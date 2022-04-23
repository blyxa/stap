package com.blyxa.stap

import com.blyxa.stap.DateHelper.FORMAT

import java.time.{Instant, ZoneOffset}
import java.time.format.DateTimeFormatter

object DateHelper {
  val FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss SSS Z").withZone(ZoneOffset.UTC)
  def fromEpochMilli(ts:Long):DateHelper = {
    DateHelper(Instant.ofEpochMilli(ts))
  }
}

case class DateHelper(instant:Instant){
  override def toString: String = FORMAT.format(instant)
}
