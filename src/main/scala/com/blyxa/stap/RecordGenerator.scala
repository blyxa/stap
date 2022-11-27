package com.blyxa.stap

/**
 * Interface to be implemented by the python avro json generator script
 */
trait RecordGenerator {
  def generate():AvroRecordAsJson
}

case class AvroRecordAsJson(key:String, valueJson:String)
