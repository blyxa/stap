package com.blyxa.stap

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
import org.apache.avro.io.EncoderFactory
import org.apache.kafka.clients.consumer.ConsumerRecord

import java.io.ByteArrayOutputStream

abstract class DecodedRecord(val original:ConsumerRecord[Array[Byte],Array[Byte]]){
  def toJson:String
  def toPrettyJson:String
}
case class StringRecord(kafkaRecord:ConsumerRecord[Array[Byte], Array[Byte]], decoded:String)extends DecodedRecord(kafkaRecord){
  override def toJson: String = decoded
  override def toPrettyJson: String = toJson
}
object AvroRecord  {
  val om = new ObjectMapper()
  val baos = new ByteArrayOutputStream()
}
case class AvroRecord(kafkaRecord:ConsumerRecord[Array[Byte], Array[Byte]], r:GenericRecord)extends DecodedRecord(kafkaRecord){
  import AvroRecord._
  override def toJson:String = {
    val j = jsonNode.toString
    baos.reset()
    j
  }
  override def toPrettyJson:String = {
    val j = jsonNode.toPrettyString
    baos.reset()
    j
  }
  private def jsonNode:JsonNode = {
    val writer = new GenericDatumWriter[GenericRecord](r.getSchema)
    val encoder = EncoderFactory.get().jsonEncoder(r.getSchema, baos)
    writer.write(r, encoder)
    encoder.flush()
    om.readTree(baos.toString)
  }
}
