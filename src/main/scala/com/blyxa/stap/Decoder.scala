package com.blyxa.stap

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory
import org.apache.kafka.clients.consumer.ConsumerRecord

trait Decoder{
  def deserialize(r:ConsumerRecord[Array[Byte], Array[Byte]]):DecodedRecord
}
case class AvroDecoder(avroSchema:Schema) extends  Decoder{
  val datumReader = new GenericDatumReader[GenericRecord](avroSchema)
  def deserialize(r:ConsumerRecord[Array[Byte], Array[Byte]]):AvroRecord = {
    val decoder = DecoderFactory.get().binaryDecoder(r.value(),null)
    AvroRecord(r, datumReader.read(null, decoder))
  }
}
case class StringDecoder() extends Decoder{
  override def deserialize(r: ConsumerRecord[Array[Byte], Array[Byte]]): DecodedRecord = {
    StringRecord(r, new String(r.value()))
  }
}
