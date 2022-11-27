package com.blyxa.stap

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}

import java.io.{ByteArrayOutputStream, File}

case class AvroUtil(schemaFilePath:File){
  val schema = new Schema.Parser().parse(schemaFilePath)
  val reader:GenericDatumReader[GenericData.Record] = new GenericDatumReader(schema)

  val byteArrayOutputStream = new ByteArrayOutputStream
  val datumWriter = new GenericDatumWriter[GenericData.Record](schema)

  def jsonToGeneric(json:String): GenericData.Record ={
    val decoder = DecoderFactory.get().jsonDecoder(schema, json)
    reader.read(null, decoder)
  }

  def genericDataToBytes(record:GenericData.Record ): Array[Byte] = {
    byteArrayOutputStream.reset()
    val binaryEncoder = EncoderFactory.get.binaryEncoder(byteArrayOutputStream, null)
    datumWriter.write(record, binaryEncoder)
    binaryEncoder.flush()
    byteArrayOutputStream.toByteArray
  }
}
