package com.blyxa.stap

case class Kafka(brokers:String)
case class StapConfig
(
  kafka: Map[String,Kafka]
){
  def getBrokers(name:String):Kafka = {
    kafka.get(name) match {
      case Some(value) => value
      case None => throw new Throwable(s"clusterName[${name}] not found in stap.conf. available are[${kafka.keys.mkString(",")}]")
    }
  }
}
