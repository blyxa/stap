package com.blyxa.stap

case class Kafka(brokers:String)
case class StapConfig
(
  kafka: Map[String,Kafka]
)
