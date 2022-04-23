package com.blyxa.stap

import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig}

import java.util.Properties

case class MethodDef
(
  name: String,
  params:List[String]=List(),
  method:Method,
  description: String=""
){
  def signature:String = s"$name(${params.mkString(",")})"
}
trait Method{
  private def buildAdminClient()(implicit kafka: Kafka):AdminClient = {
    val properties = new Properties()
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.brokers)
    AdminClient.create(properties)
  }
  private def context(clusterName:String,
              callback:(ConsoleOutput, KafkaFunctions)=>Unit)(implicit conf:StapConfig): String ={
    val o = ConsoleOutput()
    implicit val kafka: Kafka = {
      conf.kafka.get(clusterName) match {
        case Some(value) => value
        case None => throw new Throwable(s"clusterName[$clusterName] not found in stap.conf. available are[${conf.kafka.keys.mkString(",")}]")
      }
    }
    implicit val adminClient: AdminClient = buildAdminClient()
    val kf = KafkaFunctions()
    callback(o, kf)
    o.toString
  }
  def doit(clusterName:String, params:Map[String,String])(implicit conf:StapConfig): Unit ={
    context(clusterName,(o,kf)=>invoke(params, o, kf))
  }
  def invoke(params:Map[String,String], o:ConsoleOutput, kf:KafkaFunctions):Unit
}