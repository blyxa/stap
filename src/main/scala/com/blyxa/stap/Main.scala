package com.blyxa.stap

import com.blyxa.stap.models.Topics
import com.typesafe.config.ConfigRenderOptions
import org.apache.avro.Schema
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig}
import org.fusesource.jansi.Ansi
import pureconfig.ConfigSource
import scopt.OParser
import java.util.Properties
import scala.jdk.CollectionConverters._

object Main {
  def main(args: Array[String]): Unit = {
    implicit val stapConfig: StapConfig = getStapConfig
    val parser1 = ProgramArgs.build()
    val o = ConsoleOutput()
    OParser.parse(parser1, args, ProgramArgs()) match {
      case Some(programArgs) =>
        LogConfig.configLog(programArgs.verbose)

        programArgs.cmd match {
        case "topics" =>
          withContext(stapConfig, programArgs, kf=>{
            val topics = kf.listAllTopics()
            o.out(s"found [${topics.size}] topics")
            o.out(Topics(topics.values.toSeq).toAsciiTable.render())
          })

        case "createTopic" =>
          withContext(stapConfig, programArgs, kf=>{
            val topic = kf.createTopic(CreateTopic(
              programArgs.topic.get, programArgs.partitions, programArgs.replicas.toShort,programArgs.configs
            ))
            o.out(s"topic created name[${topic.name}] id[${topic.description.topicId()}]")
          })

        case "deleteTopic" =>
          withContext(stapConfig, programArgs, kf=>{
            kf.deleteTopic(programArgs.topic.get)
            o.out(s"Request topic[${programArgs.topic.get}] deletion. ${Ansi.ansi().fgBrightRed().a("(* might need to enable deleting topic. check [delete.topic.enable] property)").reset()}")
          })

        case "writeAvro" =>
          withContext(stapConfig, programArgs, kf=>{
            val res = kf.writeAvro(programArgs.topic.get,programArgs.schema.get,programArgs.key.get,programArgs.value.get)
            o.out(s"Record sent to topic[${res.topic()}] offset[${res.offset()}] partition[${res.partition()}]")
          })

        case "readAvro" =>
          withContext(stapConfig, programArgs, kf=>{
            val decoder = AvroDecoder(new Schema.Parser().parse(programArgs.schema.get))
            read(decoder, programArgs.topic.get,programArgs.startFromLatest,o, kf)
          })

        case "writeString" =>
          withContext(stapConfig, programArgs, kf=>{
            val res = kf.writeString(programArgs.topic.get,programArgs.key.get,programArgs.value.get)
            o.out(s"Record sent to topic[${res.topic()}] offset[${res.offset()}] partition[${res.partition()}]")
          })

        case "readString" =>
          withContext(stapConfig, programArgs, kf=>{
            val decoder = StringDecoder()
            read(decoder, programArgs.topic.get,programArgs.startFromLatest,o, kf)
          })

        case "describeTopic" =>
          withContext(stapConfig, programArgs, kf=>{
            implicit val kff: KafkaFunctions = kf
            val topic = kf.describeTopics(List(programArgs.topic.get))(programArgs.topic.get)
            val consumerGroups = topic.getConsumerGroups()
            o.out(s"name[${topic.name}] id[${topic.description.topicId()}] partitions[${topic.description.partitions().size()}] replicaF[${topic.description.partitions().asScala.head.replicas().size()}]")
            o.out(s"consumerGroups")
            o.out(s"    [${consumerGroups.map(_.groupId()).mkString(",")}]")
            o.out(topic.configAsAsciiTable().render())
            o.out(topic.partitionsAsAsciiTable().render())
          })
        case _ =>
          println("unsupported command. use --help for more info.")
      }

      case _ =>

    }
  }

  private def buildAdminClient()(implicit kafka:Kafka):AdminClient = {
    val properties = new Properties()
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.brokers)
    AdminClient.create(properties)
  }
  private def getStapConfig: StapConfig ={
    import pureconfig.generic.auto._
    import pureconfig.syntax._
    ConfigSource.file("stap.conf").load[StapConfig] match {
      case Left(_) =>
        val c = StapConfig(Map(
          "mykafkaclustername"->Kafka(brokers = "broker1:9092,broker2:9092"),
          "my-staging-kafka"->Kafka(brokers = "stg-broker1:9092")
        ))
        val ro = ConfigRenderOptions.defaults().setOriginComments(false)
        throw new Throwable(
          s"""stap.conf not found. example stap.conf file...
             |--------------------------------------------------------
             |${c.toConfig.render(ro)}
             |""".stripMargin)
      case Right(value) => value
    }
  }
  private def read(decoder:Decoder, topic:String, startFromLatest:Boolean, o:ConsoleOutput, kf:KafkaFunctions): Unit ={
    kf.read(topic, startFromLatest, record=>{
      o.out(decoder.deserialize(record).toJson)
    })
  }
  private def withContext(conf:StapConfig, programArgs:ProgramArgs, callback: KafkaFunctions =>Unit): Unit ={
    implicit val kafka: Kafka = conf.getBrokers(programArgs.targetKafka.get)
    implicit val adminClient: AdminClient = buildAdminClient()
    implicit val kf: KafkaFunctions = KafkaFunctions()
    callback(kf)
  }
}