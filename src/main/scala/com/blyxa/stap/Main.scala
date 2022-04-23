package com.blyxa.stap

import org.apache.avro.Schema
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

import java.io.File
import java.time.Duration
import scala.jdk.CollectionConverters._

object Main {
  def main(args: Array[String]): Unit = {
    MakeCli.addMethod(MethodDef("listTopics", List(),
      (_,o,kf) => {
        val topics = kf.listAllTopics()
        o.out(s"found [${topics.size}] topics")
        topics.toSeq.sortBy(_._1).foreach { case (topic, (description, config, replicas)) =>
          val retentionMs = config.get("retention.ms").value().toLong
          val retentionHrs = if (retentionMs < 0) retentionMs else Duration.ofMillis(retentionMs).toHours
          val retentionBytes = config.get("retention.bytes").value().toLong
          val retentionMb = if (retentionBytes < 0) retentionBytes else retentionBytes / 1000000
          o.out(s"$topic partitions[${description.partitions.size}] replicaF[${description.partitions.asScala.head.replicas.size}] retention[$retentionHrs]hrs [$retentionMb]MB size[${replicas.map(_.size()).sum / 1000000}]MB")
        }
      }
    ))
    MakeCli.addMethod(MethodDef("readAvro", List("topicName","schemaFilePath"),
      (params,o,kf) => {
        read(
          AvroDecoder(new Schema.Parser().parse(new File(params("schemaFilePath")))),
          params("topicName"),o,kf
        )
      }
    ))
    MakeCli.addMethod(MethodDef("readString", List("topicName"),
      (params,o,kf) => {
        read(
          StringDecoder(),
          params("topicName"),o,kf
        )
      }
    ))
    def read(decoder:Decoder, topic:String, o:ConsoleOutput, kf:KafkaFunctions): Unit ={
      kf.read(topic, record=>{
        //o.out(s"partition[${record.kafkaRecord.partition()}] offset[${record.kafkaRecord.offset()}] ts[${DateHelper.fromEpochMilli(record.kafkaRecord.timestamp()).toString}]")
        o.out(decoder.deserialize(record).toJson)
      })
    }
    MakeCli.addMethod(MethodDef("describeGroup", List("groupName"),
      (params,o,kf) => {
        val groupId = params("groupName")
        val groupConsumerOffsets: Map[TopicPartition, OffsetAndMetadata] = kf.groupConsumerOffsets(groupId)
        val topicOffsets: Map[String, Map[Int, (Long, Long)]] = groupConsumerOffsets.map(_._1.topic()).toList.distinct.map{ t=>t->kf.getTopicOffsets(t)}.toMap
        groupConsumerOffsets
          .toSeq
          .groupBy(_._1.topic())
          .toList
          .sortBy(_._1)
          .foreach{ case (topicName, consumerOffsets: Seq[(TopicPartition, OffsetAndMetadata)]) =>
            o.out(s"topic[$topicName]")
            consumerOffsets.sortBy(_._1.partition()).foreach{ case (tp, metadata) =>
              val offsets = topicOffsets(topicName)(tp.partition())
              o.out(s"    partition[${tp.partition()}] offsets[${offsets._1},${metadata.offset()},${offsets._2}] metadata[${metadata.metadata()}]")
            }
          }
      }
    ))
    MakeCli.addMethod(MethodDef("listGroups", List(),
      (_, o, kf) => {
        kf.getGroups.foreach{g=>
          o.out(s"groupId[${g.groupId()}] state[${g.state().get().name()}]")
        }
      }
    ))
    MakeCli.addMethod(MethodDef("describeTopic", List("topic"),
      (params,o,kf) => {
        val topic = params("topic")
        val (topicDesc, config,_) = kf.describeTopics(List(topic))(topic)
        val consumerGroups = kf.getGroups
          .filter{g=>kf.groupConsumerOffsets(g.groupId()).count(_._1.topic()==topic)>1}
        val topicOffsets = kf.getTopicOffsets(topic)
        o.out(s"name[${topicDesc.name()}] id[${topicDesc.topicId().toString}] partitions[${topicDesc.partitions().size()}] replicaF[${topicDesc.partitions().asScala.head.replicas().size()}]")
        o.out(s"consumerGroups")
        o.out(s"    [${consumerGroups.map(_.groupId()).mkString(",")}]")
        o.out(s"partitions")
        topicDesc.partitions().asScala.foreach{tpi=>
          o.out(s"    partition[${tpi.partition()}] replicas[${tpi.replicas().asScala.map(_.host()).mkString(",")}] leader[${tpi.leader().host()}] offsets[${topicOffsets(tpi.partition())._1},${topicOffsets(tpi.partition())._2}]")
        }
        o.out(s"config")
        config.entries().asScala.toList.sortBy(_.name()).foreach{ce=>
          o.out(s"    ${ce.name()}[${ce.value()}]")
        }
      }
    ))
    MakeCli.doit(args)
  }
}
case class ConsoleOutput(){
  def out(a:Any): Unit = println(a)
}
