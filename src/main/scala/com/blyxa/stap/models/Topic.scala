package com.blyxa.stap.models

import com.blyxa.stap.{AsciiTableWrapper, KafkaFunctions}
import de.vandermeer.asciitable.AsciiTable
import org.apache.kafka.clients.admin.{Config, ConsumerGroupListing, ReplicaInfo, TopicDescription}

import scala.jdk.CollectionConverters._

case class Topic(name:String, description:TopicDescription, config:Config, replicaInfo:Iterable[ReplicaInfo]){

  def getConsumerGroups()(implicit kf:KafkaFunctions): List[ConsumerGroupListing] ={
    kf.getGroups
      .filter{g=>kf.groupConsumerOffsets(g.groupId()).count(_._1.topic()==name)>1}
  }

  def partitionsAsAsciiTable()(implicit kf:KafkaFunctions): AsciiTable ={
    val topicOffsets = kf.getTopicOffsets(name)
    val table = AsciiTableWrapper.newTable(Array("partition", "replicas", "leader", "offset (min)", "offset (max)"))
    description.partitions().asScala.foreach{tpi=>
      //o.out(s"    partition[${tpi.partition()}] replicas[${tpi.replicas().asScala.map(_.host()).mkString(",")}] leader[${tpi.leader().host()}] offsets[${topicOffsets(tpi.partition())._1},${topicOffsets(tpi.partition())._2}]")
      table.addRow(
        tpi.partition(),
        tpi.replicas().asScala.map(_.host()).mkString(","),
        tpi.leader().host(),
        topicOffsets(tpi.partition())._1,
        topicOffsets(tpi.partition())._2
      )
      table.addRule()
    }
    table
  }
  def configAsAsciiTable()(implicit kf:KafkaFunctions):AsciiTable = {
    val t = AsciiTableWrapper.newTable(Array("name", "value"))
    config.entries().asScala.toList.sortBy(_.name()).foreach{ce=>
      t.addRow(ce.name(), ce.value())
      t.addRule()
    }
    t
  }
}

case class Topics(topics:Seq[Topic]){
  def toAsciiTable: AsciiTable ={
    val table = AsciiTableWrapper.newTable(Array("topic", "partitions","replicas","retention (ms)","retention (byte)","size (byte)"))
    topics.sortBy(_.name).foreach { t =>
      val retentionMs = t.config.get("retention.ms").value().toLong
      val retentionBytes = t.config.get("retention.bytes").value().toLong
      table.addRow(
        t.description.name(),
        t.description.partitions.size,
        t.description.partitions.asScala.head.replicas.size,
        retentionMs,
        retentionBytes,
        t.replicaInfo.map(_.size()).sum
      )
    }
    table.addRule()
    table
  }
}
