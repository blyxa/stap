package com.blyxa.stap

import com.blyxa.stap.models.Topic
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.apache.kafka.clients.admin._
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.config.ConfigResource.Type
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import org.slf4j.LoggerFactory

import java.io.{ByteArrayOutputStream, File}
import java.time.Duration
import java.util.Properties
import scala.collection.mutable
import scala.jdk.CollectionConverters._

case class KafkaFunctions()
(implicit
 val adminClient: AdminClient,
 val conf:Kafka)
{
  private val logger = LoggerFactory.getLogger(getClass)
  def consumer(): KafkaConsumer[Array[Byte],Array[Byte]] ={
    val config = new Properties()
    config.put("group.id", "stap")
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, conf.brokers)
    new KafkaConsumer(config, new ByteArrayDeserializer, new ByteArrayDeserializer)
  }
  def producer(): KafkaProducer[Array[Byte],Array[Byte]] ={
    val config = new Properties()
    config.put("group.id", "stap")
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, conf.brokers)
    new KafkaProducer(config, new ByteArraySerializer, new ByteArraySerializer)
  }
  def read(topicName:String, startFromLatest:Boolean = true, callback:ConsumerRecord[Array[Byte],Array[Byte]]=>Unit):Unit={
    val c = consumer()
    describeTopics(List(topicName)).get(topicName) match {
      case Some(topic) =>
        val topicPartitions = topic.description.partitions().asScala.map(tpi=> new TopicPartition(topicName,tpi.partition())).asJava
        c.assign(topicPartitions)
        if(startFromLatest) c.seekToEnd(topicPartitions)
        else c.seekToBeginning(topicPartitions)
        while(true){
          c.poll(Duration.ofSeconds(5)).asScala.foreach{r=>
            callback(r)
          }
        }
      case None => throw new Throwable(s"topic[$topicName] not found")
    }
  }
  def readAvro(topicName:String, schemaFilePath:File, startFromLatest:Boolean = true, callback: AvroRecord =>Unit): Unit ={
    val reader = AvroDecoder(new Schema.Parser().parse(schemaFilePath))
    val c= consumer()
    describeTopics(List(topicName)).get(topicName) match {
      case Some(topic) =>
        val topicPartitions = topic.description.partitions().asScala.map(tpi=> new TopicPartition(topicName,tpi.partition())).asJava
        c.assign(topicPartitions)
        if(startFromLatest) c.seekToEnd(topicPartitions)
        else c.seekToBeginning(topicPartitions)

        while(true){
          c.poll(Duration.ofSeconds(5)).asScala.foreach{r=>
            callback(reader.deserialize(r))
          }
        }
      case None => throw new Throwable(s"topic[$topicName] not found")
    }
  }
  def writeString(topicName:String, key:String, record:String): RecordMetadata ={
    val r = new ProducerRecord[Array[Byte],Array[Byte]](topicName, key.getBytes, record.getBytes)
    producer().send(r).get()
  }

  def writeAvro(topicName:String, schemaFilePath:File, key:String, recordJson:String): RecordMetadata ={
    val avroUtil = AvroUtil(schemaFilePath)
    val record = avroUtil.jsonToGeneric(recordJson)
    val bytes = avroUtil.genericDataToBytes(record)

    // create kafka producer record and send
    logger.info(s"writing key.size[${key.getBytes.length}] value.size[${bytes.length}]")
    val r = new ProducerRecord[Array[Byte],Array[Byte]](topicName, key.getBytes, bytes)
    producer().send(r).get()
  }
  def createTopic(req:CreateTopic): Topic ={
    val t= new NewTopic(req.name, req.partitions, req.replicationFactor)
    adminClient.createTopics(
      Seq(t.configs(req.configs.asJava)).asJava
    ).all().get()
    describeTopics(List(req.name))(req.name)
  }
  def listAllTopicNames(): Set[String] ={
    adminClient.listTopics().names().get().asScala.toSet
  }
  def describeTopics(topics:List[String]=List()): Map[String,Topic] = {
    val topicNames = if(topics.nonEmpty) topics else listAllTopicNames() .toList
    val topicDescriptions = adminClient.describeTopics(topicNames.asJava)
      .allTopicNames().get().asScala.toMap
    val configs = getTopicConfigs(topicNames.toSet)
    val nodes: mutable.Map[Integer, mutable.Map[String, LogDirDescription]] = clusterBrokers()

    val replicaInfos = nodes.values.flatMap(_.values.flatMap(_.replicaInfos().asScala)).groupBy(_._1.topic())
      .map{ case (topic, replicaInfos) =>
        topic -> replicaInfos.map(_._2)
      }

    topicDescriptions.map{ case (t, d) =>
      t -> Topic(t, d, configs(t), replicaInfos(t))}
  }
  def deleteTopic(name:String): Unit ={
    adminClient.deleteTopics(Set(name).asJava).all().get()
  }
  def clusterBrokers(): mutable.Map[Integer, mutable.Map[String, LogDirDescription]] ={
    val clusterBrokers = adminClient.describeCluster().nodes().get().asScala.map(_.id()).toSet
    val describeLogDirsResult = adminClient.describeLogDirs(clusterBrokers.map(Integer.valueOf).toSeq.asJava)
    val logDirInfosByBroker: mutable.Map[Integer, mutable.Map[String, LogDirDescription]] = describeLogDirsResult.allDescriptions.get().asScala.map { case (k, v) => k -> v.asScala }
    logDirInfosByBroker
  }
  def getTopicOffsets(topicName:String): Map[Int,(Long,Long)] ={
    val c = consumer()
    val topicPartitions = c.partitionsFor(topicName).asScala.map(pi=>new TopicPartition(pi.topic, pi.partition))
    val beginningOffsets = c.beginningOffsets(topicPartitions.asJava).asScala
      .map{ case (partition, offset) => partition.partition()->offset}
    val endOffsets = c.endOffsets(topicPartitions.asJava).asScala
      .map{ case (partition, offset) => partition.partition()->offset}
    (beginningOffsets.toSeq ++ endOffsets.toSeq).groupMap(_._1)(_._2).map{ case (p, offsets) =>
      p->(offsets.head, offsets.last)
    }
  }
  def getTopicConfigs(topicNames:Set[String]): Map[String, Config] ={
    adminClient.describeConfigs(topicNames.map(new ConfigResource(Type.TOPIC, _)).asJavaCollection)
      .all().get().asScala.toMap.map{ case (resource, config) => resource.name()->config}
  }
  def listAllTopics():Map[String,Topic] = describeTopics()

  def groupConsumerOffsets(groupId:String): Map[TopicPartition, OffsetAndMetadata] ={
    adminClient.listConsumerGroupOffsets(
      groupId,
      new ListConsumerGroupOffsetsOptions
    ).partitionsToOffsetAndMetadata().get().asScala.toMap
  }
  def getGroups: List[ConsumerGroupListing] ={
    adminClient.listConsumerGroups(new ListConsumerGroupsOptions()).all().get().asScala.toList
  }
}

case class CreateTopic(name:String, partitions:Int, replicationFactor:Short, configs:Map[String,String])
case class DescribeTopic(name:String)