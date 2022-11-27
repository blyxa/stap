package com.blyxa.stap

import scopt.OParser
import java.io.File

/**
 * This class and companion object represents the cli arguments
 * It contains all supported commands and it's options
 */
case class ProgramArgs
(
  cmd:String="",
  targetKafka:Option[String]=None,
  topic:Option[String]=None,
  group:Option[String]=None,
  partitions:Int =0,
  replicas:Int=0,
  configs: Map[String, String] = Map(),
  schema:Option[File] = None,
  key:Option[String] =None,
  value:Option[String] =None,
  startFromLatest:Boolean = true,
  recordGenIntervalMs:Int = 1000,
  recordGenCount:Int = 10,
  pythonFile:Option[File]=None,
  verbose:Int = 0
)

object ProgramArgs {
  def build(): OParser[Unit,ProgramArgs] ={
    val builder = OParser.builder[ProgramArgs]
    val parser1 = {
      import builder._

      val target = opt[String]('t',"target")
        .action((x, c) => c.copy(targetKafka = Some(x)))
        .text("kafka target in stap.conf")
        .required()
      val topic = opt[String]('n',"name")
        .action((x, c) => c.copy(topic = Some(x)))
        .text("name of topic")
        .required()
      val schema = opt[File]('s',"schema")
        .action((x, c) => c.copy(schema = Some(x)))
        .text("schema file")
        .required()
      val readFromLatest = opt[Boolean]('p',"startFromLatest")
        .action((x, c) => c.copy(startFromLatest = x))
        .text("start from latest or oldest")

      OParser.sequence(
        programName("stap"),
        head("kafka cli tool"),
        help("help").text("prints this usage text"),
        opt[Int]("verbose")
          .validate(x =>
            if (x >= 0 && x<=3) success
            else failure("Value must be 0 to 3 inclusive"))
          .action((x, c) => c.copy(verbose = x))
          .text("log level 0-3"),
        note(""),

        /*
        topics
         */
        cmd("topics")
          .action((_, c) => c.copy(cmd = "topics"))
          .text("show topics")
          .children(
            target,
          ),
        note(""),

        /*
        readAvro
         */
        cmd("readAvro")
          .action((_, c) => c.copy(cmd = "readAvro"))
          .text("read avro data from topic")
          .children(
            target,
            topic,
            schema,
            readFromLatest
          ),
        note(""),

        /*
        writeAvro
         */
        cmd("writeAvro")
          .action((_, c) => c.copy(cmd = "writeAvro"))
          .text("write avro data to topic")
          .children(
            target,
            topic,
            schema,
            opt[String]('k',"key")
              .action((x, c) => c.copy(key = Some(x)))
              .text("kafka record key")
              .required(),
            opt[String]('v',"value")
              .action((x, c) => c.copy(value = Some(x)))
              .text("value as json string")
              .required()
          ),
        note(""),

        /*
        readString
         */
        cmd("readString")
          .action((_, c) => c.copy(cmd = "readString"))
          .text("read as string data from topic")
          .children(
            target,
            topic,
            readFromLatest
          ),
        note(""),

        /*
        deleteTopic
         */
        cmd("deleteTopic")
          .action((_, c) => c.copy(cmd = "deleteTopic"))
          .text("delete topic")
          .children(
            target,
            topic,
          ),
        note(""),

        /*
        describeGroup
         */
        cmd("describeTopic")
          .action((_, c) => c.copy(cmd = "describeTopic"))
          .text("describe topic")
          .children(
            target,
            topic,
          ),
        note(""),

        /*
        createTopic
         */
        cmd("createTopic")
          .action((_, c) => c.copy(cmd = "createTopic"))
          .text("create topic")
          .children(
            target,
            topic,
            opt[Int]('p',"partitions")
              .action((x, c) => c.copy(partitions = x))
              .text("partition count")
              .required(),
            opt[Int]('r',"replicas")
              .action((x, c) => c.copy(replicas = x))
              .text("replica count")
              .required(),
            opt[Map[String, String]]('c',"configs")
              .valueName("k1=v1,k2=v2...")
              .action((x, c) => c.copy(configs = x))
              .text("additional name/value configurations. retention.ms=60000,retention.bytes=64000 "),
          ),
        note(""),

        /*
        genAvro
         */
        cmd("genAvro")
          .action((_, c) => c.copy(cmd = "genAvro"))
          .text("generate avro records topic")
          .children(
            target,
            topic,
            schema,
            opt[Int]('i',"intervalMs")
              .action((x, c) => c.copy(recordGenIntervalMs = x))
              .text("interval between record generation. default 1000."),
            opt[Int]('n',"number")
              .action((x, c) => c.copy(recordGenCount = x))
              .text("number of records to generate. -1 forever. default 10.")
          ),
        note(""),
      )
    }
    parser1
  }
}

