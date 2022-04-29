package com.blyxa.stap


import ch.qos.logback.classic.encoder.PatternLayoutEncoder
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.{Level, LoggerContext}
import ch.qos.logback.core.ConsoleAppender
import com.typesafe.config.ConfigRenderOptions
import org.fusesource.jansi.Ansi.{Color, ansi}
import org.fusesource.jansi.AnsiConsole
import org.jline.terminal.TerminalBuilder
import org.slf4j.LoggerFactory
import pureconfig.ConfigSource

import scala.collection.mutable

object MakeCli{

  // Ansi color init
  AnsiConsole.systemInstall()

  final val TERMINAL_WIDTH = TerminalBuilder.terminal().getWidth
  final val HEADER_PADDING = TERMINAL_WIDTH

  private lazy val logger = LoggerFactory.getLogger(getClass)

  // main entry point
  def doit(args:Array[String]): Unit ={
    try {
      // Configure logging and verbosity refer to configLog()
      configLog(args.filter(_.startsWith("-v")) match {
        case v if v.length==1 => v.head.count(_=='v')
        case _ => 0
      })

      // Init configuration
      implicit val conf: StapConfig = getConfig

      // Check arguments for help
      val argsFiltered = args.filter(!_.startsWith("-v"))
      if(argsFiltered.isEmpty || argsFiltered.contains("-h") || argsFiltered.length<2){
        val table = Main.newTable(Array("method","Params","Description"))
        methods.values.toList.sortBy(_.name).map{m=>
          table.addRow(m.name, m.params.mkString("<br>"), m.description)
        }
        println(table.render())
      }
      else{
        // Start processing method request
        val clusterName = argsFiltered(0)
        val methodName = argsFiltered(1)
        val methodParams = argsFiltered.splitAt(2)._2

        // Check if method exists
        val method = methods.get(methodName) match {
          case Some(value) => value
          case None => throw new Throwable(s"method[$methodName] not found. Use -h to see available methods.")
        }

        // Check method parameters
        if(method.params.length != methodParams.length){
          throw new Throwable(s"invalid argument count for method ${method.signature}")
        }
        val mappedParams = method.params.zip(methodParams).toMap

        // Execute the requested method
        method.method.doit(clusterName, mappedParams)
      }
    }catch {
      case e:Throwable =>
        println(s"""${header("Error (use -vv for more info)",bg = Color.RED,fg = Color.WHITE)}
                   |${e.getMessage}
                   |""".stripMargin)
        logger.error(e.getMessage, e)
    }
  }

  /**
   * Log configuration
   * @param verbosity 0: prints println
   *                  1: prints println + logger.com.blyxa[DEBUG]
   *                  2: prints println + logger.com.blyxa[DEBUG] + logger.ROOT[INFO]
   *                  3: prints println + logger.com.blyxa[DEBUG] + logger.ROOT[DEBUG]
   */
  def configLog(verbosity:Int): Unit = {
    val lc: LoggerContext = LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]

    val ple = new PatternLayoutEncoder()
    ple.setPattern(
      if (verbosity>0)
        "%d{yyyy-MM-dd HH:mm:ss} %highlight(%-5level) %cyan(%-30class{30}:%4.-4line) %magenta(%X{akkaSource}) %msg%n"
      else
        "%msg%n"
    )
    ple.setContext(lc)
    ple.start()

    val consoleAppender = new ConsoleAppender[ILoggingEvent]
    consoleAppender.setEncoder(ple)
    consoleAppender.setName("consoleAppender")
    consoleAppender.setContext(lc)
    consoleAppender.start()

    val root = lc.getLogger("ROOT")
    root.detachAndStopAllAppenders()
    root.addAppender(consoleAppender)
    root.setLevel(Level.OFF)
    verbosity match {
      case 0 =>
        root.setLevel(Level.OFF)
      case 1 =>
        root.setLevel(Level.OFF)
        lc.getLogger("com.blyxa").setLevel(Level.DEBUG)
      case 2 =>
        root.setLevel(Level.INFO)
        lc.getLogger("com.blyxa").setLevel(Level.DEBUG)
      case _ =>
        root.setLevel(Level.DEBUG)
    }

  }
  private val methods = mutable.HashMap[String, MethodDef]()
  def addMethod(m:MethodDef): Option[MethodDef] = methods.put(m.name, m)
  private def header(h:String, bg:Color=Color.YELLOW, fg:Color= Color.BLACK):String =
    ansi().reset().bg(bg).fg(fg).a(h.padTo(HEADER_PADDING, ' ')).reset().toString
  def getConfig: StapConfig ={
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

}