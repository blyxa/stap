package com.blyxa.stap

import ch.qos.logback.classic.{Level, LoggerContext}
import ch.qos.logback.classic.encoder.PatternLayoutEncoder
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.ConsoleAppender
import org.slf4j.LoggerFactory

object LogConfig {

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
}
