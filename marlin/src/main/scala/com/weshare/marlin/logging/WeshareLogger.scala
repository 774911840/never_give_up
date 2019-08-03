package com.weshare.marlin.logging

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.lang3.{RandomStringUtils, StringUtils}

/**
  * Created by xuyi on 2018/8/29 20:44.
  */
object WeshareLogger {
  def apply(): WeshareLogger = new WeshareLogger()
}

@SerialVersionUID(538248225L)
final class WeshareLogger extends Serializable with LazyLogging {
  // Error

  def error(message: String): Unit = formateLogger("ERROR", message)

  def error(message: String, cause: Throwable): Unit = formateLogger("ERROR", message, cause)

  def error(message: String, args: Any*): Unit = formateLogger("ERROR", message, args)


  // Warn

  def warn(message: String): Unit = formateLogger("WARN", message)

  def warn(message: String, cause: Throwable): Unit = formateLogger("WARN", message, cause)

  def warn(message: String, args: Any*): Unit = formateLogger("WARN", message, args)


  // Info

  def info(message: String): Unit = formateLogger("INFO", message)

  def info(message: String, cause: Throwable): Unit = formateLogger("INFO", message, cause)

  def info(message: String, args: Any*): Unit = formateLogger("INFO", message, args)


  // Debug

  def debug(message: String): Unit = formateLogger("DEBUG", message)

  def debug(message: String, cause: Throwable): Unit = formateLogger("DEBUG", message, cause)

  def debug(message: String, args: Any*): Unit = formateLogger("DEBUG", message, args)


  // Trace

  def trace(message: String): Unit = formateLogger("TRACE", message)

  def trace(message: String, cause: Throwable): Unit = formateLogger("TRACE", message, cause)

  def trace(message: String, args: Any*): Unit = formateLogger("TRACE", message, args)

  private def formateLogger(level: String, message: String): Unit = {
//    val fs = getFormatStr(message)
    level match {
      case "ERROR" =>
//        logger.error(fs)
        logger.error(message)
//        logger.error(fs)
      case "WARN" =>
//        logger.warn(fs)
        logger.warn(message)
//        logger.warn(fs)
      case "INFO" =>
//        logger.info(fs)
        logger.info(message)
//        logger.info(fs)
      case "DEBUG" =>
//        logger.debug(fs)
        logger.debug(message)
//        logger.debug(fs)
      case "TRACE" =>
//        logger.trace(fs)
        logger.trace(message)
//        logger.trace(fs)
    }
  }

  private def formateLogger(level: String, message: String, cause: Throwable): Unit = {
//    val fs = getFormatStr(message)
    level match {
      case "ERROR" =>
//        logger.error(fs)
        logger.error(message, cause)
//        logger.error(fs)
      case "WARN" =>
//        logger.warn(fs)
        logger.warn(message, cause)
//        logger.warn(fs)
      case "INFO" =>
//        logger.info(fs)
        logger.info(message, cause)
//        logger.info(fs)
      case "DEBUG" =>
//        logger.debug(fs)
        logger.debug(message, cause)
//        logger.debug(fs)
      case "TRACE" =>
//        logger.trace(fs)
        logger.trace(message, cause)
//        logger.trace(fs)
    }
  }

  private def formateLogger(level: String, message: String, args: Any*): Unit = {
//    val fs = getFormatStr(message)
    level match {
      case "ERROR" =>
//        logger.error(fs)
        logger.error(message, args)
//        logger.error(fs)
      case "WARN" =>
//        logger.warn(fs)
        logger.warn(message, args)
//        logger.warn(fs)
      case "INFO" =>
//        logger.info(fs)
        logger.info(message, args)
//        logger.info(fs)
      case "DEBUG" =>
//        logger.debug(fs)
        logger.debug(message, args)
//        logger.debug(fs)
      case "TRACE" =>
//        logger.trace(fs)
        logger.trace(message, args)
//        logger.trace(fs)
    }
  }

  private def getFormatStr(message: String): String = {
    var num = 10
    if (StringUtils.isNotBlank(message)) {

      num = message.length
    }

    RandomStringUtils.random(num, "#")
  }

  private def strLength(message: String): Int = {
    var v = 0
    val chinese = "[\u0391-\uFFE5]"
    for (i <- 0 until message.length) {
      val temp = message.substring(i, i + 1)
      if (temp.matches(chinese)) v += 2 else v += 1
    }

    v
  }

  private def length(message: String): Int = {
    var v = 0
    for (i <- 0 until message.length) {
      val ascii = Character.codePointAt(message, i)
      if (ascii >= 0 && ascii <= 255) v += 1 else v += 2
    }
    v
  }
}