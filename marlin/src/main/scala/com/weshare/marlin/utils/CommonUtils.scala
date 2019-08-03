package com.weshare.marlin.utils


import java.text.SimpleDateFormat
import java.util.Date
import java.util.regex.Pattern

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.core.{JsonFactory, JsonParser}
import com.fasterxml.jackson.databind.{DeserializationFeature, SerializationFeature}
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.github.nscala_time.time.Imports._
import com.weshare.marlin.constant.CommonConstant.DataType._
import com.weshare.marlin.logging.WeshareLogging
import com.weshare.marlin.model.conf.TaskConf
import com.weshare.marlin.utils.PathUtils.checkPathExists
import com.weshare.marlin.utils.ResourceUtils.RawFile
import org.apache.commons.lang3.StringUtils
import org.joda.time.{Days, DurationFieldType}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, LinkedHashMap}

/**
  * Created by xuyi on 2018/8/31 15:10.
  */
object CommonUtils extends WeshareLogging {

  val DateFormat = "yyyyMMdd"
  val MonthFormat = "yyyyMM"

  def matchType[T](v: T) = v match {
    case null => NULL
    case _: Int => INT
    case _: String => STRING
    case _: java.util.LinkedHashMap[_, _] => MAP
    case _: java.util.ArrayList[_] => LIST
    case _: Double => DOUBLE
    case _: Float => FLOAT
    case _: Long => LONG
    case _: Byte => BYTE
    case _: Short => SHORT
    case _: Boolean => BOOLEAN
    case _: java.util.LinkedList[_] => LINKED_LIST
    case _: BigDecimal => BIG_DECIMAL
    case _: LinkedHashMap[_, _] => LINKED_HASH_MAP
    case _: Array[_] => ARRAY
    case _ => UNKNOWN
  }

  /**
    * 清洗国内手机号码方法，暂不支持国外手机号清洗
    *
    * @param phone
    * @return
    */
  def cleanPhoneCN(phone: String): String = {
    if (StringUtils.isBlank(phone)) {
      return phone
    }

    val phoneNum = phone replaceAll("[^0-9]", "")
    if (StringUtils.isBlank(phoneNum)) {
      return phoneNum
    }

    if (phoneNum.length == 11 && isPhone(phoneNum)) {
      return phoneNum
    }

    if (phoneNum.length == 13 && phoneNum.startsWith("86") && isPhone(phoneNum.substring(2))) {
      return phoneNum.substring(2)
    }

    phoneNum
  }

  private def isPhone(phone: String): Boolean = {
    if (phone matches "^1([358][0-9]|4[56789]|66|7[012345678]|9[89])[0-9]{8}$")
      return true

    false
  }

  /**
    * 获取当前日期的昨天
    *
    * @return yyyyMMdd
    */
  def getYesterday: String = {
    DateTime.now.minusDays(1).toString(DateFormat)
  }

  def getCurrentTimestamp: String = {
    DateTime.now.getMillis.toString
  }

  def dateToStr(d: Date): String = {
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    dateFormat.format(d)
  }

  def strToDate(s: String): Date = {
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    dateFormat.parse(s)
  }

  def getCurrentDate: Date = {
    DateTime.now.toDate
  }

  def listBetweenDays(startDate: String, endDate: String): ArrayBuffer[String] = {
    val dateArray = new ArrayBuffer[String]()

    val start = DateTime.parse(startDate, DateTimeFormat.forPattern(DateFormat))
    val end = DateTime.parse(endDate, DateTimeFormat.forPattern(DateFormat))
    val days = Days.daysBetween(start, end).getDays
    for (i <- 0 to days) {
      val date = start.withFieldAdded(DurationFieldType.days(), i)
      dateArray += date.toString(DateFormat)
    }

    dateArray
  }

  /**
    * 获取两日期间所有的日期
    *
    * @param date 可以是yyyy格式，是获取一年的所有日期，
    *             可以是yyyyMM格式，是获取一个月的所有日期，
    *             可以是yyyyMM,yyyyMM格式，是获取两个固定月的所有日期，
    *             可以是yyyyMMdd,yyyyMMdd，是获取两个固定日期间的所有日期
    * @return 日期列表
    */
  def listBetweenDays(date: String): ArrayBuffer[String] = {
    if (date.length == 4) {
      listBetweenDays(date + "0101", date + "1231")
    } else if (date.length == 6) {
      listBetweenDays(date + "01", DateTime.parse(date, DateTimeFormat.forPattern(MonthFormat)).dayOfMonth().withMaximumValue().toString(DateFormat))
    } else if (date.length == 13 && date.contains(",")) {
      val dateFT = date.split(',')
      listBetweenDays(dateFT(0) + "01", DateTime.parse(dateFT(1), DateTimeFormat.forPattern(MonthFormat)).dayOfMonth().withMaximumValue().toString(DateFormat))
    } else if (date.length == 17 && date.contains(",")) {
      val dateFT = date.split(',')
      listBetweenDays(dateFT(0), dateFT(1))
    } else {
      null
    }
  }

  def argTask(args: Array[String]): (String, Array[String]) = {
    if (args.length == 0) {
      logger.error("The args' length is 0, the task type could not be determined!")

      System.exit(1)
    }

    if (args(0).split('.').length != 2) {
      logger.error("The table's name is error")

      System.exit(1)
    }

    if (args.length > 1 && !Array(4, 6, 8, 13, 17).contains(args(1).length)) {
      logger.error("The task date is error")

      System.exit(1)
    }

    val tableName = args(0)
    val taskDate = if (args.length == 1) getYesterday else args(1)
    if (taskDate.length == 8) {
      (tableName, Array(taskDate))
    } else {
      (tableName, listBetweenDays(taskDate).toArray)
    }
  }

  def argTaskSingle(args: Array[String]): (String, String) = {
    if (args.length == 0) {
      logger.error("The args' length is 0, the task type could not be determined!")

      System.exit(1)
    }

    if (args(0).split('.').length != 2) {
      logger.error("The table's name is error")

      System.exit(1)
    }

    if (args.length > 1 && args(1).length != 8) {
      logger.error("The task date is error")

      System.exit(1)
    }

    val tableName = args(0)
    val taskDate = if (args.length == 1) getYesterday else args(1)

    (tableName, taskDate)
  }

  //  import org.json4s._
  //  import org.json4s.jackson.Serialization
  //  import org.json4s.jackson.Serialization.write
  //
  //  private implicit val formats = Serialization.formats(NoTypeHints)
  //
  //  def objToJson(o: Object): String = {
  //    write(o)
  //  }

  import com.fasterxml.jackson.databind.ObjectMapper
  import com.fasterxml.jackson.module.scala.DefaultScalaModule

  private val factory = new JsonFactory()
  factory.enable(JsonParser.Feature.ALLOW_SINGLE_QUOTES)
  val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.setSerializationInclusion(Include.NON_NULL)
    .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
    .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
    .registerModule(DefaultScalaModule)

  def objToJson(m: Any): String = {
    mapper.writeValueAsString(m)
  }

  def getTaskPath(taskDate: String, path: String): String = {
    if (path.endsWith("/")) {
      path + taskDate
    } else {
      path + '/' + taskDate
    }
  }

  private val upperCasePattern = Pattern.compile("[A-Z]")

  def upperToUnderLine(text: String): String = {
    var r: String = null

    if (StringUtils.isNotBlank(text)) {
      if (text == text.toUpperCase) {
        r = text.toLowerCase
      } else {
        val builder = new mutable.StringBuilder(text)
        var i = 0
        val mc = upperCasePattern.matcher(text)
        while (mc.find()) {
          builder.replace(mc.start + i, mc.end + i, "_" + mc.group().toLowerCase)
          i = i + 1
        }

        val underLine: Character = '_'
        if (underLine == builder.charAt(0)) {
          builder.deleteCharAt(0)
        }

        r = builder.toString.replace("__", "_")
      }
    }

    r
  }

  def getFiledNameByFullPath(fullPath: String): String = {
    upperToUnderLine(pathLastName(fullPath))
  }

  def pathLastName(fullPath: String): String = {
    if (StringUtils.isNotBlank(fullPath) && fullPath.endsWith("']")) {
      fullPath.substring(fullPath.lastIndexOf("['")).replace("['", "").replace("']", "")
    } else {
      null
    }
  }

  /**
    *
    * @param path eg: $['store'][0]['book'][1]['price']
    * @return "_0_1_"
    */
  def getPathIndex(path: String): String = {
    val indexList = new ArrayBuffer[String]()
    for (item <- path.split("\\]\\[")) {
      val key = item.replace("\\$", "").replace("\\[", "").replace("\\]", "")
      if (!key.contains("'")) {
        indexList += key
      } else {
        indexList += "_"
      }
    }

    indexList.mkString
  }

  def getLastKey(path: String): String = {
    if (path.startsWith("$[")) {
      path.split("\\]\\[").last.replace("$", "").replace("[", "").replace("]", "").replace("'", "")
    } else {
      path.split('.').last
    }
  }

  /**
    *
    * @param fullPath
    * @return (blockPath,indexPath)
    */
  def getBlockPath(fullPath: String): (String, String) = {
    val indexList = new ArrayBuffer[String]()
    val keyList = new ArrayBuffer[String]()
    for (item <- fullPath.split("\\]\\[")) {
      val key = item.replace("$", "").replace("[", "").replace("]", "")
      if (!key.contains("'")) {
        indexList += key
        keyList += "^INDEX^"
      } else {
        keyList += key.replace("'", "")
      }
    }

    var (blockPath, indexPath) = ("_", "_")
    val path = keyList.mkString(".")
    if (path.contains("^INDEX^")) {
      blockPath = path.substring(0, path.lastIndexOf("^INDEX^")).replace("^INDEX^", "[*]") + "[*]"
      indexPath = indexList.mkString("_") + "_"
    }

    (blockPath, indexPath)
  }

  /**
    *
    * @param pathMap  (fullPath,(fieldName,value))
    * @param blockMap blockPath,index,(fullPath,fieldName,value)
    */
  def getPathMap(pathMap: mutable.HashMap[String, (String, String)],
                 blockMap: mutable.HashMap[String, mutable.HashMap[String, ArrayBuffer[(String, String, String)]]]): Unit = {
    if (pathMap != null && pathMap.nonEmpty) {
      for (fullPath <- pathMap.keySet) {
        val field = pathMap(fullPath)
        val biPath = getBlockPath(fullPath)
        val blockPath = biPath._1
        val index = biPath._2

        if (blockMap.contains(blockPath)) {
          val indexMap = blockMap(blockPath)

          if (indexMap.contains(index)) {
            indexMap(index) += Tuple3(fullPath, field._1, field._2)
          } else {
            val indexArray = new ArrayBuffer[(String, String, String)]()
            indexArray += Tuple3(fullPath, field._1, field._2)
            indexMap += (index -> indexArray)
          }
        } else {
          val indexMap = mutable.HashMap[String, ArrayBuffer[(String, String, String)]]()
          val indexArray = new ArrayBuffer[(String, String, String)]()
          indexArray += Tuple3(fullPath, field._1, field._2)
          indexMap += (index -> indexArray)

          blockMap += (index -> indexMap)
        }
      }
    }
  }

  def prepareTaskPath(taskDate: String, taskConfs: Array[TaskConf]): Array[RawFile] = {
    var rawFiles = new ArrayBuffer[RawFile]()
    for (conf <- taskConfs) {
      val taskPath = getTaskPath(taskDate, conf.mainConf.filePath)
      if (checkPathExists(taskPath)) {
        rawFiles += RawFile(conf.mainConf.fileType, taskPath)
      }
    }

    rawFiles.toArray
  }

  def getClassPathFile(filename:String):String= System.getProperty("user.dir") + "/src/main/resources/" + filename
}