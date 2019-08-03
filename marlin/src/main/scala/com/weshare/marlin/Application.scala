package com.weshare.marlin

import com.hery.bigdata.conf.MarlinProperty
import com.weshare.marlin.MarlinDef.{getSchema, readFile}
import com.weshare.marlin.auto.AutoFilter.dataFilter
import com.weshare.marlin.auto.{AutoMain, AutoMainV2}
import com.weshare.marlin.constant.CommonConstant.ReportErrorCode._
import com.weshare.marlin.constant.CommonConstant.ReportStatus._
import com.weshare.marlin.constant.{CommonConstant, CustomException}
import com.weshare.marlin.logging.WeshareLogging
import com.weshare.marlin.model.conf.TaskConf
import com.weshare.marlin.model.conf2.TaskConf2
import com.weshare.marlin.model.report.HiveReport
import com.weshare.marlin.utils.CommonUtils
import com.weshare.marlin.utils.CommonUtils._
import com.weshare.marlin.utils.MySqlLiteTools.{insertHiveReport, queryTaskConf}
import com.weshare.marlin.utils.PathUtils.getWritePath
import com.weshare.marlin.utils.ResourceUtils.{RawFile, Resource, getGzResource}
import com.weshare.marlin.utils.TableUtils.msckTable
import com.weshare.marlin.utils.WriteUtils.{delFile, generateTempPath, writeParquet, writeTag}
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.spark_project.jetty.util.component.AbstractLifeCycle

import scala.collection.mutable.ArrayBuffer
import scala.runtime.ScalaRunTime.stringOf

/**
  * Created by xuyi on 2018/8/29 20:46.
  */
object Application extends WeshareLogging {
  // 传参 第一个参数是kunlun 表名
  // 第二个参数是时间 可选填
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    Logger.getLogger(classOf[AbstractLifeCycle]).setLevel(Level.ERROR)

    logger.warn(s"receive the params! args=${stringOf(args)}")
    //检查传参是否正确
    val (tableName, taskDate) = argTaskSingle(args)

    val hiveReport = new HiveReport
    hiveReport.taskName = tableName
    hiveReport.dataTime = taskDate
    hiveReport.startTime = getCurrentDate
    //获取kunlun 对应表属性
    val tca = queryTaskConf(tableName, taskDate)
    // 生成临时目录/user/koala/temp/uuid
    val splitTempPath = generateTempPath

    logger.warn(s"Split temp path: $splitTempPath")

    var exitStatus = 0
    try {
      if ((tca._1 == null || tca._1.isEmpty)
        && (tca._2 == null || tca._2.isEmpty)) {
        logger.error(s"Can not get any conf! tableName=$tableName,taskDate=$taskDate")
        throw CustomException(CONF_EMPTY)
      }

      val isV1Conf = isV1(tca._1) // 判断是否为taskconf1 类型

      // 获取文件类型和存放路径
      val files = rawFiles(taskDate, tca._1, tca._2)
      //获取GZ文件信息 需要本地有原始文件夹并且文件不为空
      val resource = getGzResource(files)

      logger.warn(s"spark resource | ${objToJson(resource)}")
      if (resource != null) {
        val spark = initSpark(taskDate, tableName, resource)
        val appId = spark.sparkContext.applicationId

        logger.warn(s"Application report for $appId (state: ACCEPTED)")

        hiveReport.appId = appId
        hiveReport.driverMem = "default"
        hiveReport.executorMem = resource.execMem
        hiveReport.executorCores = resource.execCores.toInt
        hiveReport.executorNum = resource.execNum.toInt

        // 获取表tableName的存储路径
        val writePath = getWritePath(tableName, taskDate, spark)
        if (StringUtils.isBlank(writePath)) {
          logger.error(s"The task's write path is blank! table=$tableName,taskDate=$taskDate")
          throw CustomException(WRITE_PATH_BLANK)
        }
        // 根据数据库中的配置，组合schema信息
        val schemas = getSchema(tca._1, tca._2)
        // 获取过滤信息 数据库表中的：filterConf
        val filterArray = initFilters(tca._1, tca._2)

        import spark.implicits._

        // 读取数据
        val lineRDD = if (filterArray == null || filterArray.isEmpty) {
          //readFile(files, spark).repartition(resource.partNum)
          readFile(files, spark) //TODO   测试用
        } else {
          //readFile(files, spark).repartition(resource.partNum)
          readFile(files, spark).filter(r => lineFilter(r, filterArray)) //TODO 测试用
        }
        //        logger.warn("lineRDD ........")
        //        lineRDD.collect().foreach(println)

        if (!lineRDD.isEmpty()) {
          // 把读取到的信息写入到临时文件中，
          lineRDD.toDF().write.json(splitTempPath)
          // 读取json数据，封装成spark的DataSet
          var tmpDS = spark.read.json(splitTempPath)
          var dataRDD = tmpDS
            .select("value").rdd
            .map(_.getString(0).replaceAll("(\t|\r|\n)", " "))

          // 输出数据
          logger.warn("dataRDD ........")
//          dataRDD.collect().foreach(println)

          if (filterArray != null && !filterArray.isEmpty) {
            dataRDD = dataRDD.filter(r => dataFilter(r, filterArray))
          }
          // 进行脱敏处理
          val fieldRDD = if (isV1Conf) {
            dataRDD.flatMap(r => AutoMain.process(r, schemas.fieldNames, tca._1))
          } else {
            dataRDD.flatMap(r => AutoMainV2.process(r, schemas.fieldNames, tca._2))
          }
          // 创建DateFrame
          val df = spark.createDataFrame(fieldRDD, schemas)

          //df.show(100)
          if (!df.take(1).isEmpty) {
            logger.warn("The df is not empty!")
            // 写入hive表
            writeParquet(writePath, df, spark)
            //修复hive分区
            msckTable(tableName, spark)
          }

          spark.stop()

          hiveReport.endTime = getCurrentDate
          hiveReport.status = SUCCESS
          hiveReport.createTime = getCurrentDate
        }
      }
    } catch {
      case ex: CustomException => {
        hiveReport.errorCode = ex.message

        logger.error(s"The spark run error! args=${stringOf(args)}", ex)
      }
      case ex: Exception => {
        hiveReport.errorCode = UNKNOWN_ERROR

        logger.error(s"The spark run error! args=${stringOf(args)}", ex)
      }

        hiveReport.endTime = getCurrentDate
        hiveReport.status = FAILED
        hiveReport.createTime = getCurrentDate
        hiveReport.errorMsg = ExceptionUtils.getStackTrace(ex)

        exitStatus = 1
    }
    //执行成功写入tag数据
    if (SUCCESS.equals(hiveReport.status)) {
      writeTag(taskDate, tableName)
    }
    // 删除临时路径
    delFile(splitTempPath)
    // 向eel报告相关数据
    insertHiveReport(hiveReport)

    if (exitStatus != 0) {
      System.exit(exitStatus)
    }
  }

  import scala.util.control.Breaks._

  /**
    * 对数据进行过滤，过滤条件是从数据库中查找到的filterConf
    *
    * @param line
    * @param filterArray
    * @return
    */
  private def lineFilter(line: String, filterArray: Array[Map[String, String]]): Boolean = {
    var f = false

    breakable {
      filterArray.foreach { filterMap =>
        var sf = true
        filterMap.values.foreach { value =>
          if (!line.contains(value)) {
            sf = false
          }
        }

        if (sf) {
          f = true
          break
        }
      }
    }

    f
  }

  /**
    * 初始化spark
    *
    * @param taskDate
    * @param tableName
    * @param resource
    * @return
    */
  private def initSpark(taskDate: String, tableName: String, resource: Resource): SparkSession = {

    val sparkConf = new SparkConf()
      .setAppName(s"marlin_${taskDate}_$tableName")
      //        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //        .set("spark.kryo.registrationRequired", "true")
      .set("spark.yarn.principal", "koala@QXFDATA.COM")

    val pro = new MarlinProperty()
    val env = pro.getString("project.environment")
    if ("local".equals(env)) {
      // 本地测试环境
      sparkConf.setMaster("local")
        .set("spark.network.timeout", "300")
        .set("spark.task.maxFailures", "8")
        .set("spark.files", Application.getClass.getResource("/hive-site.xml").getPath)
        .set("spark.yarn.keytab",Application.getClass.getResource("/koala.keytab" ).getPath)

    } else {
      sparkConf
        .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:NewRatio=8")
        .set("spark.executor.memory", resource.execMem)
        .set("spark.executor.cores", resource.execCores)
        .set("spark.executor.instances", resource.execNum)
        .set("spark.files", "/usr/hdp/2.6.0.3-8/spark/conf/hive-site.xml")
        .set("spark.yarn.keytab", "/data/config/spark/koala.keytab")
    }

    val spark = SparkSession.builder()
      .config(sparkConf)
      .config("spark.debug.maxToStringFields", "1000")
      .enableHiveSupport().getOrCreate()

    logger.info("spark init end ..")
    spark
  }

  private def isV1(taskConfArray: Array[TaskConf]): Boolean = {
    if (taskConfArray == null || taskConfArray.isEmpty) {
      return false
    }
    true
  }

  /**
    *
    * @param taskDate       任务日期
    * @param taskConfArray  taskConf
    * @param taskConf2Array taskConf2
    * @return RawFile 数组
    */
  private def rawFiles(taskDate: String, taskConfArray: Array[TaskConf], taskConf2Array: Array[TaskConf2]): Array[RawFile] = {
    var rawFiles: Array[RawFile] = null
    if (taskConfArray != null && taskConfArray.nonEmpty) {
      rawFiles = for (c <- taskConfArray)
        yield RawFile(c.mainConf.fileType, getTaskPath(taskDate, c.mainConf.filePath))
    } else {
      rawFiles = for (c <- taskConf2Array)
        yield RawFile(c.mainConf.fileType, getTaskPath(taskDate, c.mainConf.filePath))
    }

    rawFiles
  }

  private def initFilters(taskConfArray: Array[TaskConf], taskConf2Array: Array[TaskConf2]): Array[Map[String, String]] = {
    var filterArray = new ArrayBuffer[Map[String, String]]()

    if (taskConfArray != null && taskConfArray.nonEmpty) {
      taskConfArray.map(_.filterConf).foreach { filterConf =>
        if (filterConf != null && filterConf.nonEmpty) {
          filterArray = filterArray ++ filterConf
        }
      }
    } else {
      taskConf2Array.map(_.filterConf).foreach { filterConf =>
        if (filterConf != null && filterConf.nonEmpty) {
          filterArray = filterArray ++ filterConf
        }
      }
    }

    filterArray.toArray
  }
}

