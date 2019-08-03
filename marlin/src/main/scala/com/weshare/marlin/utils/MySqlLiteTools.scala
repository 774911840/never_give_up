package com.weshare.marlin.utils

import java.sql.{Connection, Date, DriverManager, Timestamp}

import com.weshare.marlin.logging.WeshareLogging
import com.weshare.marlin.model.conf.TaskConf
import com.weshare.marlin.model.conf2.TaskConf2
import com.weshare.marlin.model.report.HiveReport
import com.weshare.marlin.utils.CommonUtils.{dateToStr, strToDate}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by xuyi on 2018/9/14 14:24.
  */
object MySqlLiteTools extends WeshareLogging {

  private def getKunlunConn: Connection = {
    //  val (url, userName, passward) = ("jdbc:mysql://10.15.1.230:3306/kunlun?autoReconnect=true&useUnicode=true&characterEncoding=utf8&characterSetResults=utf8&serverTimezone=Asia/Shanghai", "root", "root")
    val (url, userName, passward) = ("jdbc:mysql://172.17.51.111:3306/kunlun?autoReconnect=true&useUnicode=true&characterEncoding=utf8&characterSetResults=utf8&useSSL=true&serverTimezone=Asia/Shanghai", "kunlun_rw", "YIf5QH1glUQ6tqOA")
    Class.forName("com.mysql.cj.jdbc.Driver")
    DriverManager.getConnection(url, userName, passward)
  }

  private def getEelConn: Connection = {
    //      val (url, userName, passward) = ("jdbc:mysql://10.15.1.230:3306/eel?autoReconnect=true&useUnicode=true&characterEncoding=utf8&characterSetResults=utf8&serverTimezone=Asia/Shanghai", "root", "root")
    val (url, userName, passward) = ("jdbc:mysql://172.17.51.111:3306/eel?autoReconnect=true&useUnicode=true&characterEncoding=utf8&characterSetResults=utf8&useSSL=true&serverTimezone=Asia/Shanghai", "eel_rw", "jwP4c8CJDfHJLlqx")
    Class.forName("com.mysql.cj.jdbc.Driver")
    DriverManager.getConnection(url, userName, passward)
  }

  /**
    * 根据表名去bg_data_hive_config_v2进行查询  status=1(1-待建表)
    *
    * @param tableName
    * @param taskDate
    * @return
    */
  private def getTaskSample(tableName: String, taskDate: String): Array[String] = {
    logger.warn(s"getTaskSample | entry | tableName=$tableName,taskDate=$taskDate")

    val conn = getKunlunConn
    val sql =
      s"""
         |SELECT * FROM bg_data_hive_config_v2
         |WHERE table_name='$tableName'
         |AND status=1
      """.stripMargin

    val samples = new ArrayBuffer[String]()
    try {
      val statement = conn.createStatement()
      val resultSet = statement.executeQuery(sql)
      val taskDateInt = taskDate.toInt
      while (resultSet.next()) {
        val sd = resultSet.getDate("start_date")
        val ed = resultSet.getDate("end_date")

        if (sd == null && ed == null) {
          samples += resultSet.getString("sample")
        } else if (sd == null) {
          val endDate = dateToStr(ed).toInt
          if (taskDateInt <= endDate) {
            samples += resultSet.getString("sample")
          }
        } else if (ed == null) {
          val startDate = dateToStr(sd).toInt
          if (taskDateInt >= startDate) {
            samples += resultSet.getString("sample")
          }
        } else {
          val startDate = dateToStr(sd).toInt
          val endDate = dateToStr(ed).toInt

          if (taskDateInt >= startDate && taskDateInt <= endDate) {
            samples += resultSet.getString("sample")
          }
        }
      }
    } finally {
      conn.close()
    }

    samples.toArray
  }

  private def getTaskConf(tableName: String, taskDate: String): Array[TaskConf] = {
    logger.warn(s"Get task conf! tableName=$tableName,taskDate=$taskDate")

    val samples = getTaskSample(tableName, taskDate)

    val taskConfArray = new ArrayBuffer[TaskConf]()
    if (samples.nonEmpty) {
      for (json <- samples) {
        logger.warn(json)
        if (json.contains("packList") || json.contains("maskList")) {
          throw new RuntimeException("conf version error")
        }

        taskConfArray += CommonUtils.mapper.readValue[TaskConf](json)
      }
    }

    taskConfArray.toArray
  }

  private def getTaskConf2(tableName: String, taskDate: String): Array[TaskConf2] = {
    logger.warn(s"Get task conf! tableName=$tableName,taskDate=$taskDate")

    val samples = getTaskSample(tableName, taskDate)

    val taskConfArray = new ArrayBuffer[TaskConf2]()
    if (samples.nonEmpty) {
      for (json <- samples) {
        logger.warn(json)
        taskConfArray += CommonUtils.mapper.readValue[TaskConf2](json)
      }
    }

    taskConfArray.toArray
  }

  def queryTaskConf(tableName: String, taskDate: String): (Array[TaskConf], Array[TaskConf2]) = {
    var taskConfArray: Array[TaskConf] = null
    try {
      taskConfArray = getTaskConf(tableName, taskDate)
    } catch {
      case ex: Exception => {
        //            logger.error(s"Failed to format conf json! tableName=$tableName,taskDate=$taskDate", ex)
        logger.warn(s"Failed to format conf json! tableName=$tableName,taskDate=$taskDate")
      }
    }
    var taskConf2Array: Array[TaskConf2] = null
    try {
      taskConf2Array = getTaskConf2(tableName, taskDate)
    } catch {
      case ex: Exception => {
        //        logger.error(s"Failed to format conf2 json! tableName=$tableName,taskDate=$taskDate", ex)
        logger.warn(s"Failed to format conf2 json! tableName=$tableName,taskDate=$taskDate")
      }
    }
    (taskConfArray, taskConf2Array)

  }

  def insertReport(taskDate: String, report: String): Unit = {
    logger.warn(s"Insert report! taskDate=$taskDate,report=$report")

    val conn = getKunlunConn
    try {
      val sql =
        """
          |INSERT INTO bg_data_receive(type,report_data,report_date)
          |VALUES(?, ?, ?)
        """.stripMargin

      val pstm = conn.prepareStatement(sql)

      pstm.setString(1, "HIVE_ODS")
      pstm.setString(2, report)
      pstm.setDate(3, new Date(strToDate(taskDate).getTime))

      if (pstm.executeUpdate() <= 0) {
        logger.error(s"Failed to insert report! taskDate=$taskDate,report=$report")
      }
    } finally {
      conn.close()
    }
  }

  /**
    * 将相关信息插入到eel库中
    * @param report
    */
  def insertHiveReport(report: HiveReport): Unit = {
    logger.warn(s"insert report info to eel ,table:bg_hive_report  | ${CommonUtils.objToJson(report)}")

    val conn = getEelConn
    try {
      val sql =
        """
          |INSERT INTO bg_hive_report
          |(task_name,data_time,start_time,end_time,driver_mem,executor_mem,executor_cores,executor_num,data_num,app_id,status,create_time,error_msg,error_code)
          |VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """.stripMargin

      val pstm = conn.prepareStatement(sql)

      pstm.setString(1, report.taskName)
      pstm.setString(2, report.dataTime)
      pstm.setTimestamp(3, new Timestamp(report.startTime.getTime))
      pstm.setTimestamp(4, new Timestamp(report.endTime.getTime))

      pstm.setString(5, report.driverMem)
      pstm.setString(6, report.executorMem)
      pstm.setInt(7, report.executorCores)
      pstm.setInt(8, report.executorNum)

      pstm.setInt(9, report.dataNum)
      pstm.setString(10, report.appId)
      pstm.setString(11, report.status)
      pstm.setTimestamp(12, new Timestamp(report.createTime.getTime))
      pstm.setString(13, report.errorMsg)
      pstm.setString(14, report.errorCode)

      if (pstm.executeUpdate() <= 0) {
        logger.error(s"Failed to insert bg_hive_report ! ${CommonUtils.objToJson(report)}")
      }
    } finally {
      conn.close()
    }
  }


}
