package com.weshare.marlin

import java.sql.Date

import com.weshare.marlin.model.report.HiveReport
import com.weshare.marlin.utils.{CommonUtils, MySqlLiteTools}

/**
  * Created by xuyi on 2018/10/18 16:45.
  */
object TestMysql {
  def main(args: Array[String]): Unit = {
    val report = new HiveReport

    report.taskName = "nj_xuyi_test_xxxxxxxxx"
    report.dataTime = "20181018"
    report.startTime = CommonUtils.getCurrentDate
    report.endTime = CommonUtils.getCurrentDate

    println(CommonUtils.getCurrentDate)

    println(CommonUtils.getCurrentDate.getTime)

    report.driverMem = "1G"
    report.executorMem = "1024M"
    report.executorCores = 4
    report.executorNum = 10

    report.dataNum = 129844
    report.appId = "application_1527145420043_343105"
    report.status = "1"
    report.createTime = CommonUtils.getCurrentDate

    report.errorMsg="23dfsfafafjajgiagoanganga"

    MySqlLiteTools.insertHiveReport(report)



    val sqlDate = new Date(CommonUtils.getCurrentDate.getTime)
    println(sqlDate.getTime)

    println(new Date(report.createTime.getTime))
  }
}
