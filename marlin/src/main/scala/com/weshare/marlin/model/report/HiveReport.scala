package com.weshare.marlin.model.report

import java.util.Date

import com.weshare.marlin.constant.CommonConstant.ReportStatus.SUCCESS
import com.weshare.marlin.utils.CommonUtils.getCurrentDate

/**
  * hive执行报告
  * Created by xuyi on 2018/10/18 10:04.
  */
@SerialVersionUID(2018L)
class HiveReport {
  var taskName: String = null
  var dataTime: String = null
  var startTime: Date = getCurrentDate
  var endTime: Date = getCurrentDate
  var driverMem: String = "default"
  var executorMem: String = null
  var executorCores: Integer = 0
  var executorNum: Integer = 0
  var dataNum: Integer = -1
  var appId: String = null
  var status: String = SUCCESS
  var createTime: Date = getCurrentDate
  var errorCode: String = null
  var errorMsg: String = null
}
