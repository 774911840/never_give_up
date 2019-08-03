package com.weshare.marlin.model.report

import com.weshare.marlin.utils.CommonUtils.getCurrentTimestamp

/**
  * 任务执行报告
  *
  * @param tableName    表名
  * @param taskDate     任务日期
  * @param appId        任务执行applicationId
  * @param startTime    任务执行开始时间
  * @param endTime      任务执行结束时间
  * @param taskState    任务执行结果
  * @param dataNum      结果数据行数
  * @param rawDirInfo   原始数据目录信息
  * @param tableDirInfo 结果数据目录信息
  * @param fieldPulse   列信息采集
  */
@SerialVersionUID(2018L)
class TaskReport(val tableName: String,
                 val taskDate: String,
                 val appId: String,
                 val startTime: String,
                 var endTime: String,
                 var taskState: String,
                 var dataNum: Long,
                 var rawDirInfo: Array[FileInfo],
                 var tableDirInfo: Array[FileInfo],
                 var fieldPulse: Array[FieldPulse]) extends Serializable {

  def this(tableName: String, taskDate: String, appId: String) {
    this(tableName,
      taskDate,
      appId,
      getCurrentTimestamp,
      null,
      "FAILURE",
      0l,
      null,
      null,
      null)
  }
}