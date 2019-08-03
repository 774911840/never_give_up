package com.weshare.marlin

import com.weshare.marlin.logging.WeshareLogging
import com.weshare.marlin.model.conf.{FieldConf, MainConf, TaskConf}
import com.weshare.marlin.utils.CommonUtils

/**
  * Created by xuyi on 2018/9/19 16:22.
  */
object TaskInit extends WeshareLogging {
//
//  def testTask: Array[TaskConf] = {
//    val mainConf = new MainConf("gz", "/apps/logs/raw/ocean", true)
//
//    val fieldConfs = Array(
//      new FieldConf("ext_sub_channel", "$.ext_sub_channel", "", false, true),
//      new FieldConf("fee", "$.fee", "", false, true),
//      new FieldConf("idcard_no", "$.idCardNO", "idcard", true, true),
//      new FieldConf("ugid", "$.userGid", "", false, true),
//      new FieldConf("terminal", "$.terminal", "", false, true)
//    )
//
//    val taskConf = new TaskConf(mainConf, Array(Map("$.m_name" -> "VCL022")), fieldConfs, null)
//    Array[TaskConf](taskConf)
//  }
//
//  def main(args: Array[String]): Unit = {
//    logger.info(s"${CommonUtils.objToJson(testTask(0))}")
//
//    val jsonStr = CommonUtils.objToJson(testTask(0))
//
//
//    val a = CommonUtils.mapper.readValue[TaskConf](jsonStr)
//  }
}
