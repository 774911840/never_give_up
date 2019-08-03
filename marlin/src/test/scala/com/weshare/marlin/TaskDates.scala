package com.weshare.marlin

import com.weshare.marlin.utils.CommonUtils

import scala.collection.mutable.ArrayBuffer

/**
  * Created by xuyi on 2018/11/8 10:53.
  */
object TaskDates {
  val finalDate =""

  def main(args: Array[String]): Unit = {
    val fd = finalDate.split(",")
    val nd = new ArrayBuffer[String]()
    CommonUtils.listBetweenDays("20180717", CommonUtils.getYesterday).foreach { d =>
//    CommonUtils.listBetweenDays("20160915", "20190121").foreach { d =>
      if (!fd.contains(d)) {
        nd += d
      }
    }

    println(nd.reverse.mkString(","))

  }
}
