package com.weshare.marlin

import com.hery.bigdata.conf.MarlinProperty

/**
  *
  * @author heng
  */
object Demo {
  def main(args: Array[String]): Unit = {
    val pro = new MarlinProperty()
    val env = pro.getString("project.environment")
    println(env)
  }
}
