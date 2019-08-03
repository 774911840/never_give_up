package com.weshare.marlin

import java.security.MessageDigest
import java.util.Base64

import com.weshare.marlin.logging.WeshareLogging
import com.weshare.marlin.model.conf.TaskConf
import com.weshare.marlin.model.conf2.TaskConf2
import com.weshare.marlin.utils.{CommonUtils, MaskTools, ResourceUtils}
import org.apache.commons.lang3.StringUtils

/**
  * Created by xuyi on 2018/8/29 20:54.
  */
object TestScala extends WeshareLogging {
  def main(args: Array[String]): Unit = {
    val a = 1 / 2
    logger.info(s"$a")
  }

  private object myMD5 {
    private val salt_default = "QUBl3T2RWWWtEd2kXUSVmbapFUNtGckJUaJ5kSztmZNJXch1GNzZVV"
    private val salt_prod = "L8iGtvuxOTqjPUzWziJDg2IwJfXDLeontfMffpPI"
    private val salt = new String(Base64.getDecoder.decode(salt_default.reverse + "=="), "UTF-8")

    def bigMD5(text: String): String = {
      val partText = nativeMD5(text.reverse.concat(salt.reverse).concat(salt_prod))

      println(partText)

      nativeMD5(text.concat(partText).concat(salt).concat(salt_prod))
    }

    private def nativeMD5(text: String): String = {
      val md = MessageDigest.getInstance("MD5")
      md.digest(text.getBytes("UTF-8"))
        .map("%02x".format(_))
        .mkString
    }
  }
}