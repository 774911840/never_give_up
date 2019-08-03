package com.weshare.marlin.utils

import java.security.MessageDigest
import java.util.Base64

import org.apache.commons.lang3.StringUtils


/**
  * Created by xuyi on 2018/9/3 10:10.
  */
object MaskTools {

  def bigMD5(text: String): String = {
    if (StringUtils.isBlank(text)) {
      return text
    }

    myMD5.bigMD5(text)
  }

  val bgAESkey = "1:885cef4e3f9b436a9674326420d5afc7"

  def bigAES(text: String): String = {
    if (StringUtils.isBlank(text)) {
      return text
    }

    val aesHelper = new ZZAESHelper(bgAESkey)
    aesHelper.encrypt(text)
  }

  private object myMD5 {
    private val salt_default = "QUBl3T2RWWWtEd2kXUSVmbapFUNtGckJUaJ5kSztmZNJXch1GNzZVV"
    private val salt_prod = "L8iGtvuxOTqjPUzWziJDg2IwJfXDLeontfMffpPI"
    private val salt = new String(Base64.getDecoder.decode(salt_default.reverse + "=="), "UTF-8")

    def bigMD5(text: String): String = {
      val partText = nativeMD5(text.reverse.concat(salt.reverse).concat(salt_prod))
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
