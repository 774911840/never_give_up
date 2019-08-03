package com.weshare.marlin.utils

/**
  * Created by xuyi on 2018/9/1 14:23.
  */
object NumberStrTools {

  def numValueToStr(key: String, jsonStr: String): String = {
    val fullKey = "\"" + key + "\"" + ":"
    val r = s"\\Q$fullKey\\E".r

    val keyEndIndexs = r.findAllMatchIn(jsonStr).map(_.end).toArray
    val strSplited = splitByIndexs(keyEndIndexs, jsonStr)

    var resStr = strSplited(0)
    for (i <- 1 until strSplited.length) {
      val subStr = strSplited(i)
      if (subStr.startsWith("\"")) {
        resStr += subStr
      } else {
        resStr += numToStr(firstSymbolIndex(subStr), subStr)
      }
    }

    resStr
  }

  private def splitByIndexs(pos: Array[Int], str: String): List[String] = {
    val (rest, result) = pos.foldRight((str, List[String]())) {
      case (curr, (s, res)) =>
        val (rest, split) = s.splitAt(curr)
        (rest, split :: res)
    }
    rest :: result
  }

  private def firstSymbolIndex(str: String): Int = {
    val i1 = str.indexOf(',')
    val i2 = str.indexOf('}')

    if (i1 > i2) i2 else i1
  }

  private def numToStr(i: Int, str: String): String = {
    "\"" + splitByIndexs(Array(i), str).mkString("\"")
  }

}
