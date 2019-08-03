package com.weshare.marlin.constant

/**
  * Created by xuyi on 2018/8/31 15:13.
  */
object CommonConstant {

  val DEFAULT_FS = "hdfs://qxfdata"

  object DataType {
    val NULL = "null"
    val INT = "Int"
    val STRING = "String"
    val MAP = "Map"
    val LIST = "List"
    val DOUBLE = "Double"
    val FLOAT = "Float"
    val LONG = "Long"
    val BYTE = "Byte"
    val BOOLEAN = "Boolean"
    val LINKED_LIST = "LinkedList"
    val BIG_DECIMAL = "BigDecimal"
    val LINKED_HASH_MAP = "LinkedHashMap"
    val UNKNOWN = "Unknown"
    val ARRAY = "Array"
    val SHORT = "Short"
  }

  object MaskContentType {
    val PHONE = "phone"
    val PHONE_CN = "phone_cn"
    val IDCARD = "idcard"
    val EMAIL = "email"
    val BANK_CARD = "bankcard"
    val REAL_NAME = "real_name"
  }

  object Props {
    val aesProps: Map[String, String] = Map(
      BizMask.SDJK_AES -> "aes.key.sdjk",
      BizMask.XJSD_AES -> "aes.key.xjsd",
      BizMask.BIGDATA_AES -> "aes.key.bigdata"
    )

    val aesTypeProps: Map[String, String] = Map(
      BizMask.SDJK_AES -> "aes.type.sdjk",
      BizMask.XJSD_AES -> "aes.type.xjsd",
      BizMask.BIGDATA_AES -> "aes.type.bigdata"
    )

    val md5Props: Map[String, String] = Map(
      BizMask.BIGDATA_MD5 -> "md5.salt.bigdata"
    )
  }

  object BizMask {
    val CLEAR = "clear"
    val SDJK_AES = "sdjk_aes"
    val XJSD_AES = "xjsd_aes"
    val BIGDATA_AES = "bg_aes"
    val BIGDATA_MD5 = "bg_md5"
  }

  object ReportStatus {
    val UN_RUN = "0"
    val FAILED = "1"
    val SUCCESS = "2"
  }

  object ReportErrorCode {
    val UNKNOWN_ERROR = "0"
    val FILE_EMPTY = "1"
    val CONF_EMPTY = "2"
    val RESOURCE_ERROR = "3"
    val WRITE_PATH_BLANK = "4"
  }

}
