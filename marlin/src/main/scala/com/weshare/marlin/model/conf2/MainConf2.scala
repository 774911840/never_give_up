package com.weshare.marlin.model.conf2

/**
  *
  * @param fileType 文件类型，eg：gz,parquet...
  * @param filePath hdfs文件路径，eg：...
  */
@SerialVersionUID(2018L)
class MainConf2(val fileType: String,
                val filePath: String) extends Serializable {

}
