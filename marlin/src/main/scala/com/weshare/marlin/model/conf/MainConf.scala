package com.weshare.marlin.model.conf

/**
  *
  * @param fileType 文件类型，eg：gz,parquet...
  * @param filePath hdfs文件路径，eg：...
  * @param unique   数据关系，eg：1:n or 1:1
  */
@SerialVersionUID(2018L)
class MainConf(val fileType: String,
               val filePath: String,
               val unique: Boolean,
               val pack: Boolean = false) extends Serializable {

}
