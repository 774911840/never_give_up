package com.weshare.marlin.model.report

/**
  * Created by xuyi on 2018/8/30 15:07.
  */
@SerialVersionUID(2018L)
class FileInfo(val fullPath: String,
               val isDirectory: Boolean,
               val size: Long,
               val modificationTime: Long,
               val accessTime: Long) extends Serializable {

}
