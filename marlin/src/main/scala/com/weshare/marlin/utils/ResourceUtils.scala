package com.weshare.marlin.utils

import com.weshare.marlin.logging.WeshareLogging
import com.weshare.marlin.model.report.FileInfo
import com.weshare.marlin.utils.PathUtils.listDirInfo

import scala.collection.mutable.ArrayBuffer

/**
  * Created by xuyi on 2018/9/27 17:33.
  */
object ResourceUtils extends WeshareLogging {

  case class RawFile(fileType: String, filePath: String)

  def getResource(rawFiles: Array[RawFile]): Resource = {
    try {
      val dirSizes = for (dir <- rawFiles) yield dirFileSize(dir.filePath)
      val fileNums = for (s <- dirSizes) yield s.length
      val fileSizes = for (s <- dirSizes) yield s.max

      return gzResource(fileNums.max, fileSizes.max)
    } catch {
      case ex: Exception =>
        logger.error(s"The spark run error! args=${CommonUtils.objToJson(rawFiles)}", ex)
    }

    null
  }

  def getResource(fileType: String, dirPath: String): Resource = {
    val sizeArray = dirFileSize(dirPath)

    fileType match {
      case "gz" => gzResource(sizeArray.length, sizeArray.max)
      case _ => null
    }
  }

  private def dirFileSize(dirPath: String): Array[Int] = {
    val sizeArray = new ArrayBuffer[Int]()

    val files: Array[FileInfo] = listDirInfo(dirPath)
    for (f <- files) {
      if (!f.isDirectory) {
        sizeArray += fileSize(f.size)
      }
    }

    sizeArray.toArray
  }

  /**
    * 获取文件路径下的文件总大小
    * @param dirPath 文件路径 String
    * @return 文件总大小 Long
    */
  private def dirFileSize_2(dirPath: String): Array[Long] = {
    val sizeArray = new ArrayBuffer[Long]()
    //获取读取文件的基本信息
    val files: Array[FileInfo] = listDirInfo(dirPath)
    for (f <- files) {
      if (!f.isDirectory) {
        sizeArray += f.size
      }
    }

    sizeArray.toArray
  }

  private def fileSize(size: Long): Int = {
    (size / (1024 * 1024)).toInt
  }

  case class Resource(execMem: String, execCores: String, execNum: String, partNum: Int)

  private def gzResource(fileNum: Int, maxVal: Int): Resource = {
    val cf = sswr(maxVal, 64)

    logger.warn(s"FileNum=$fileNum,fileMaxSize =$maxVal,memblock=$cf")

    val oneMem = cf * 512
    val exeMemMin: Int = 512

    if (fileNum <= 4) {
      val exeMem = if (oneMem * fileNum > exeMemMin) oneMem * fileNum else exeMemMin
      Resource(exeMem.toString + "m", fileNum.toString, "1", 64)
    } else {
      val execNum = sswr(fileNum, 4)
      val n = if (execNum >= 6) 6 else execNum
      val exeMem = if (oneMem * 4 > exeMemMin) oneMem * 4 else exeMemMin
      Resource(exeMem.toString + "m", "4", n.toString, 64)
    }
  }

  private def sswr(fz: Int, fm: Int): Int = {
    val n = fz / fm
    val e = if (fz % 64 >= fm / 2) 1 else 0
    val cf = n + e

    if (cf > 0) cf else 1
  }

  /**
    * 获取GZ文件的Resource配置
    * @param rawFiles 原始文件格式路径 Array[RawFile]
    * @return Resource
    */
  def getGzResource(rawFiles: Array[RawFile]): Resource = {
    try {
      val dirSizes = for (dir <- rawFiles) yield dirFileSize_2(dir.filePath)
      val fileNums = for (s <- dirSizes) yield s.length
      val fileSizes = for (s <- dirSizes) yield s.max
      val fileSizeSum = for (s <- dirSizes) yield s.sum

      return forecastResourceForGZ(fileNums.sum, fileSizes.max, fileSizeSum.sum)
    } catch {
      case ex: Exception =>
        logger.error(s"The spark run error! args=${CommonUtils.objToJson(rawFiles)}", ex)
    }

    null
  }

  /**
    * 计算执行所需资源
    * @param fileNum 文件数量 Int
    * @param maxSize 最大大小 Long
    * @param allSize 总共大小 Long
    * @return 执行资源配置 Resource
    */
  def forecastResourceForGZ(fileNum: Int, maxSize: Long, allSize: Long): Resource = {
    val zipRatio = 16 // 压缩倍数
    val perCores = 3 // core  数量
    val maxExecNum = 16 // 默认最大executor数量
    val unzipSize = maxSize * zipRatio //预估未压缩大小
    val fullSize = fileSize((unzipSize / 0.66).toLong) // taskMem

    var partNum = fileSize(allSize * zipRatio) / 64 //
    if (partNum <= 24) {
      partNum = 24
    }

    var execNum = partNum / perCores
    if (execNum >= maxExecNum) {
      execNum = maxExecNum
    }

    var a = fileNum / execNum
    val b = fileNum % execNum
    if (a == 0) {
      a = 1
    } else if (a > 0 && b > 0) {
      a = a + 1
    }

    if (a >= perCores) {
      a = perCores
    }

    val taskMem = if (fullSize > 512) fullSize else 512
    //    val execMem = taskMem * perCores
    val execMem = taskMem * a

    Resource(execMem.toString + "m", perCores.toString, execNum.toString, partNum)

    //    if (fileNum <= perCores) {
    //      val execMem = taskMem * fileNum
    //      Resource(execMem.toString + "m", fileNum.toString, "1")
    //    } else {
    //      val execMem = taskMem * perCores
    //      val execNum = getExecNum(fileNum, perCores)
    //      Resource(execMem.toString + "m", perCores.toString, execNum.toString)
    //    }
  }

//  private def getExecNum(fileNum: Int, perCores: Int): Int = {
//    val n = fileNum / perCores
//    val y = fileNum % perCores
//
//    val execNum = if (y > 0) n + 1 else n
//
//    if (execNum > 8) 8 else execNum
//  }

}
