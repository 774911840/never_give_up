package com.weshare.marlin.utils

import java.util.UUID

import com.hery.bigdata.conf.MarlinProperty
import com.hery.bigdata.utils.KrbUtils
import com.weshare.marlin.constant.CommonConstant
import com.weshare.marlin.logging.WeshareLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by xuyi on 2018/7/17 17:25.
  */
object WriteUtils extends WeshareLogging {

  private val PATH_TEMP = "/user/koala/temp/"
  private val PATH_TAG = "/user/koala/tags/"

  /**
    * 任务执行成功后在hdfs固定位置写tag文件，方便其它依赖任务check
    *
    * @param taskDate
    * @param tableName
    */
  def writeTag(taskDate: String, tableName: String): Unit = {
    val writePath = PATH_TAG + taskDate + '/' + tableName
    try {
      logger.warn(s"writeTag | entry | $writePath")

      //      import org.apache.hadoop.conf.Configuration
      //      val conf = new Configuration()
      //      conf.set("fs.defaultFS", CommonConstant.DEFAULT_FS)
      //
      // 获取认证后的conf
      var conf:Configuration=null
      val prop=new MarlinProperty
      if(prop.getString("project.environment")=="local") {
        // 获取认证后的conf
        conf = KrbUtils.getConfiguration()
        conf.set("fs.defaultFS", CommonConstant.DEFAULT_FS)
      }else{
        conf=new Configuration
      }
      conf.set("fs.defaultFS", CommonConstant.DEFAULT_FS)
      val fs = FileSystem.get(conf)

      fs.mkdirs(new Path(writePath))
    } catch {
      case ex: Exception => {
        logger.error(s"writeTag | failed write tag | $writePath", ex)
      }
    }
  }

  def generateTempPath: String = {
    PATH_TEMP + UUID.randomUUID().toString
  }

  /**
    * write parquet file, the default compression type is snappy
    *
    * @param finalPath
    * @param df
    * @param spark
    */
  def writeParquet(finalPath: String, df: DataFrame, spark: SparkSession): Unit = {
    logger.warn(s"The final write path: $finalPath")

    val tempPath = generateTempPath

    logger.warn(s"The final write temp path: $tempPath")

    df.write.mode("overwrite").option("compression", "snappy").parquet(tempPath)

    val partNum = getPartNum(tempPath, spark)
    spark.read.parquet(tempPath)
      .repartition(partNum)
      .write.mode("overwrite")
//      .option("compression", "snappy")  //TODO    修养放开
      .parquet(finalPath)

    logger.warn(s"Repart num is: $partNum")

    delFile(tempPath, spark)
  }

  /**
    * write json file, the default compression type is gzip
    *
    * @param finalPath
    * @param df
    * @param spark
    */
  def writeJson(finalPath: String, df: DataFrame, spark: SparkSession): Unit = {
    logger.warn(s"The final write path: $finalPath")

    val tempPath = PATH_TEMP + UUID.randomUUID().toString

    logger.warn(s"The temp path: $tempPath")

    df.write.mode("overwrite").option("compression", "gzip").json(tempPath)

    val partNum = getPartNum(tempPath, spark)
    spark.read.json(tempPath)
      .repartition(partNum)
      .write.mode("overwrite")
      .option("compression", "gzip")
      .json(finalPath)

    logger.warn(s"Repart num is: $partNum")

    delFile(tempPath, spark)
  }

  private def getPartNum(dfsPath: String, spark: SparkSession): Int = {
    val pathLegth = FileSystem.get(spark.sparkContext.hadoopConfiguration).getContentSummary(new Path(dfsPath)).getLength
    val mb = 1024 * 1024

    var partInt = (pathLegth / (128 * mb)).asInstanceOf[Number].intValue()
    val partRemainder = pathLegth % (128 * mb)
    if (partRemainder > (10 * mb)) {
      partInt = partInt + 1
    }

    if (partInt == 0) {
      partInt = partInt + 1
    }

    partInt
  }

  def delFile(dfsPath: String, spark: SparkSession): Unit = {
    logger.warn(s"Del file! $dfsPath")
    try {
      FileSystem.get(spark.sparkContext.hadoopConfiguration).delete(new Path(dfsPath), true)
    } catch {
      case ex: Exception => logger.error(s"Failed to del task temp file. $dfsPath", ex)
    }
  }

  def delFile(dfsPath: String): Unit = {
    logger.warn(s"Del file! $dfsPath")
    try {
      import org.apache.hadoop.conf.Configuration
      val conf = new Configuration()
      conf.set("fs.defaultFS", CommonConstant.DEFAULT_FS)
      val fs = FileSystem.get(conf)

      fs.delete(new Path(dfsPath), true)
    } catch {
      case ex: Exception => logger.error(s"Failed to del task temp file. $dfsPath", ex)
    }
  }

}