package com.weshare.marlin.utils

import com.weshare.marlin.logging.WeshareLogging
import org.apache.spark.sql.SparkSession

import scala.collection.mutable._

/**
  * Created by xuyi on 2018/7/16 20:18.
  */
object TableUtils extends WeshareLogging {

  /**
    * 获取表的分区
    *
    * @param tableName 表名
    * @param partMap   分区过滤条件，可以为空
    * @param spark     spark上下文
    * @return 返回分区条件下的所有分区，值不为null，可能为empty
    */
  def getTableParts(tableName: String, partMap: LinkedHashMap[String, String], spark: SparkSession): ArrayBuffer[Map[String, String]] = {
    logger.warn(s"get table parts. tableName=$tableName, part=$partMap")

    var partSql: String = null
    if (partMap == null || partMap.isEmpty) {
      partSql = s"SHOW PARTITIONS $tableName"
    } else {
      var partArray = new ArrayBuffer[String]()
      partMap.keys.foreach { key =>
        val value = partMap(key)
        partArray += s"$key='$value'"
      }

      val parts: String = partArray.mkString(",")
      partSql = s"SHOW PARTITIONS $tableName PARTITION ($parts)"
    }

    logger.info(s"get the part sql: $partSql")

    import spark.implicits._
    import spark.sql
    val partRows: Array[String] = sql(partSql).as[String].collect()

    var partList = new ArrayBuffer[Map[String, String]]()
    for (i <- 0 until partRows.length) {
      var pm: Map[String, String] = Map()
      val p: Array[String] = partRows(i).split('/')
      for (j <- 0 until p.length) {
        val sp = p(j).split("=")
        pm += (sp(0) -> sp(1))
      }

      partList += pm
    }

    partList
  }

  /**
    * 检查表是否有该分区
    *
    * @param tableName 表名
    * @param partMap   分区，多分区和单分区都适用
    * @param spark     spark上下文
    * @return
    */
  def checkTableParts(tableName: String, partMap: LinkedHashMap[String, String], spark: SparkSession): Boolean = {
    val partArray = getTableParts(tableName, partMap, spark)

    if (partArray.isEmpty) false else true
  }

  /**
    * 修复表分区
    *
    * @param tableName
    * @param spark
    */
  def msckTable(tableName: String, spark: SparkSession): Unit = {
    spark.sql(s"MSCK REPAIR TABLE $tableName")
  }

}
