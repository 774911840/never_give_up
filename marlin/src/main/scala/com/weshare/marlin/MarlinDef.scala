package com.weshare.marlin

import com.weshare.marlin.model.conf.{FieldConf, TaskConf}
import com.weshare.marlin.model.conf2.{FieldConf2, TaskConf2}
import com.weshare.marlin.utils.ResourceUtils.RawFile
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{MapType, StringType, StructField, StructType}

import scala.collection._
import scala.collection.mutable.ArrayBuffer

/**
  * Created by xuyi on 2018/9/19 12:02.
  */
object MarlinDef {

  def readFile(rawArray: Array[RawFile], spark: SparkSession): RDD[String] = {
    val rdds = new ArrayBuffer[RDD[String]]()

    for (r <- rawArray) {
      val rdd = r.fileType match {
        case "gz" => {
          spark.read.textFile(r.filePath).rdd
        }
      }

      rdds += rdd
    }

    spark.sparkContext.union(rdds)
  }

  /**
    *获取schema
    * @param cs
    * @param cs2
    * @return
    */
  def getSchema(cs: Array[TaskConf], cs2: Array[TaskConf2]): StructType = {
    if (cs != null && cs.nonEmpty) {
      getSchema(cs)
    } else {
      getSchema2(cs2)
    }
  }

  private def getSchema(cs: Array[TaskConf]): StructType = {
    var fields = new ArrayBuffer[FieldConf]()
    for (c <- cs) {
      fields = fields ++ c.fieldList
    }

    val fieldNames = for (f <- fields) yield f.fieldName
    getSchema(fieldNames)
  }

  private def getSchema2(cs: Array[TaskConf2]): StructType = {
    var fields = new ArrayBuffer[FieldConf2]()
    for (c <- cs) {
      fields = fields ++ c.fieldList
    }

    val fieldNames = for (f <- fields) yield f.fieldName
    getSchema(fieldNames)
  }

  /**
    * 封装schema
    * @param fieldNames
    * @return
    */
  private def getSchema(fieldNames: ArrayBuffer[String]): StructType = {
    val dmSet = mutable.Set[String]()
    val fieldArray = new ArrayBuffer[StructField]()

    fieldNames.foreach { fieldName =>
      if (fieldName.contains('.')) {
        dmSet.add(fieldName.split('.')(0))
      } else {
        fieldArray += StructField(fieldName, StringType, true)
      }
    }

    var dm = false
    if (dmSet.nonEmpty) {
      for (item <- dmSet.toArray.sorted) {
        if (item == "dm") {
          dm = true
        }

        fieldArray += StructField(item, MapType(StringType, StringType), true)
      }
    }

    if (!dm) {
      fieldArray += StructField("dm", MapType(StringType, StringType), true)
    }

    StructType(fieldArray)
  }

}
