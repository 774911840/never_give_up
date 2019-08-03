package com.weshare.marlin.utils

import com.weshare.marlin.logging.WeshareLogging
import com.weshare.marlin.model.report.FieldPulse
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by xuyi on 2018/9/17 18:33.
  */
object PulseUtils extends WeshareLogging {

  def getFieldPulse(rdd: RDD[Row], spark: SparkSession): Array[FieldPulse] = {
    spark.createDataFrame(rdd, pulseSchema)
      .createOrReplaceTempView("t_field_pulse")

    val pulseRows = spark.sql(
      """
        |SELECT field_name,raw_type,path_not_exist,value_blank,COUNT(1) AS cnt
        |FROM t_field_pulse
        |GROUP BY field_name,raw_type,path_not_exist,value_blank
      """.stripMargin).collect()

    getPulseReport(pulseRows)
  }

  private def getPulseReport(rowArray: Array[Row]): Array[FieldPulse] = {

    val resArray = new ArrayBuffer[FieldPulse]()
    for (r <- rowArray) {
      val fieldName = r.getString(0)

      var rawType: String = null
      if (!r.isNullAt(1)) {
        rawType = r.getString(1)
      }

      var pathNotExist: Integer = null
      if (!r.isNullAt(2)) {
        pathNotExist = r.getInt(2)
      }

      var valueBlank: Integer = null
      if (!r.isNullAt(3)) {
        valueBlank = r.getInt(3)
      }

      val cnt = r.getLong(4)

      resArray += new FieldPulse(fieldName, pathNotExist, rawType, valueBlank, cnt)
    }

    resArray.toArray
  }

  private val pulseSchema = StructType(List(
    StructField("field_name", StringType, false),
    StructField("path_not_exist", IntegerType, true),
    StructField("raw_type", StringType, true),
    StructField("value_blank", IntegerType, true)))

}
