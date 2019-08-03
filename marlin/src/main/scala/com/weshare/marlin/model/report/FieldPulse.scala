package com.weshare.marlin.model.report

import org.apache.spark.sql.Row

/**
  *
  * @param pathNotExist 路径是否存在，0：存在，1：不存在
  * @param rawType      路径原始数据类型
  * @param valueBlank   值为空，0：不为空，1：为空
  */
@SerialVersionUID(2018L)
class FieldPulse(val fieldName: String,
                 val pathNotExist: Integer = 1,
                 val rawType: String,
                 val valueBlank: Integer = 1,
                 val cnt: Long = 1) extends Serializable {

  def this(fieldName: String, pathNotExist: Int) {
    this(fieldName, pathNotExist, null, null, 1)
  }

  def this(fieldName: String, pathNotExist: Int, rawType: String) {
    this(fieldName, pathNotExist, rawType, null, 1)
  }

  def toRow(): Row = {
    Row(this.fieldName, this.pathNotExist, this.rawType, this.valueBlank)
  }

}
