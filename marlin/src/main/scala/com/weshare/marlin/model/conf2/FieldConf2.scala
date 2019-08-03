package com.weshare.marlin.model.conf2

/**
  * @param fieldName   表字段名称，eg：name，dm.name...
  * @param fieldPath   字段在数据中的xpath路径，eg：$.name
  */
@SerialVersionUID(2018L)
class FieldConf2(val fieldName: String,
                 val fieldPath: String) extends Serializable {
}
