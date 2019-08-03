package com.weshare.marlin.model.conf

/**
  * @param fieldName   表字段名称，eg：name，dm.name...
  * @param fieldPath   字段在数据中的xpath路径，eg：$.name
  * @param contentType 字段内容的类型，eg：手机号，身份证号码，邮箱等
  * @param mask        是否需要脱敏
  * @param unique      数据关系，eg：true 1:1,false 1:n
  */
@SerialVersionUID(2018L)
class FieldConf(val fieldName: String,
                val fieldPath: String,
                val contentType: String,
                val mask: Boolean,
                val unique: Boolean) extends Serializable {
}
