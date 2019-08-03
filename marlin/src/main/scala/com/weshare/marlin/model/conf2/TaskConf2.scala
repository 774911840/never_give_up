package com.weshare.marlin.model.conf2

/**
  * 配置的主要bean
  *
  * @param mainConf   任务的主要内容
  * @param filterConf 过滤条件
  * @param fieldList  特殊配置字段
  * @param packList   收录的地方
  */
@SerialVersionUID(2018L)
class TaskConf2(val mainConf: MainConf2,
                val filterConf: Array[Map[String, String]],
                val fieldList: Array[FieldConf2],
                val packList: Array[String],
                val maskList: Array[MaskConf2]) extends Serializable {

}
