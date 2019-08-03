package com.weshare.marlin.model.conf

/**
  * 配置的主要bean
  *
  * @param mainConf   任务的主要内容
  * @param filterConf 过滤条件
  * @param fieldList  特殊配置字段
  */
@SerialVersionUID(2018L)
class TaskConf(val mainConf: MainConf,
               val filterConf: Array[Map[String, String]],
               val fieldList: Array[FieldConf]) extends Serializable {

}
