package com.weshare.marlin.utils

import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder
import org.apache.commons.configuration2.builder.fluent.Parameters
import org.apache.commons.configuration2.{FileBasedConfiguration, PropertiesConfiguration}


/**
  * @Desc:
  * @Author: Mars9527
  * @Date: 2018-11-11
  */
object ConfigurationUtil {

  /*
  用法
  val config: FileBasedConfiguration = ConfigurationUtil("condition.properties").config
  val conditionJson: String = config.getString("condition.params.json")*/

  def apply(file:String): ConfigurationUtil = {
    val configurationUtil = new ConfigurationUtil()
    if (configurationUtil.config == null) {
      configurationUtil.config = new FileBasedConfigurationBuilder[FileBasedConfiguration](classOf[PropertiesConfiguration])
        .configure(new Parameters().properties().setFileName(file)).getConfiguration
    }
    configurationUtil
  }
/**
 * desc:测试
 * @param:[args]
 * @return:void
 * @author:Mars9527
 * @date:2018/11/11
 */
  def main(args: Array[String]): Unit = {
    val config: FileBasedConfiguration = ConfigurationUtil(CommonUtils.getClassPathFile("local.properties")).config
    val database: String = config.getString("core.site")
    println(database)
  }

}

class ConfigurationUtil{

  var config: FileBasedConfiguration = null

}
