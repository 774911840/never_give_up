package com.weshare.marlin.utils

import com.weshare.marlin.logging.WeshareLogging
import org.apache.commons.configuration.tree.OverrideCombiner
import org.apache.commons.configuration.{CombinedConfiguration, PropertiesConfiguration}

/**
  * Created by xuyi on 2018/1/31 16:42.
  */
object MarlinProperty extends WeshareLogging {

  val props: CombinedConfiguration = new CombinedConfiguration()
  props.setNodeCombiner(new OverrideCombiner)

  try {
    val prodPath = ""
    val prodProps: PropertiesConfiguration = new PropertiesConfiguration(prodPath)

    logger.info(s"Load prod config $prodPath")

    props.addConfiguration(prodProps)
  } catch {
    case ex: Exception =>
      logger.warn("No prod property file!")
  }

  try {
    val defaultPropUrl = MarlinProperty.getClass.getResource("/marlin-local.properties")
    val defaultProps: PropertiesConfiguration = new PropertiesConfiguration(defaultPropUrl)

    logger.info(s"load resource default config $defaultPropUrl")

    props.addConfiguration(defaultProps)
  } catch {
    case ex: Exception =>
      logger.warn("No resource default app property file!")
  }

  try {
    val defaultPropUrl = MarlinProperty.getClass.getResource("/marlin-local.properties")
    val defaultProps: PropertiesConfiguration = new PropertiesConfiguration(defaultPropUrl)

    logger.info(s"load resource default config $defaultPropUrl")

    props.addConfiguration(defaultProps)
  } catch {
    case ex: Exception =>
      logger.warn("No resource default app property file!")
  }

  def getString(key: String): String = {
    props.getString(key)
  }

  def getInteger(key: String): Int = {
    props.getInt(key)
  }
}
