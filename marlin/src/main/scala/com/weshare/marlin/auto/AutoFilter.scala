package com.weshare.marlin.auto

import com.jayway.jsonpath.spi.json.JacksonJsonProvider
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider
import com.jayway.jsonpath.{Configuration, DocumentContext, JsonPath, Option}

object AutoFilter {

  private val valueConf = Configuration.builder
    .jsonProvider(new JacksonJsonProvider())
    .mappingProvider(new JacksonMappingProvider())
    .options(Option.DEFAULT_PATH_LEAF_TO_NULL).build

  def dataFilter(jsonStr: String, filterArray: Array[Map[String, String]]): Boolean = {

    try {
      // check string is json
      val valueCtx = JsonPath.using(valueConf).parse(jsonStr)

      if (filterArray.isEmpty) {
        return true
      }

      for (f <- filterArray) {
        if (dataFilter(valueCtx, f)) {
          return true
        }
      }
    } catch {
      case ex: Exception =>
        return false
    }

    false
  }

  private def dataFilter(valueCtx: DocumentContext, filterConf: Map[String, String]): Boolean = {
    if (filterConf == null || filterConf.isEmpty) {
      return true
    }

    try {
      filterConf.keys.foreach { key =>
        val value = filterConf(key)
        val v = valueCtx.read[String](key)
        println("key:    "+v+"   "+value)
        if (v != value) {
          return false
        }
      }
    } catch {
      case _: Exception => {
        return false
      }
    }

    true
  }
}
