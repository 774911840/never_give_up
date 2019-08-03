package com.weshare.marlin.auto

import com.jayway.jsonpath.spi.json.JacksonJsonProvider
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider
import com.jayway.jsonpath.{Configuration, DocumentContext, JsonPath, Option}
import com.weshare.marlin.auto.MaskMain._
import com.weshare.marlin.constant.CommonConstant.DataType._
import com.weshare.marlin.logging.WeshareLogging
import com.weshare.marlin.model.conf.{FieldConf, TaskConf}
import com.weshare.marlin.utils.CommonUtils.{matchType, objToJson, upperToUnderLine}
import com.weshare.marlin.utils.NumberStrTools.numValueToStr
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


/**
  * 数据脱敏类
  * Created by xuyi on 2018/8/30 15:46.
  */
object AutoMain extends WeshareLogging {

  private val SUP_TYPE = Array(FLOAT, DOUBLE, BIG_DECIMAL)
  private val COLL_TYPE = Array(MAP, LIST, LINKED_LIST, LINKED_HASH_MAP, ARRAY, UNKNOWN)

  private val pathConf = Configuration.builder
    .jsonProvider(new JacksonJsonProvider())
    .mappingProvider(new JacksonMappingProvider())
    .options(Option.AS_PATH_LIST)
    .options(Option.SUPPRESS_EXCEPTIONS).build

  private val valueConf = Configuration.builder
    .jsonProvider(new JacksonJsonProvider())
    .mappingProvider(new JacksonMappingProvider())
    .options(Option.DEFAULT_PATH_LEAF_TO_NULL).build

  /**
    * 根据TaskConf 进行数据脱敏处理
    * @param jsonStr  原始json数据
    * @param schema fields信息
    * @param confArray
    * @return
    */
  def process(jsonStr: String, schema: Array[String], confArray: Array[TaskConf]): Array[Row] = {
    val rowArray = new ArrayBuffer[Row]()

    confArray.foreach { taskConf =>
      if (AutoFilter.dataFilter(jsonStr, taskConf.filterConf)) {
        if (taskConf.mainConf.unique) {
          rowArray += oAuto(jsonStr, schema, taskConf)
        } else {
          rowArray ++= nAuto(jsonStr, schema, taskConf)
        }
      }
    }

    rowArray.toArray
  }

  private def oAuto(jsonStr: String, schema: Array[String], taskConf: TaskConf): Row = {
    val pathCtx = JsonPath.using(pathConf).parse(jsonStr)
    val valueCtx = JsonPath.using(valueConf).parse(jsonStr)

    val fieldMap: mutable.LinkedHashMap[String, Any] = mutable.LinkedHashMap()

    val confPaths = new ArrayBuffer[String]()
    taskConf.fieldList.foreach { field =>
      val paths: java.util.LinkedList[String] = pathCtx.read(field.fieldPath)
      if (paths != null && paths.nonEmpty) {
        // 在1:1情况下一个路径只保留一个值
        val fullPath = paths(0)
        confPaths += fullPath

        val fieldValues = getOConfValue(fullPath, jsonStr, field, valueCtx)
        if (fieldValues != null && fieldValues.nonEmpty) fieldValues.foreach { v => setOValueMap(v, fieldMap) }
      }
    }

    if (taskConf.mainConf.pack) {
      val allPaths: java.util.LinkedList[String] = pathCtx.read("$..*")
      val otherPaths = allPaths.diff(confPaths)

      otherPaths.foreach { fullPath =>
        val fieldValue = getOPackValue(fullPath, jsonStr, valueCtx)
        if (fieldValue != null) setOValueMap(fieldValue, fieldMap)
      }
    }

    val valueArray = new ArrayBuffer[Any]()
    schema.foreach { s =>
      if (fieldMap.contains(s)) {
        valueArray += fieldMap(s)
      } else {
        valueArray += null
      }
    }

    Row.fromSeq(valueArray)
  }

  private def nAuto(jsonStr: String, schema: Array[String], taskConf: TaskConf): Array[Row] = {
    val pathCtx = JsonPath.using(pathConf).parse(jsonStr)
    val valueCtx = JsonPath.using(valueConf).parse(jsonStr)

    val oFieldMap: mutable.LinkedHashMap[String, Any] = mutable.LinkedHashMap()
    val nFieldMap: mutable.LinkedHashMap[String, mutable.LinkedHashMap[String, Any]] = mutable.LinkedHashMap()

    taskConf.fieldList.foreach { field =>
      val paths: java.util.LinkedList[String] = pathCtx.read(field.fieldPath)
      if (paths != null && paths.nonEmpty) {
        if (field.unique) {
          // 在1:1情况下一个路径只保留一个值
          val fieldValues = getOConfValue(paths(0), jsonStr, field, valueCtx)
          if (fieldValues != null && fieldValues.nonEmpty) fieldValues.foreach { v => setOValueMap(v, oFieldMap) }
        } else {
          paths.foreach { fullPath =>
            val fieldValues = getOConfValue(fullPath, jsonStr, field, valueCtx)
            if (fieldValues != null && fieldValues.nonEmpty) fieldValues.foreach { v => setNValueMap(getPathIndex(fullPath), v, nFieldMap) }
          }
        }
      }
    }


    val rows = new ArrayBuffer[Row]()
    for (v <- nFieldMap.values) {
      val valueArray = new ArrayBuffer[Any]()
      schema.foreach { s =>
        var fv: Any = null
        if (oFieldMap.contains(s) && v.contains(s)) {
          val ov = oFieldMap(s)
          val nv = v(s)
          if (ov != null && nv == null) {
            fv = ov
          } else if (ov == null && nv != null) {
            fv = nv
          } else if (ov != null && nv != null) {
            if (matchType(ov) == STRING) {
              fv = ov
            } else {
              // map merge
              val ovm = ov.asInstanceOf[mutable.LinkedHashMap[String, Any]]
              val nvm = nv.asInstanceOf[mutable.LinkedHashMap[String, Any]]
              if (ovm.isEmpty && nvm.nonEmpty) {
                fv = nv
              } else if (ovm.nonEmpty && nvm.isEmpty) {
                fv = ov
              } else if (ovm.nonEmpty && nvm.nonEmpty) {
                ovm.putAll(nvm)
                fv = ovm
              }
            }
          }
        } else if (oFieldMap.contains(s) && !v.contains(s)) {
          fv = oFieldMap(s)
        } else if (!oFieldMap.contains(s) && v.contains(s)) {
          fv = v(s)
        }

        valueArray += fv
      }

      rows += Row.fromSeq(valueArray)
    }

    rows.toArray
  }

  private def setOValueMap(value: (String, String), fieldMap: mutable.LinkedHashMap[String, Any]): Unit = {
    if (value != null) {
      val fieldName = value._1
      val fieldValue = value._2

      if (fieldName.contains('.')) {
        val nameSplit = fieldName.split('.')

        if (fieldMap.keySet.contains(nameSplit(0))) {
          fieldMap(nameSplit(0)).asInstanceOf[mutable.LinkedHashMap[String, Any]] += (nameSplit(1) -> fieldValue)
        } else {
          fieldMap += (nameSplit(0) -> mutable.LinkedHashMap(nameSplit(1) -> fieldValue))
        }
      } else {
        fieldMap += (fieldName -> fieldValue)
      }
    }
  }

  private def setNValueMap(pathIndex: String,
                           value: (String, String),
                           nFieldMap: mutable.LinkedHashMap[String, mutable.LinkedHashMap[String, Any]]): Unit = {

    if (value != null) {
      if (!nFieldMap.contains(pathIndex)) nFieldMap += (pathIndex -> new mutable.LinkedHashMap[String, Any]())
      setOValueMap(value, nFieldMap(pathIndex))
    }
  }

  private def getOConfValue(fullPath: String, jsonStr: String, fieldConf: FieldConf, valueCtx: DocumentContext): Array[(String, String)] = {
    try {
      val value: Any = valueCtx.read(fullPath)

      val strValue = matchType(value) match {
        case NULL => null
//        case t if SUP_TYPE.contains(t) =>
//          JsonPath.using(valueConf)
//            .parse(numValueToStr(getLastKey(fullPath), jsonStr))
//            .read[String](fullPath)
        case t if COLL_TYPE.contains(t) =>
          objToJson(value)
        case _ => value.toString
      }

      val fieldName = fieldConf.fieldName
      if (StringUtils.isNotBlank(strValue) && fieldConf.mask) {
        return maskCube(fieldConf.contentType, fieldName, strValue)
      } else {
        return Array((fieldName, strValue))
      }
    } catch {
      case ex: Exception => {
        logger.error(s"Failed to read value ! $fullPath | $fullPath", ex)
      }
    }

    null
  }

  private def getOPackValue(fullPath: String, jsonStr: String, valueCtx: DocumentContext): (String, String) = {
    try {
      val value: Any = valueCtx.read(fullPath)

      val strValue = matchType(value) match {
        case NULL => null
        case t if COLL_TYPE.contains(t) => null
        case t if SUP_TYPE.contains(t) =>
          JsonPath.using(valueConf)
            .parse(numValueToStr(getLastKey(fullPath), jsonStr))
            .read[String](fullPath)
        case _ => value.toString
      }

      val fieldName = getFiledNameByFullPath(fullPath)
      if (fieldName != null && strValue != null) {
        return ("dm." + fieldName, strValue)
      }
    } catch {
      case ex: Exception => {
        logger.error(s"Failed to read value ! $fullPath | $fullPath", ex)
      }
    }

    null
  }

  private def getFiledNameByFullPath(fullPath: String): String = {
    upperToUnderLine(pathLastName(fullPath))
  }

  private def pathLastName(fullPath: String): String = {
    if (StringUtils.isNotBlank(fullPath) && fullPath.endsWith("']")) {
      fullPath.substring(fullPath.lastIndexOf("['")).replace("['", "").replace("']", "")
    } else {
      null
    }
  }

  /**
    *
    * @param path eg: $['store'][0]['book'][1]['price']
    * @return "_0_1_"
    */
  private def getPathIndex(path: String): String = {
    val indexList = new ArrayBuffer[String]()
    for (item <- path.split("\\]\\[")) {
      val key = item.replace("\\$", "").replace("\\[", "").replace("\\]", "")
      if (!key.contains("'")) {
        indexList += key
      } else {
        indexList += "_"
      }
    }

    indexList.mkString
  }

  private def getLastKey(path: String): String = {
    if (path.startsWith("$[")) {
      path.split("\\]\\[").last.replace("$", "").replace("[", "").replace("]", "").replace("'", "")
    } else {
      path.split('.').last
    }
  }
}
