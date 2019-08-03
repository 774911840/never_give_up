package com.weshare.marlin.auto

import com.jayway.jsonpath.spi.json.JacksonJsonProvider
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider
import com.jayway.jsonpath.{Configuration, DocumentContext, JsonPath, Option}
import com.weshare.marlin.auto.MaskMain.maskCube
import com.weshare.marlin.constant.CommonConstant.DataType.{NULL, _}
import com.weshare.marlin.logging.WeshareLogging
import com.weshare.marlin.model.conf2.{FieldConf2, TaskConf2}
import com.weshare.marlin.utils.CommonUtils
import com.weshare.marlin.utils.CommonUtils._
import com.weshare.marlin.utils.NumberStrTools.numValueToStr
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by xuyi on 2018/8/30 15:46.
  */
object AutoMainV2 extends WeshareLogging {

  private val SUP_TYPE = Array(FLOAT, DOUBLE, BIG_DECIMAL)
  private val COLL_TYPE = Array(MAP, LIST, LINKED_LIST, LINKED_HASH_MAP, ARRAY, UNKNOWN)
  private val PACK_LIST_TYPE = Array(LIST, LINKED_LIST, ARRAY, UNKNOWN)
  private val PACK_MAP_TYPE = Array(MAP, LINKED_HASH_MAP)

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
    * 根据TaskConf2 进行数据脱敏
    * @param jsonStr
    * @param schema
    * @param confArray
    * @return
    */
  def process(jsonStr: String, schema: Array[String], confArray: Array[TaskConf2]): Array[Row] = {

    confArray.foreach { taskConf =>
      if (AutoFilter.dataFilter(jsonStr, taskConf.filterConf)) {
        return auto(jsonStr, schema, taskConf)
      }
    }

    null
  }

  case class FieldInfo(blockPath: String, indexPath: String, fullPath: String, fieldName: String, fieldValue: String)

  /**
    *
    * @param jsonStr
    * @param schema
    * @param taskConf
    * @return
    */
  private def auto(jsonStr: String, schema: Array[String], taskConf: TaskConf2): Array[Row] = {
    val pathCtx = JsonPath.using(pathConf).parse(jsonStr)
    val valueCtx = JsonPath.using(valueConf).parse(jsonStr)

    val confFieldInfoArray = getConfPath(jsonStr, taskConf.fieldList, pathCtx, valueCtx)
    val confFullPathArray = for (confFieldInfo <- confFieldInfoArray) yield confFieldInfo.fullPath
    val packFieldInfoArray = getPackPath(jsonStr, confFullPathArray, taskConf.packList, pathCtx, valueCtx)

    val allFieldInfoArray = confFieldInfoArray ++ packFieldInfoArray

    val allBlocks = for (fieldInfo <- allFieldInfoArray) yield fieldInfo.blockPath
    val longestPath = allBlocks.sortBy(_.length).reverse(0)

    val fieldConverge = mutable.HashMap[String, ArrayBuffer[FieldInfo]]()
    if ("_".equals(longestPath)) {
      // 单层
      fieldConverge += (longestPath -> allFieldInfoArray.to[ArrayBuffer])
    } else {
      val indexSet = mutable.Set[String]()
      for (fieldInfo <- allFieldInfoArray) {
        if (longestPath.equals(fieldInfo.blockPath)) {
          indexSet += fieldInfo.indexPath
        }
      }

      for (index <- indexSet) {
        for (fieldInfo <- allFieldInfoArray) {

          if ("_".equals(fieldInfo.blockPath)) {
            convergeField(index, fieldInfo, fieldConverge)
          } else {
            if (longestPath.equals(fieldInfo.blockPath)) {
              if (index.equals(fieldInfo.indexPath)) {
                convergeField(index, fieldInfo, fieldConverge)
              }
            } else {
              // not equal
              if (longestPath.contains(fieldInfo.blockPath) && index.startsWith(fieldInfo.indexPath)) {
                convergeField(index, fieldInfo, fieldConverge)
              }
            }
          }
        }
      }
    }

    // --
    val maskConfMap = mutable.HashMap[String, String]()
    val maskList = taskConf.maskList
    if (maskList != null && maskList.nonEmpty) {
      maskList.foreach { maskConf =>
        val paths: java.util.LinkedList[String] = pathCtx.read(maskConf.fieldPath)
        if (paths != null && paths.nonEmpty) {
          paths.foreach { path =>
            maskConfMap += (path -> maskConf.contentType)
          }
        }
      }
    }

    val rows = for (fieldInfos <- fieldConverge.values) yield assembleRow(schema, maskConfMap, fieldInfos.toArray)
    rows.toArray
  }

  /**
    *
    * @param jsonStr
    * @param fieldList
    * @param pathCtx
    * @param valueCtx
    * @return [fullPath,(fieldName,value)]
    */
  private def getConfPath(jsonStr: String,
                          fieldList: Array[FieldConf2],
                          pathCtx: DocumentContext,
                          valueCtx: DocumentContext): Array[FieldInfo] = {
    val fields = ArrayBuffer[FieldInfo]()
    fieldList.foreach { field =>
      val paths: java.util.LinkedList[String] = pathCtx.read(field.fieldPath)

      if (paths != null && paths.nonEmpty) {
        paths.foreach { fullPath =>
          val v = getConfValue(jsonStr, fullPath, valueCtx)
          if (v != null) {
            val p = CommonUtils.getBlockPath(fullPath)
            FieldInfo(p._1, p._2, fullPath, field.fieldName, v)
            fields += FieldInfo(p._1, p._2, fullPath, field.fieldName, v)
          }
        }
      }
    }

    fields.toArray
  }

  /**
    *
    * @param jsonStr
    * @param confFullPath
    * @param packList
    * @param pathCtx
    * @param valueCtx
    * @return [fullPath,(fieldName,value)]
    */
  private def getPackPath(jsonStr: String,
                          confFullPath: Array[String],
                          packList: Array[String],
                          pathCtx: DocumentContext,
                          valueCtx: DocumentContext): Array[FieldInfo] = {
    if (packList == null || packList.isEmpty) {
      return null
    }

    val fields = ArrayBuffer[FieldInfo]()
    packList.foreach { packPath =>
      val paths: java.util.LinkedList[String] = pathCtx.read(packPath)

      if (paths != null && paths.nonEmpty) {
        paths.foreach { fullPath =>
          if (confFullPath == null || !confFullPath.contains(fullPath)) {
            if (!packPath.contains("..*") && !fullPath.endsWith("']")) {
              // list
              val childrenPaths: java.util.LinkedList[String] = pathCtx.read(fullPath + ".*")
              if (childrenPaths != null && childrenPaths.nonEmpty) {
                childrenPaths.foreach { childrenPath =>
                  if (confFullPath == null || !confFullPath.contains(childrenPath)) {
                    val v = getPackValue(jsonStr, childrenPath, valueCtx)
                    val fieldName = getFiledNameByFullPath(childrenPath)

                    if (v != null && fieldName != null) {
                      val p = CommonUtils.getBlockPath(childrenPath)
                      fields += FieldInfo(p._1, p._2, childrenPath, "dm." + fieldName, v)
                    }
                  }
                }
              }
            } else {
              val v = getPackValue(jsonStr, fullPath, valueCtx)
              val fieldName = getFiledNameByFullPath(fullPath)

              if (v != null && fieldName != null) {
                val p = CommonUtils.getBlockPath(fullPath)
                fields += FieldInfo(p._1, p._2, fullPath, "dm." + fieldName, v)
              }
            }
          }
        }
      }
    }

    fields.toArray
  }

  private def getConfValue(jsonStr: String, fullPath: String, valueCtx: DocumentContext): String = {
    var strValue: String = null
    try {
      val value: Any = valueCtx.read(fullPath)
      strValue = matchType(value) match {
        case NULL => null
//        case t if SUP_TYPE.contains(t) =>
//          JsonPath.using(valueConf)
//            .parse(numValueToStr(getLastKey(fullPath), jsonStr))
//            .read[String](fullPath)

        case t if COLL_TYPE.contains(t) =>
          objToJson(value)
        case _ => value.toString
      }
    } catch {
      case ex: Exception =>
        strValue = "Failed to parse conf and get value!"
    }

    strValue
  }

  private def getPackValue(jsonStr: String, fullPath: String, valueCtx: DocumentContext): String = {
    var strValue: String = null

    try {
      val value: Any = valueCtx.read(fullPath)
      strValue = matchType(value) match {
        case NULL => null
        case t if SUP_TYPE.contains(t) =>
          JsonPath.using(valueConf)
            .parse(numValueToStr(getLastKey(fullPath), jsonStr))
            .read[String](fullPath)
        case t if PACK_MAP_TYPE.contains(t) => null
        case t if PACK_LIST_TYPE.contains(t) => {
          val tv = objToJson(value)
          if (tv.startsWith("[{") && tv.endsWith("}]")) {
            return null
          } else {
            return tv
          }
        }
        case _ => value.toString
      }
    } catch {
      case ex: Exception =>
        logger.error(s"Failed to read value ! $fullPath | $jsonStr", ex)
    }

    strValue
  }

  private def assembleRow(schema: Array[String], maskConf: mutable.HashMap[String, String], fieldInfoArray: Array[FieldInfo]): Row = {

    val mm = mutable.HashMap[String, Any]()
    fieldInfoArray.foreach { fieldInfo =>
      val fvm = mutable.HashMap[String, String]()

      if (maskConf.contains(fieldInfo.fullPath)) {
        // 脱敏
        val ma = maskCube(maskConf(fieldInfo.fullPath), fieldInfo.fieldName, fieldInfo.fieldValue)
        if (ma != null && ma.nonEmpty) {
          ma.foreach { m =>
            fvm += (m._1 -> m._2)
          }
        }
      } else {
        fvm += (fieldInfo.fieldName -> fieldInfo.fieldValue)
      }

      if (fvm.nonEmpty) {
        fvm.foreach { case (fn: String, fv: String) =>
          if (fn.contains(".")) {
            val fns = StringUtils.split(fn, ".", 2)
            if (mm.contains(fns(0))) {
              mm(fns(0)).asInstanceOf[mutable.HashMap[String, String]] += (fns(1) -> fv)
            } else {
              val mv = mutable.HashMap[String, String]()
              mv += (fns(1) -> fv)
              mm += (fns(0) -> mv)
            }
          } else {
            mm += (fn -> fv)
          }
        }
      }
    }

    val va = ArrayBuffer[Any]()
    schema.foreach { fn =>
      if (mm.contains(fn)) {
        va += mm(fn)
      } else {
        va += null
      }
    }

    Row.fromSeq(va)
  }

  /**
    * 多层 converge
    *
    * @param index
    * @param fieldInfo
    * @param fieldConverge
    */
  private def convergeField(index: String, fieldInfo: FieldInfo,
                            fieldConverge: mutable.HashMap[String, ArrayBuffer[FieldInfo]]): Unit = {
    if (fieldConverge.contains(index)) {
      fieldConverge(index) += fieldInfo
    } else {
      fieldConverge += (index -> ArrayBuffer[FieldInfo](fieldInfo))
    }
  }
}
