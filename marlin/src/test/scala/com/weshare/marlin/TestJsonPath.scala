package com.weshare.marlin

import com.jayway.jsonpath.spi.json.JacksonJsonProvider
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider
import com.jayway.jsonpath.{Configuration, JsonPath, Option}
import com.weshare.marlin.logging.WeshareLogging

import scala.collection.JavaConversions._


/**
  * Created by xuyi on 2018/8/30 16:58.
  */
object TestJsonPath extends WeshareLogging {
  def main(args: Array[String]): Unit = {
    //    val jsonStr = "{\"expensive\":10,\"store\":{\"bicycle\":{\"color\":\"red\",\"price\":19.95},\"book\":[{\"author\":\"Nigel Rees\",\"category\":\"reference\",\"price\":8.95,\"title\":\"Sayings of the Century\"},{\"author\":\"Evelyn Waugh\",\"category\":\"fiction\",\"price\":12.99,\"title\":\"Sword of Honour\"},{\"author\":\"Herman Melville\",\"category\":\"fiction\",\"isbn\":\"0-553-21311-3\",\"price\":8.99,\"title\":\"Moby Dick\"},{\"author\":\"J. R. R. Tolkien\",\"category\":\"fiction\",\"isbn\":\"0-395-19395-8\",\"price\":22.99,\"title\":\"The Lord of the Rings\"}]}}"

    //    val jsonStr = "{\"expensive\":10,\"store\":{\"bicycle\":{\"color\":\"red\",\"price\":19.95},\"book\":[{\"author\":\"Nigel Rees\",\"category\":\"reference\",\"price\":8.95,\"title\":\"Sayings of the Century\"},{\"author\":\"Evelyn Waugh\",\"category\":\"fiction\",\"price\":12.99,\"title\":\"Sword of Honour\"},{\"author\":\"Herman Melville\",\"category\":\"fiction\",\"isbn\":\"0-553-21311-3\",\"price\":8.99,\"title\":\"Moby Dick\"},{\"author\":\"J. R. R. Tolkien\",\"category\":\"fiction\",\"isbn\":\"0-395-19395-8\",\"price\":22.99,\"title\":\"The Lord of the Rings\"}],\"xuyi\":\"testnode\"}}"

    //    val jsonStr = "{\"expensive\":0.1,\"store\":{\"bicycle\":{\"color\":\"red\",\"price\":\"19.95\"},\"book\":[{\"author\":\"Nigel Rees\",\"category\":\"reference\",\"price\":8.95,\"title\":\"Sayings of the Century\"},{\"author\":\"Evelyn Waugh\",\"category\":\"fiction\",\"price\":12.99,\"title\":\"Sword of Honour\"},{\"author\":\"Herman Melville\",\"category\":\"fiction\",\"isbn\":\"0-553-21311-3\",\"price\":8.99,\"title\":\"Moby Dick\"},{\"author\":\"J. R. R. Tolkien\",\"category\":\"fiction\",\"isbn\":\"0-395-19395-8\",\"price\":22.99,\"title\":\"The Lord of the Rings\"}],\"xuyi\":\"testnode\",\"xuyilist\":[1,2,3]}}"

    val jsonStr = "{\"globalType\":\"black\",\"sptype\":\"xybg.black.gf\",\"record_gid\":\"5cf6925a38f047a9a749015c8bae8297_1\",\"user_gid\":\"21667da7-2bba-4e52-9c6d-e1bf31b57c4c\",\"order_id\":\"31206627480522199040\",\"channel\":\"1030100280\",\"bizType\":\"zxmc\",\"name\":\"王海军\",\"phone\":\"13832920744\",\"idcardNo\":\"130227197409085014\",\"totalItems\":7,\"currentItem\":5,\"crawler_time\":1549957654464,\"report\":{\"type\":1,\"identity\":\"130227197409085014\",\"name\":\"王海军\",\"cases\":[{\"type\":0,\"caseID\":\"（2018）冀02执12356号\",\"caseCreateTime\":\"2018-06-10T16:00:00.000Z\",\"courtName\":\"唐山市中级人民法院\",\"province\":\"\",\"executionTarget\":\"64800\"}]},\"s_path\":\"/data/bigdata/xybgblack/black.20190212.log\",\"s_line_num\":473,\"s_host\":\"tai-as-sx-53\",\"s_site\":\"tai\",\"s_forwarder\":\"tai-as-sx-53,1549957654,tai-es-04,1549957655,tai-bg-07,1549957656\",\"tag\":\"spider.xybgblack\",\"time\":\"2019-02-12T15:47:34+08:00\"}"

    val pathConf = Configuration.builder
      .jsonProvider(new JacksonJsonProvider())
      .mappingProvider(new JacksonMappingProvider())
      .options(Option.AS_PATH_LIST)
      .options(Option.SUPPRESS_EXCEPTIONS).build
    val pathCtx = JsonPath.using(pathConf).parse(jsonStr)

    val valueConf = Configuration.builder
      .jsonProvider(new JacksonJsonProvider())
      .mappingProvider(new JacksonMappingProvider())
      .options(Option.DEFAULT_PATH_LEAF_TO_NULL).build
    val valueCtx = JsonPath.using(valueConf).parse(jsonStr)

    //    val keyPath = "$.store.book[*].*"
    //    val keyPath = "$.*"
    //    val keyPath = "$.store.*"
    //    val keyPath = "$..bicycle.*"
    //    val keyPath = "$..store..*"
    //    val keyPath = "$..*"
    //    val keyPath = "$..category"

    //    val keyPath = "$.feedModels.*"
    val keyPath = "$.report.cases..type"

    val ps: java.util.LinkedList[String] = pathCtx.read(keyPath)

    ps.foreach { p =>
      //      val value: Any = valueCtx.read(p)
      //      val vtype = matchType(value)
      //
      //      logger.info(s"$p   $vtype")

      logger.info(s"$p")
    }
  }

}
