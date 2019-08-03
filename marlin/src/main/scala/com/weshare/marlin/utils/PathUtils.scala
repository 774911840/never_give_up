package com.weshare.marlin.utils

import com.hery.bigdata.conf.MarlinProperty
import com.hery.bigdata.utils.KrbUtils
import com.weshare.marlin.constant.CommonConstant
import com.weshare.marlin.logging.WeshareLogging
import com.weshare.marlin.model.report.FileInfo
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

/**
  * Created by xuyi on 2018/7/17 16:18.
  */
object PathUtils extends WeshareLogging {

  /**
    * 检查hdfs目录是否存在
    *
    * @param spark spark上下文
    * @param paths 路径
    * @return
    */
  def checkDirExists(spark: SparkSession, paths: String*): Boolean = {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    var notExistsPath = new ArrayBuffer[String]()
    for (path <- paths) {
      if (StringUtils.isBlank(path)) {
        notExistsPath += path
      } else if (!fs.exists(new Path(path))) {
        logger.error(s"经过检查，hdfs中该目录不存在：$path")
        notExistsPath += path
      }
    }

    if (notExistsPath.nonEmpty) false else true
  }

  /**
    * 判断路径中真实存在并且有文件的目录
    *
    * @param path
    * @return 返回有数据的目录
    */
  def checkPathExists(path: String): Boolean = {
    import org.apache.hadoop.conf.Configuration

    val conf = new Configuration()
    conf.set("fs.defaultFS", CommonConstant.DEFAULT_FS)
    val fs = FileSystem.get(conf)

    val p = new Path(path)
    if (fs.exists(p)) {
      val files = fs.listStatus(p)
      if (files.nonEmpty) {
        return true
      }
    }

    false
  }

  /**
    * 获取hdfs目录下子目录的名字
    * FileStatus{path=hdfs://hbasedata/user/koala/xuyi/BD-3225; isDirectory=true; modification_time=1529654058058; access_time=0; owner=koala; group=koala; permission=rwxr-xr-x; isSymlink=false}
    *
    * @param path
    * @param spark
    */
  def listChildDirName(path: String, spark: SparkSession): Array[String] = {
    val nameArray = new ArrayBuffer[String]()

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    for (f <- fs.listStatus(new Path(path))) {
      if (f.isDirectory) nameArray += f.getPath.getName
    }

    nameArray.toArray
  }

  /**
    * return info's json str
    *
    * @param path
    * @param spark
    * @return
    */
  def listDirInfo(path: String, spark: SparkSession): Array[FileInfo] = {
    val res = new ArrayBuffer[FileInfo]()
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    for (f <- fs.listStatus(new Path(path))) {
      res += new FileInfo(f.getPath.toString, f.isDirectory, f.getLen, f.getModificationTime, f.getAccessTime)
    }

    res.toArray
  }

  def listDirInfo(paths: Array[String], spark: SparkSession): Array[FileInfo] = {
    var res = new ArrayBuffer[FileInfo]()

    for (p <- paths) {
      res = res ++ listDirInfo(p, spark)
    }

    res.toArray
  }

  /**
    * 获取hdfs 上path路径下的文件信息
    * @param path
    * @return
    */
  def listDirInfo(path: String): Array[FileInfo] = {
//    import org.apache.hadoop.conf.Configuration
//    val conf = new Configuration()
    val res = new ArrayBuffer[FileInfo]()
    val prop=new MarlinProperty
    var conf:Configuration=null
    if(prop.getString("project.environment")=="local") {
      // 获取认证后的conf
      conf = KrbUtils.getConfiguration()
      conf.set("fs.defaultFS", CommonConstant.DEFAULT_FS)
    }else{
      conf=new Configuration
    }

    val fs = FileSystem.get(conf)

    for (f <- fs.listStatus(new Path(path))) {
      res += new FileInfo(f.getPath.toString, f.isDirectory, f.getLen, f.getModificationTime, f.getAccessTime)
    }

    res.toArray
  }


  /**
    * 获取单分区表的hdfs路径
    * eg:
    * hdfs://hbasedata/apps/hive/datawarehouse/fk2/t_ods_fk_derived_wy_log
    *
    * root
    * |-- col_name: string (nullable = false)
    * |-- data_type: string (nullable = false),
    * |-- comment: string (nullable = true)
    *
    * @param tableName
    * @param taskDate
    * @param spark
    * @return
    */
  def getWritePath(tableName: String, taskDate: String, spark: SparkSession): String = {
    logger.warn(s"Get task write path! tableName=$tableName,taskDate=$taskDate")

    val info = spark.sql(s"DESC FORMATTED $tableName").collect()
    var location = ""
    for (i <- info) {
      if (i.getString(0).contains("Location")) {
        location = i.getString(1)
      }
    }

    if (StringUtils.isBlank(location)) null else s"$location/dt=$taskDate"
  }

}
