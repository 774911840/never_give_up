package com.weshare.marlin.tools

import com.weshare.marlin.logging.WeshareLogging
import com.weshare.marlin.model.conf.TaskConf
import com.weshare.marlin.model.conf2.{FieldConf2, MainConf2, MaskConf2, TaskConf2}
import com.weshare.marlin.utils.CommonUtils

import scala.collection.mutable.ArrayBuffer

object ConfV1ToV2 extends WeshareLogging {

  def main(args: Array[String]): Unit = {
    val v1Json = "{\"fieldList\":[{\"fieldName\":\"utime\",\"unique\":true,\"fieldPath\":\"$.utime\",\"contentType\":\"\",\"mask\":false},{\"fieldName\":\"zuid\",\"unique\":true,\"fieldPath\":\"$.zuid\",\"contentType\":\"\",\"mask\":false},{\"fieldName\":\"appid\",\"unique\":true,\"fieldPath\":\"$.appid\",\"contentType\":\"\",\"mask\":false},{\"fieldName\":\"ugid\",\"unique\":true,\"fieldPath\":\"$.ugid\",\"contentType\":\"\",\"mask\":false},{\"fieldName\":\"ctime\",\"unique\":true,\"fieldPath\":\"$.ctime\",\"contentType\":\"\",\"mask\":false},{\"fieldName\":\"mid\",\"unique\":true,\"fieldPath\":\"$.mid\",\"contentType\":\"\",\"mask\":false}],\"filterConf\":[{\"$.mid\":\"ACQ01\"}],\"mainConf\":{\"filePath\":\"/apps/logs/raw/sea\",\"unique\":true,\"fileType\":\"gz\",\"pack\":true}}"

    val v1Conf = CommonUtils.mapper.readValue[TaskConf](v1Json)
    val mainConf2 = new MainConf2(v1Conf.mainConf.fileType, v1Conf.mainConf.filePath)
    val fieldConf2Array = new ArrayBuffer[FieldConf2]()
    val maskConf2Array = new ArrayBuffer[MaskConf2]()
    v1Conf.fieldList.foreach { field =>
      val conf2 = clearField(field.fieldName, field.fieldPath)
      if (conf2 != null) {
        fieldConf2Array += conf2
      }

      if (field.mask) {
        maskConf2Array += new MaskConf2(field.fieldPath, field.contentType)
      }
    }
    //capybara_0001

    // TODO packList 在v2需要特殊配置
    val packList = new ArrayBuffer[String]()
    if (v1Conf.mainConf.pack) {
      packList += "$..*"
    }

    packList += "$..*"

    //    packList += "$.*"
    //    packList += "$.photosModels[*].*"
    //    packList += "$..place..*"
    //    packList += "$..likes..*"


    //    packList += "$.orderBeans.*"
    //    packList += "$..shoppingAddress..*"

    //    packList += "$..*"

    //    packList += "$.orderDetailInfos.*"
    //    packList += "$..buyerAddress.*"

//    maskConf2Array += new MaskConf2("$..inputIdcardNo", "idcard")
//    maskConf2Array += new MaskConf2("$..phone", "phone")
//
//
//    //maskConf2Array += new MaskConf2("$..phoneNo", "phone")
//    maskConf2Array += new MaskConf2("$..realName", "real_name") //
//    maskConf2Array += new MaskConf2("$..fullName", "real_name") //
//    maskConf2Array += new MaskConf2("$..nationalId", "idcard") //
//    maskConf2Array += new MaskConf2("$..passportNo", "passportNo") //

    //    maskConf2Array += new MaskConf2("$..bankCardNo", "idcard") //mobile
//    maskConf2Array += new MaskConf2("$..contact_name", "real_name")
//    maskConf2Array += new MaskConf2("$..contacts_mobile", "phone")


    // maskConf2Array += new MaskConf2("$..realName", "real_name")

    //
    // maskConf2Array += new MaskConf2("$..email", "email")
    // maskConf2Array += new MaskConf2("$..idcardNo", "idcard")

    // maskConf2Array += new MaskConf2("$..realName", "real_name")
    // maskConf2Array += new MaskConf2("$..idcardNo", "idcard")
    //   maskConf2Array += new MaskConf2("$..inputIdcardNo", "idcard")
    //
    //maskConf2Array += new MaskConf2("$..phone", "phone")

    // maskConf2Array += new MaskConf2("$..realName", "real_name")


    val v2Conf = new TaskConf2(mainConf2, v1Conf.filterConf, fieldConf2Array.toArray, packList.toArray, maskConf2Array.toArray)
    logger.info(CommonUtils.objToJson(v2Conf))
  }

  private def clearField(fieldName: String, fieldPath: String): FieldConf2 = {

    var conf: FieldConf2 = null
    if (fieldName.startsWith("dm")) {
      val name = fieldName.split('.')(1)
      val path = fieldPath.split('.').last
      if (!CommonUtils.upperToUnderLine(path).equals(name)) {
        conf = new FieldConf2(fieldName, fieldPath)
      }
    } else {
      conf = new FieldConf2(fieldName, fieldPath)
    }

    conf
  }

}
