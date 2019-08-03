package com.weshare.marlin.auto

import com.weshare.marlin.constant.CommonConstant.MaskContentType._
import com.weshare.marlin.utils.CommonUtils.cleanPhoneCN
import com.weshare.marlin.utils.MaskTools._

/**
  * Created by xuyi on 2018/9/3 10:38.
  */
object MaskMain {

  /**
    *
    * @param contentType
    * @param fieldName
    * @param text
    * @return
    */
  def maskCube(contentType: String, fieldName: String, text: String): Array[(String, String)] = {
    contentType match {
      case t if Array(PHONE, EMAIL, BANK_CARD, REAL_NAME).contains(t) => baseMask(fieldName, text)
      case PHONE_CN => cnPhoneMask(fieldName, text)
      case IDCARD => transIdcard(fieldName, text.toUpperCase)
      case _ => baseMask(fieldName, text)
    }
  }

  def cnPhoneMask(fieldName: String, text: String): Array[(String, String)] = {
    val baseArray = baseMask(fieldName, text)
    val cleanArray = cleanMask(fieldName, cleanPhoneCN(text))

    baseArray ++ cleanArray
  }
  def transIdcard(fieldName: String, text: String):Array[(String, String)] ={
    val res = new Array[(String, String)](2)
    if (text!=""){
      val substring1=text.substring(0,text.length-12)
      val substring2=text.substring(text.length-12);
      val substring3=text.substring(text.length-4,text.length-1);
      val char=substring1.toCharArray
      var s=new StringBuilder
      for(i<-char){
        val temp=(Integer.valueOf(i)^Integer.valueOf(substring3))%10
        s.append(temp)
      }
      s.append(substring2)
      res(0)=(fieldName,s.toString())
    }
    else{
      res(0)=(fieldName,text)
    }
    var reversibleFieldName: String = null
    if (fieldName.contains('.')) {
      reversibleFieldName = fieldName + "_reversible"
    } else {
      reversibleFieldName = "dm." + fieldName + "_reversible"
    }

    res(1) = (reversibleFieldName, bigAES(text))
    res
  }



  private def baseMask(fieldName: String, text: String): Array[(String, String)] = {
    val res = new Array[(String, String)](2)
    res(0) = (fieldName, bigMD5(text))
    //reversible

    var reversibleFieldName: String = null
    if (fieldName.contains('.')) {
      reversibleFieldName = fieldName + "_reversible"
    } else {
      reversibleFieldName = "dm." + fieldName + "_reversible"
    }

    res(1) = (reversibleFieldName, bigAES(text))

    res
  }

  private def cleanMask(fieldName: String, text: String): Array[(String, String)] = {
    var cleanFieldName: String = null
    var cleanReversibleFieldName: String = null
    if (fieldName.contains('.')) {
      cleanFieldName = fieldName + "_clean"
      cleanReversibleFieldName = fieldName + "_clean_reversible"
    } else {
      cleanFieldName = "dm." + fieldName + "_clean"
      cleanReversibleFieldName = "dm." + fieldName + "_clean_reversible"
    }

    Array((cleanFieldName, bigMD5(text)), (cleanReversibleFieldName, bigAES(text)))
  }
}
