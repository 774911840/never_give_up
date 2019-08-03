package com.weshare.marlin.constant

final case class CustomException(message: String = "",
                                 cause: Throwable = None.orNull)
  extends Exception(message, cause) {

}
