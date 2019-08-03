package com.weshare.marlin.logging

/**
  * Created by xuyi on 2018/8/29 20:45.
  */
trait WeshareLogging {
  protected lazy val logger: WeshareLogger = WeshareLogger()
}
