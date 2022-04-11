package com.neu.edu.FlightPricePrediction.utils

import org.slf4j.{Logger, LoggerFactory}
import org.zeroturnaround.zip.{ZipUtil}

import java.io.File
/**
 * @author Caspar
 * @date 2022/4/9 00:53 
 */
object FileUtil{
  val logger: Logger = LoggerFactory.getLogger(getClass.getSimpleName)

  def unzip(source: String, destination: String) = {
    logger.info(s"Start to unzip $source to $destination")
    ZipUtil.unpack(new File(source), new File(destination))
    logger.info(s"Succeed to unzip $source to $destination")
  }

  def zip(source: String, destination: String) = {
    logger.info(s"Start to zip $source to $destination")
    ZipUtil.pack(new File(source), new File(destination))
    logger.info(s"Succeed to zip $source to $destination")
  }

}
