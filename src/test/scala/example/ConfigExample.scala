package example

import com.typesafe.config.ConfigFactory
import org.slf4j.{Logger, LoggerFactory}

object ConfigExample {

  val logger: Logger = LoggerFactory.getLogger(getClass.getSimpleName)


  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load("application.conf").getConfig("com.ram.batch")
    val sparkConfig = config.getConfig("spark")
    val mysqlConfig = config.getConfig("mysql")
    val appName = sparkConfig.getString("app-name")
    logger.info(appName)
    logger.info(mysqlConfig.toString)
  }

}
