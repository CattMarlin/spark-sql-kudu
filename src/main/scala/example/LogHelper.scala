package example

import org.apache.log4j.Logger

trait LogHelper {
  lazy val log = Logger.getLogger(this.getClass.getName)
}
