import org.apache.commons.io.FileUtils
import play.api.Logger

import java.io.File
import java.nio.file.{Files, Path, Paths}
import scala.collection.JavaConverters.asScalaIteratorConverter
import collection.JavaConversions._

val topic = "WECC/SAQ200/861108035980803/sensor"
val topic1 = "861108035980803/sensor"
val pattern = "WECC/SAQ200/([0-9]+)/sensor".r
val pattern1 = "WECC/SAQ200/([0-9]+)/.*".r
val pattern1(a) =  topic

topic.replace("^\\.+", "").replaceAll("[\\\\/:*?\"<>|]", "")