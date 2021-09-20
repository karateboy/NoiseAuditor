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

val str= "\02"
val v = str.getBytes
print(v(0))
val filter= Array("dbf")

def getDbfList(dir: String, relativePath:String) = {
  val paths: List[Path] = Files.list(Paths.get(dir, relativePath)).iterator()
    .asScala.toList
  println(paths.map(p=>p.toFile.getAbsolutePath))
  val list = Files.list(paths(0)).iterator().asScala.map(p=>p.toFile.getName).toList
  println(s"$dir ${paths(1)}")
  println(list.toString())
}

getDbfList("E:\\Temp\\110Y1Q_airport10v11", "每秒風速監測資料")