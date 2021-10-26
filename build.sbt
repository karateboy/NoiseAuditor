name := """NoiseAuditor"""

version := "1.2.2"

lazy val root = (project in file(".")).enablePlugins(PlayScala, LauncherJarPlugin)

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  ws,
  filters,
  "com.github.tototoshi" %% "scala-csv" % "1.3.5"
)

// https://mvnrepository.com/artifact/org.mongodb.scala/mongo-scala-driver
libraryDependencies += "org.mongodb.scala" %% "mongo-scala-driver" % "4.2.3"

// https://mvnrepository.com/artifact/com.github.nscala-time/nscala-time
libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "2.26.0"

// https://mvnrepository.com/artifact/org.apache.poi/poi-ooxml
libraryDependencies += "org.apache.poi" % "poi-ooxml" % "5.0.0"

// https://mvnrepository.com/artifact/commons-io/commons-io
libraryDependencies += "commons-io" % "commons-io" % "2.11.0"

// https://mvnrepository.com/artifact/com.github.albfernandez/javadbf
libraryDependencies += "com.github.albfernandez" % "javadbf" % "1.13.2"

routesGenerator := StaticRoutesGenerator

mappings in Universal ++=
(baseDirectory.value / "report_template" * "*" get) map
    (x => x -> ("report_template/" + x.getName))

mappings in Universal ++=
(baseDirectory.value / "importEPA" * "*" get) map
    (x => x -> ("importEPA/" + x.getName))
	
 	
//libraryDependencies += "com.google.guava" % "guava" % "19.0"
scalacOptions += "-feature"
resolvers += "scalaz-bintray" at "http://dl.bintray.com/scalaz/releases"

routesGenerator := InjectedRoutesGenerator