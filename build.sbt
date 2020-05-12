name := "JoynStreamingApp"

version := "0.1"

scalaVersion := "2.12.4"

libraryDependencies += "org.apache.kafka" %% "kafka" % "2.5.0"
libraryDependencies += "org.apache.kafka" % "kafka-streams" % "2.5.0"
libraryDependencies += "com.googlecode.json-simple" % "json-simple" % "1.1.1"
libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-api" % "1.7.25",
  "ch.qos.logback" % "logback-classic" % "1.2.3"
)