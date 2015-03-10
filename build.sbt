name := "RxSpark"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.2.1"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.2.1"

libraryDependencies += "org.apache.spark" % "spark-streaming-twitter_2.10" % "1.2.1"

libraryDependencies += "io.reactivex" % "rxscala_2.10" % "0.23.1"
