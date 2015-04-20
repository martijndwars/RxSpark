name := "RxSpark"

version := "1.0"

scalaVersion := "2.10.4"

resolvers += Resolver.mavenLocal

libraryDependencies += "io.reactivex" % "rxscala_2.10" % "0.23.1"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.2.1"

libraryDependencies += "nl.tudelft.spark" % "spark-streaming_2.10" % "1.4.0-SNAPSHOT"

libraryDependencies += "org.twitter4j" % "twitter4j-stream" % "4.0.3"
