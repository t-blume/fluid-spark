name := "fluid-spark"

version := "1.3"

scalaVersion := "2.12.8"


parallelExecution in Test := false

Compile/mainClass := Some("Main")

resolvers += "Bintray" at "https://dl.bintray.com/sbcd90/org.apache.spark/"


libraryDependencies += "com.googlecode.json-simple" % "json-simple" % "1.1.1"
libraryDependencies += "com.typesafe" % "config" % "1.3.4"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.3"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.4.3"
libraryDependencies += "com.orientechnologies" % "orientdb-graphdb" % "3.0.18"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.3"

libraryDependencies += "com.novocode" % "junit-interface" % "0.10" % "test"
// http://mvnrepository.com/artifact/junit/junit
libraryDependencies += "junit" % "junit" % "4.10"
