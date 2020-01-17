name := "fluid-spark"

version := "1.1"

scalaVersion := "2.12.8"



resolvers += "Bintray" at "https://dl.bintray.com/sbcd90/org.apache.spark/"



//libraryDependencies += "org.scala-lang" % "scala-library" % "2.12.8"
//libraryDependencies += "org.scala-lang" % "scala-xml" % "2.12.8"
libraryDependencies += "com.googlecode.json-simple" % "json-simple" % "1.1.1"
libraryDependencies += "com.typesafe" % "config" % "1.3.4"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.3"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.4.3"
libraryDependencies += "com.orientechnologies" % "orientdb-graphdb" % "3.0.18"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.3"
//libraryDependencies += "org.apache.spark" %% "spark-orientdb-2.2.1_2.11" % "1.4"
//libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.3"