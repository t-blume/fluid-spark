name := "fluid-spark"

version := "1.1"

scalaVersion := "2.11.12"



//resolvers += "Bintray" at "https://dl.bintray.com/sbcd90/org.apache.spark/"


//libraryDependencies += "org.scala-lang" % "scala-library" % "2.11.8"
//libraryDependencies += "org.scala-lang,modules" %% "scala-xml" % "2.11.0-M4"
libraryDependencies += "com.googlecode.json-simple" % "json-simple" % "1.1.1"
libraryDependencies += "com.typesafe" % "config" % "1.3.4"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.3"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.4.3"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.3"
libraryDependencies += "com.arangodb" % "arangodb-spark-connector" % "1.0.2"
//libraryDependencies += "org.arangodb" % "arangodb-tinkerpop-provider" % "2.0.2"