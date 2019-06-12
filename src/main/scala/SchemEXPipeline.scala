import java.io.File

import database.OrientDb
import graph.Edge
import org.apache.spark.{SparkConf, SparkContext}
import schema.{ISchemaElement, SchemEX}
import org.apache.spark.graphx.Graph
import scala.collection.JavaConverters._

object SchemEXPipeline {


  def main(args: Array[String]): Unit = {

    //    create tmp directory
    val logDir: File = new File("/tmp/spark-events")
    if (!logDir.exists())
      logDir.mkdirs();

    //delete output directory
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[1]").set("spark.eventLog.enabled", "true").set("spark.eventLog.dir", "/tmp/spark-events")
    val sc = new SparkContext(conf)


    //TODO: JUnits for lower case, backslashes, fragments etc.

    val graphSilo: GraphSiloScala = new GraphSiloScala()
    val igsi: IGSIScalaNew = new IGSIScalaNew()

    //IN
    //    val inputFile = "/home/till/data/2012-05-06/data-500k.nq.gz"
    val inputFile = "resources/timbl-500.nq"

    //OUT
    OrientDb.create("remote:localhost", "schemex-test", "root", "rootpwd", true)


    //    //SchemEX Pipeline
    //    val superdruper = sc.textFile(inputFile).map(line => Edge.fromNQuad(line)).map(x => (x.start, Set(x))).reduceByKey(_ ++ _).
    //      map(item => SchemEX.fromInstance(item._2.asJava)).map(SE => {
    //      if(SE.getDependencies != null)
    //        (SE.getDependencies.iterator().next(), Set(SE))
    //      else ("", Set(SE))
    //    }).reduceByKey(_++_)


    //SchemEX Pipeline
    val superdruper = sc.textFile(inputFile).map(line => Edge.fromNQuad(line)).map(x => (x.start, Set(x))).reduceByKey(_ ++ _).
      map(item => SchemEX.fromInstance(item._2.asJava)).flatMap(SE => {
      if (SE.getDependencies != null)
        helper(SE.getDependencies.asScala.iterator, SE).iterator
      else Set(("", Set(SE))).iterator
    }).reduceByKey(_ ++ _).collect().foreach(X => print(X))

    //TODO: get for each locator (key) the computed schema element an add it there

    print(superdruper)

    //      .reduce((d, SEs) => )
    //
    //      .map(SE => SchemEX.combine(SE, ))
    //
    //      .map(x => (x.getID, Set(x))).reduceByKey(_ ++ _).collect().foreach(tuple => (igsi.tryAdd(tuple._2)))


  }


  def helper(iterator: Iterator[String], schemaElement: ISchemaElement): Set[(String, Set[ISchemaElement])] = {
    val empty: String = ""
    val t = schemaElement.getDependencies

    if (t.isEmpty)
      return Set((empty, Set(schemaElement)))
    else {
      var res = Set[(String, Set[ISchemaElement])]()
      val it = t.iterator()
      while (it.hasNext) {
        val d = it.next()
        res += new Tuple2(d, Set(schemaElement))
      }
      return res
    }
  }
}
