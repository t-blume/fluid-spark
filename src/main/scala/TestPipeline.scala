import java.io.File

import classes.{GraphSiloScala, IGSI}
import database.{ChangeTracker, Constants, OrientDb}
import graph.Edge
import org.apache.spark.{SparkConf, SparkContext}
import schema.TypesAndProperties

import scala.collection.JavaConverters._
import scala.collection.mutable

object TestPipeline {


  def main(args: Array[String]): Unit = {
    val startTime = Constants.NOW()
    Thread.sleep(1000)
    //    create tmp directory
    val logDir: File = new File("/tmp/spark-events")
    if(!logDir.exists())
      logDir.mkdirs();

    //delete output directory
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[1]").set("spark.eventLog.enabled", "true").set("spark.eventLog.dir", "/tmp/spark-events")
    val sc = new SparkContext(conf)


    //TODO: JUnits for lower case, backslashes, fragments etc.

    val graphSilo: GraphSiloScala= new GraphSiloScala()
    val igsi: IGSI = new IGSI()

    //IN
//    val inputFile = "/home/till/data/2012-05-06/data-500k.nq.gz"
//    val inputFile = "resources/manual-test-3.nq"
    val inputFile = "resources/timbl-test-3.nq"

    //OUT
    OrientDb.create("remote:localhost", "timbl-test-incremental", "root", "rootpwd", false)



    //TODO: FIX parallel updates
//    sc.textFile(inputFile).map(line => Edge.fromNQuad(line)).map(x => (x.start, Set(x))).reduceByKey(_ ++ _).
//      map(item => TypesAndProperties.fromInstance(item._2.asJava)).map(x => (x.getID, Set(x))).reduceByKey(_ ++ _).map(tuple => (igsi.tryAdd(tuple._2))).collect()

    sc.textFile(inputFile).
      map(line => Edge.fromNQuad(line)).

      map(x => (x.start, Set(x))).
      reduceByKey(_ ++ _).
      map(item => TypesAndProperties.fromInstance(item._2.asJava)).
      map(x => (x.getID, mutable.HashSet(x))).
      reduceByKey(_ ++ _).//foreach(x => println(x))
      collect().
      foreach(tuple => (println(tuple._2)))

    sc.stop()

    OrientDb.getInstance().removeOldImprintsAndElements(startTime)

    print(ChangeTracker.pprintSimple())


    //SchemEX Pipeline
//    sc.textFile(inputFile).map(line => Edge.fromNQuad(line)).map(x => (x.start, Set(x))).reduceByKey(_ ++ _).
//      map(item => SchemEX.fromInstance(item._2.asJava)).map(x => (x.getID, Set(x))).reduceByKey(_ ++ _).collect().foreach(tuple => (igsi.tryAdd(tuple._2)))

      //.saveAsTextFile("resources/schema")


    sc.stop()


    //    for (item <- items) {
    //      println(item._1)
    //      item._2.foreach(x => println(x))
    //      println("________________")
    //    }

    //    for (item <- items) {
    //      println(item._1 + ": ")
    //      item._2.foreach(x => println("\t" + x))
    //    }
    // TODO: internal int datatype? convert uri to hash?
    //compute schema
    //    val schemaItems = items.map(item => schemaComputation.compute(item._2.asJava)).reduce((a, b) => {
    //      if (a._1.eq(b._2)) {
    //        a._2.asInstanceOf[classes.SchemaElement].sources.addAll(b._2.asInstanceOf[classes.SchemaElement].sources)
    //        return a._2
    //      }
    //    })
    //
    //    items.map(item => classes.SchemaElement.fromInstance(item._2.asJava)).map(x => (x.id, Set(x))).reduceByKey(_ ++ _).collect()
    //
    //    items.map(item => SchemaFactory.compute(item._2)).map(x => (x.id, Set(x))).reduceByKey(_ ++ _).collect()
    ////      .map(x => (x.start, Set(x))).reduceByKey(_ ++ _).collect()
    //
    //
    //    val schemaItems = items.map(item => schemaComputation.compute(item._2.asJava)).map(schemaItem => {(schemaItem.asInstanceOf[classes.SchemaElement].id,
    //      Set(schemaItem))})


    //        for (item <- schemaItems) {
    //          println(item._1 + ": ")
    //          item._2.foreach(x => println(x))
    //          println("________________")
    //        }


  }


}
