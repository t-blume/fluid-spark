import graph.Edge
import org.apache.spark.{SparkConf, SparkContext}
import schema.SchemaElement

import scala.collection.JavaConverters._

object Pipeline {


  def main(args: Array[String]): Unit = {

//    create tmp directory
    //delete output directory
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[4]").set("spark.eventLog.enabled", "true").set("spark.eventLog.dir", "/tmp/spark-events")
    val sc = new SparkContext(conf)

    val data = sc.textFile("resources/timbl-500.nq")
    //TODO: JUnits for lower case, backslashes, fragments etc.


    sc.textFile("resources/timbl-500.nq").map(line => Edge.fromNQuad(line)).map(x => (x.start, Set(x))).reduceByKey(_ ++ _).
      map(item => SchemaElement.fromInstance(item._2.asJava)).map(x => (x.id, Set(x))).reduceByKey(_ ++ _).saveAsTextFile("resources/schema")




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
    //        a._2.asInstanceOf[SchemaElement].sources.addAll(b._2.asInstanceOf[SchemaElement].sources)
    //        return a._2
    //      }
    //    })
//
//    items.map(item => SchemaElement.fromInstance(item._2.asJava)).map(x => (x.id, Set(x))).reduceByKey(_ ++ _).collect()
//
//    items.map(item => SchemaFactory.compute(item._2)).map(x => (x.id, Set(x))).reduceByKey(_ ++ _).collect()
////      .map(x => (x.start, Set(x))).reduceByKey(_ ++ _).collect()
//
//
//    val schemaItems = items.map(item => schemaComputation.compute(item._2.asJava)).map(schemaItem => {(schemaItem.asInstanceOf[SchemaElement].id,
//      Set(schemaItem))})


//        for (item <- schemaItems) {
//          println(item._1 + ": ")
//          item._2.foreach(x => println(x))
//          println("________________")
//        }


  }


}
