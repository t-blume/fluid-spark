
import java.io.File

import classes.{GraphSiloScala, IGSI, MyConfig, SchemaElement}
import database.{Constants, OrientDb}
import org.apache.spark.graphx.{Graph, VertexRDD}
import org.apache.spark.{SparkConf, SparkContext}
import schema.ChangeTracker

import scala.collection.mutable

object TestingGrounds {
  def main(args: Array[String]) {
    val startTime = Constants.NOW()
    Thread.sleep(1000)
    // this can be set into the JVM environment variables, you can easily find it on google
    if(args.isEmpty){
      println("Need config file")
      return
    }else
      println("Conf:" + args(0))
    val config :  MyConfig = new MyConfig(args(0))



    //    create tmp directory
    val logDir: File = new File(config.getString(config.VARS.spark_log_dir))
    if(!logDir.exists())
      logDir.mkdirs()

    //delete output directory
    val conf = new SparkConf().setAppName(config.getString(config.VARS.spark_name)).
      setMaster(config.getString(config.VARS.spark_master)).
      set("spark.eventLog.enabled", "true").
      set("spark.eventLog.dir", config.getString(config.VARS.spark_log_dir))

    val sc = new SparkContext(conf)

    val inputFile = config.getString(config.VARS.input_filename)
    Constants.TYPE = config.getString(config.VARS.input_graphLabel)

    val graphSilo: GraphSiloScala= new GraphSiloScala()
    val igsi: IGSI = new IGSI()

//
//    //OUT
    OrientDb.create(config.getString(config.VARS.db_url),
      config.getString(config.VARS.db_name),
      config.getString(config.VARS.db_user),
      config.getString(config.VARS.db_password),
      config.getBoolean(config.VARS.igsi_batch_computation))

    //parse n-triple file to RDD of GraphX Edges
    val edges = sc.textFile(inputFile).map(line => NTripleParser.parse(line))
    //build graph from vertices and edges from edges
    val graph: Graph[Set[(String, String)], (String, String, String, String)] = RDFGraphParser.parse(edges);




    /*
    Schema Summarization:
     */
    val schemaElements: VertexRDD[SchemaElement] = graph.aggregateMessages[SchemaElement](
      triplet => { // Map Function
        // Send message to destination vertex containing types and property
        val srcElement = new SchemaElement
        val dstElement = new SchemaElement

        //get origin types
        if (triplet.srcAttr != null)
          for ((a, _) <- triplet.srcAttr)
            srcElement.label.add(a)

        //get dst types
        if (triplet.dstAttr != null)
          for ((a, _) <- triplet.dstAttr)
            dstElement.label.add(a)

        //add neighbor element connected over this property
        srcElement.neighbors.put(triplet.attr._2, dstElement)
        //add datasource/source graph as payload
        srcElement.payload.add(triplet.attr._4)
        //add src vertex as instance
        srcElement.instances.add(triplet.attr._1)
        triplet.sendToSrc(srcElement)
      },
      // Add counter and age

      (a, b) => {
        a.merge(b)
        a
      } // Reduce Function
    )

    /*
    (incremental) writing
     */
//    schemaElements.map(x => (x._2.getID, mutable.HashSet(x._2))).reduceByKey(_ ++ _).collect().foreach(S => println(S))
    schemaElements.map(x => (x._2.getID, mutable.HashSet(x._2))).reduceByKey(_ ++ _).collect().foreach(tuple => igsi.tryAdd(tuple._2))

    sc.stop

    OrientDb.getInstance().removeOldImprints(startTime)

    print(ChangeTracker.pprintSimple())
  }


}
