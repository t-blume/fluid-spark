
import java.io.File

import database._
import input.{NTripleParser, RDFGraphParser}
import org.apache.spark.graphx.{Graph, VertexRDD}
import org.apache.spark.{SparkConf, SparkContext}
import schema.{SchemaElement, SchemaExtraction}

import scala.collection.mutable

class ConfigPipeline(config: MyConfig) {
  //    create tmp directory
  val logDir: File = new File(config.getString(config.VARS.spark_log_dir))
  if (!logDir.exists())
    logDir.mkdirs()

  //delete output directory
  val conf = new SparkConf().setAppName(config.getString(config.VARS.spark_name)).
    setMaster(config.getString(config.VARS.spark_master)).
    set("spark.eventLog.enabled", "true").
    set("spark.eventLog.dir", config.getString(config.VARS.spark_log_dir))

  val sc = new SparkContext(conf)

  val inputFile = config.getString(config.VARS.input_filename)
  Constants.TYPE = config.getString(config.VARS.input_graphLabel)


  //
  //    //OUT
  OrientDb.URL = config.getString(config.VARS.db_url)
  OrientDb.USERNAME = config.getString(config.VARS.db_user)
  OrientDb.PASSWORD = config.getString(config.VARS.db_password)

  val database = config.getString(config.VARS.db_name)
  OrientDb.create(database, config.getBoolean(config.VARS.igsi_batch_computation))
  val trackChanges = config.getBoolean(config.VARS.igsi_batch_computation)

  val igsi: IGSI = new IGSI(database, trackChanges)


  def start(waitBefore: Long, waitAfter:Long): ChangeTracker = {
    if(waitBefore > 0){
      println("waiting for " + waitBefore + " ms")
      Thread.sleep(waitBefore)
      println("...continuing!")
    }
    val startTime = Constants.NOW()
    if(waitAfter > 0){
      println("waiting for " + waitAfter + " ms")
      Thread.sleep(waitAfter)
      println("...continuing!")
    }

    //parse n-triple file to RDD of GraphX Edges
    val edges = sc.textFile(inputFile).filter(line => !line.isBlank).map(line => NTripleParser.parse(line))
    //build _graph from vertices and edges from edges
    val graph: Graph[Set[(String, String)], (String, String, String, String)] = RDFGraphParser.parse(edges)

    val schemaExtraction: SchemaExtraction = config.INDEX_MODELS.get(config.getString(config.VARS.schema_indexModel))

    /*
    Schema Summarization:
     */
    val schemaElements: VertexRDD[SchemaElement] = graph.aggregateMessages[SchemaElement](
      triplet => schemaExtraction.sendMessage(triplet),
      (a, b) => schemaExtraction.mergeMessage(a, b))
    /*
    (incremental) writing
     */
    //    schemaElements.map(x => (x._2.getID, mutable.HashSet(x._2))).reduceByKey(_ ++ _).collect().foreach(S => println(S))
    schemaElements.map(x => (x._2.getID, mutable.HashSet(x._2))).reduceByKey(_ ++ _).collect().foreach(tuple => igsi.tryAdd(tuple._2))

    println("Cleanup stage")
    //TODO execute on spark
    OrientDb.getInstance(database, trackChanges).removeOldImprintsAndElements(startTime)

    sc.stop
    OrientDb.getInstance(database, trackChanges)._changeTracker
  }

}
object Main {
  def main(args: Array[String]) {

    // this can be set into the JVM environment variables, you can easily find it on google
    if (args.isEmpty) {
      println("Need config file")
      return
    } else
      println("Conf:" + args(0))

    val pipeline: ConfigPipeline = new ConfigPipeline(new MyConfig(args(0)))

    //recommended to wait 1sec after timestamp since time is measured in seconds (not ms)
    pipeline.start(0, 1000)

  }
}