
import java.io.File

import database._
import input.{NTripleParser, RDFGraphParser}
import org.apache.spark.{SparkConf, SparkContext}
import schema.SchemaElement

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


  val inputFiles: java.util.List[String] = config.getStringList(config.VARS.input_filename)
  Constants.TYPE = config.getString(config.VARS.input_graphLabel)

//  var namespace = "http://informatik.uni-kiel.de/fluid#"
  if (config.getString(config.VARS.input_namespace) != null)
    NTripleParser.baseURI = config.getString(config.VARS.input_namespace)

//  var defaultSource = "http://informatik.uni-kiel.de/fluid"
  if (config.getString(config.VARS.input_defaultSource) != null)
    NTripleParser.defaultSource = config.getString(config.VARS.input_defaultSource)

//  val parser = new NTripleParser(namespace, defaultSource)


  //
  //    //OUT
  OrientDb.URL = config.getString(config.VARS.db_url)
  OrientDb.USERNAME = config.getString(config.VARS.db_user)
  OrientDb.PASSWORD = config.getString(config.VARS.db_password)

  val database = config.getString(config.VARS.db_name)
  val trackChanges = config.getBoolean(config.VARS.igsi_trackChanges)

  var logChangesDir: String = null
  if (trackChanges == true) {
    logChangesDir = config.getString(config.VARS.igsi_logChangesDir)
    val file: File = new File(logChangesDir)
    if (!file.exists) file.mkdirs
  }

  val minWait = config.getLong(config.VARS.igsi_minWait)


  def start(): ChangeTracker = {


    var iteration = 0

    val iterator: java.util.Iterator[String] = inputFiles.iterator()
    while (iterator.hasNext) {

      if (iteration == 0)
        OrientDb.create(database, config.getBoolean(config.VARS.igsi_clearRepo))
      else if (trackChanges)
        OrientDb.getInstance(database, trackChanges)._changeTracker.resetScores()

      if (minWait > 0) {
        println("waiting for " + minWait + " ms")
        Thread.sleep(minWait)
        println("...continuing!")
      }
      val startTime = Constants.NOW()
      if (minWait > 0) {
        println("waiting for " + minWait + " ms")
        Thread.sleep(minWait)
        println("...continuing!")
      }

      val sc = new SparkContext(conf)
      val igsi = new IGSI(database, trackChanges)

      val inputFile = iterator.next()

      //parse n-triple file to RDD of GraphX Edges
      val edges = sc.textFile(inputFile).filter(line => !line.trim.isEmpty).map(line => NTripleParser.parse(line))
      //build _graph from vertices and edges from edges
      val graph = RDFGraphParser.parse(edges)

      val schemaExtraction = config.INDEX_MODELS.get(config.getString(config.VARS.schema_indexModel))

      /*
      Schema Summarization:
       */
      val schemaElements = graph.aggregateMessages[SchemaElement](
        triplet => schemaExtraction.sendMessage(triplet),
        (a, b) => schemaExtraction.mergeMessage(a, b))
      /*
      (incremental) writing
       */
      //    schemaElements.map(x => (x._2.getID, mutable.HashSet(x._2))).reduceByKey(_ ++ _).collect().foreach(S => println(S))
      schemaElements.map(x => (x._2.getID, mutable.HashSet(x._2))).
        reduceByKey(_ ++ _).collect().
        foreach(tuple => igsi.tryAdd(tuple._2))

      println("Cleanup stage")
      //TODO execute on spark
      OrientDb.getInstance(database, trackChanges).removeOldImprintsAndElements(startTime)

      sc.stop
      if (trackChanges)
        OrientDb.getInstance(database, trackChanges)._changeTracker.exportToCSV(logChangesDir + "/changes.csv", iteration)
      iteration += 1
    }

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
    pipeline.start()

  }
}