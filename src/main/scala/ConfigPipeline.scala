
import java.io.{BufferedWriter, File, FileWriter}

import database._
import input.{NTripleParser, RDFGraphParser}
import org.apache.spark.{SparkConf, SparkContext}
import schema.SchemaElement

import scala.collection.mutable

class ConfigPipeline(config: MyConfig) {

  var timeLoadingData = 0L
  var timeParsingData = 0L
  var timeSummarizeData = 0L
  var timeAggregateSummaries = 0L
  var timeWriteSummaries = 0L
  var timeDeleteSummaries = 0L


  //    create tmp directory
  val logDir: File = new File(config.getString(config.VARS.spark_log_dir))
  if (!logDir.exists())
    logDir.mkdirs()

  var maxMemory = "1g"
  if (config.getString(config.VARS.spark_memory) != null)
    maxMemory = config.getString(config.VARS.spark_memory)

  //delete output directory
  val conf = new SparkConf().setAppName(config.getString(config.VARS.spark_name)).
    setMaster(config.getString(config.VARS.spark_master)).
    set("spark.eventLog.enabled", "true").
    set("spark.eventLog.dir", config.getString(config.VARS.spark_log_dir)).
    set("spark.driver.memory", maxMemory).
    set("spark.executor.memory", maxMemory)


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
      var tmpTimestamp = System.currentTimeMillis()
      val edges = sc.textFile(inputFile).filter(line => !line.trim.isEmpty).map(line => NTripleParser.parse(line))
      timeLoadingData = System.currentTimeMillis() - tmpTimestamp
      println(s"Loaded data graph in $timeLoadingData ms")
      //build graph from vertices and edges from edges
      tmpTimestamp = System.currentTimeMillis()
      val graph = RDFGraphParser.parse(edges)
      timeParsingData = System.currentTimeMillis() - tmpTimestamp
      println(s"Parsed data graph in $timeParsingData ms")

      val schemaExtraction = config.INDEX_MODELS.get(config.getString(config.VARS.schema_indexModel))
      //Schema Summarization:
      tmpTimestamp = System.currentTimeMillis()
      val schemaElements = graph.aggregateMessages[(Int, mutable.HashSet[SchemaElement])](
        triplet => schemaExtraction.sendMessage(triplet),
        (a, b) => schemaExtraction.mergeMessage(a, b))
      timeSummarizeData = System.currentTimeMillis() - tmpTimestamp
      println(s"Build summaries in $timeSummarizeData ms")


      //merge all instances with same schema
      tmpTimestamp = System.currentTimeMillis()
      val aggregatedSchemaElements = schemaElements.values.reduceByKey(_ ++ _)
      timeAggregateSummaries = System.currentTimeMillis() - tmpTimestamp
      println(s"Aggregated summaries in $timeAggregateSummaries ms")

      //  (incremental) writing
      tmpTimestamp = System.currentTimeMillis()
      schemaElements.map(tuple => igsi.tryAdd(tuple._2._2)).collect()
      timeWriteSummaries = System.currentTimeMillis() - tmpTimestamp
      println(s"Written new summaries/instances in $timeWriteSummaries ms")


      //TODO: parallelize?
      tmpTimestamp = System.currentTimeMillis()
      OrientDb.getInstance(database, trackChanges).removeOldImprintsAndElements(startTime)
      timeDeleteSummaries = System.currentTimeMillis() - tmpTimestamp
      println(s"Deleted old summaries/instances in $timeDeleteSummaries ms")

      sc.stop
      if (trackChanges) {
        OrientDb.getInstance(database, trackChanges)._changeTracker.exportToCSV(logChangesDir + "/changes.csv", iteration)
        export(logChangesDir + "/performance.csv", iteration)
      }
      iteration += 1
    }

    OrientDb.getInstance(database, trackChanges)._changeTracker
  }

  def export(filename: String, iteration: Int): Unit = {
    val file = new File(filename)
    val delimiter = ';'

    val header = Array[String]("Iteration", "timeLoadingData (ms)", "timeParsingData (ms)", "timeSummarizeData (ms)",
      "timeAggregateSummaries (ms)", "timeWriteSummaries (ms)", "timeDeleteSummaries (ms)", "totalTimeUpdate (ms)", "totalTime (ms)")

    val writer = new BufferedWriter(new FileWriter(file, iteration > 0))
    if (iteration <= 0) { //write headers
      var i = 0
      while (i < header.length - 1) {
        writer.write(header(i) + delimiter)
        i += 1
      }
      writer.write(header(header.length - 1))
      writer.newLine()
    }
    val contentLine: String = iteration.toString + delimiter + timeLoadingData.toString + delimiter +
      timeParsingData.toString + delimiter + timeSummarizeData.toString + delimiter + timeAggregateSummaries.toString + delimiter +
      timeWriteSummaries.toString + delimiter + timeDeleteSummaries.toString + delimiter +
      (timeWriteSummaries + timeDeleteSummaries).toString + delimiter +
      (timeLoadingData + timeParsingData + timeSummarizeData + timeAggregateSummaries + timeWriteSummaries + timeDeleteSummaries).toString

    writer.write(contentLine)
    writer.newLine()
    writer.close()
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
