
import java.io.{BufferedWriter, File, FileWriter}

import database._
import input.{NTripleParser, RDFGraphParser}
import org.apache.log4j.LogManager
import org.apache.spark.graphx.PartitionStrategy.RandomVertexCut
import org.apache.spark.{SparkConf, SparkContext}
import schema.SchemaElement

import scala.collection.mutable

class ConfigPipeline(config: MyConfig) {
  //***** mandatory *****//
  val appName = config.getString(config.VARS.spark_name)

  val database = config.getString(config.VARS.db_name)

  val inputFolder = config.getString(config.VARS.input_folder)

  val inputFiles: java.util.List[String] = config.getStringList(config.VARS.input_filename)

  val indexModel = config.INDEX_MODELS.get(config.getString(config.VARS.schema_indexModel))
  RDFGraphParser.useIncoming = config.getBoolean(config.VARS.schema_undirected)

  val trackUpdateTimes = config.getBoolean(config.VARS.igsi_trackUpdateTimes)
  val trackPrimaryChanges = config.getBoolean(config.VARS.igsi_trackPrimaryChanges)
  val trackSecondaryChanges = config.getBoolean(config.VARS.igsi_trackSecondaryChanges)
  //*********************//

  // ------- spark ------- //
  val sparkMaster =
    if (config.exists(config.VARS.spark_master))
      config.getString(config.VARS.spark_master)
    else
      "local[*]"

  val sparkEventDir =
    if (config.exists(config.VARS.spark_log_dir))
      config.getString(config.VARS.spark_log_dir)
    else
      "/tmp/spark-events"

  val maxMemory =
    if (config.exists(config.VARS.spark_memory))
      config.getString(config.VARS.spark_memory)
    else
      "4g"

  val maxCores =
    if (config.exists(config.VARS.spark_cores))
      config.getString(config.VARS.spark_cores)
    else
      "4"

  val minPartitions =
    if (config.exists(config.VARS.spark_partitions))
      config.getInt(config.VARS.spark_partitions)
    else
      4


  // ------- database ------- //
  OrientConnector.URL =
    if (config.exists(config.VARS.db_url))
      config.getString(config.VARS.db_url)
    else
      "remote:localhost"

  OrientConnector.USERNAME =
    if (config.exists(config.VARS.db_user))
      config.getString(config.VARS.db_user)
    else
      "admin"

  OrientConnector.PASSWORD =
    if (config.exists(config.VARS.db_password))
      config.getString(config.VARS.db_password)
    else
      "admin"

  // ------- schema ------- //
  if (config.exists(config.VARS.schema_classSignal))
    RDFGraphParser.classSignal = config.getString(config.VARS.schema_classSignal)


  // ------- parser ------- //
  NTripleParser.baseURI =
    if (config.exists(config.VARS.input_namespace))
      config.getString(config.VARS.input_namespace)
    else
      "http://informatik.uni-kiel.de/fluid#"

  NTripleParser.defaultSource =
    if (config.exists(config.VARS.input_defaultSource))
      config.getString(config.VARS.input_defaultSource)
    else
      "http://informatik.uni-kiel.de"


  val logChangesDir: String =
    if (trackPrimaryChanges || trackSecondaryChanges)
      config.getString(config.VARS.igsi_logChangesDir)
    else
      null


  // ------- other ------- //
  val alsoBatch =
    if (config.exists(config.VARS.igsi_alsoBatch))
      config.getBoolean(config.VARS.igsi_alsoBatch)
    else
      true

  val minWait = if (config.exists(config.VARS.igsi_minWait)) config.getLong(config.VARS.igsi_minWait) else 1000L


  // _______INIT_______ //
  val logger = LogManager.getLogger("ConfigPipeline \"" + appName + "\"")
  val logDir: File = new File(sparkEventDir)
  if (!logDir.exists())
    logDir.mkdirs()

  if (logChangesDir != null) {
    val file: File = new File(logChangesDir)
    if (!file.exists) file.mkdirs
  }

  val conf = new SparkConf().
    setMaster(sparkMaster).
    set("spark.executor.heartbeatInterval", "3600s"). //1h
    set("spark.network.timeout", "50400s"). //14h
    set("spark.eventLog.enabled", "true").
    set("spark.eventLog.dir", sparkEventDir).
    set("spark.driver.memory", maxMemory).
    set("spark.executor.memory", maxMemory).
    set("spark.driver.maxResultSize", "0").
    set("spark.core.max", maxCores).
    set("spark.executor.core", maxCores)


  def sleep() = {
    if (minWait > 0) {
      logger.info("waiting for " + minWait + " ms")
      Thread.sleep(minWait)
      logger.info("...continuing!")
    }
  }

  /**
   * Run all computations
   *
   * @return
   */
  def start(): ChangeTracker = {
    var iteration = 0
    val iterator: java.util.Iterator[String] = inputFiles.iterator()

    val secondaryIndexFile = "secondaryIndex.ser.gz"
    val updateResult: Result[Boolean] = new Result[Boolean](trackUpdateTimes, trackPrimaryChanges || trackSecondaryChanges)

    while (iterator.hasNext) {
      if (trackPrimaryChanges || trackSecondaryChanges)
        updateResult.resetScores()

      if (iteration == 0)
        OrientConnector.create(database, config.getBoolean(config.VARS.igsi_clearRepo))
      else
        OrientConnector.getInstance(database, trackPrimaryChanges, trackUpdateTimes).open()

      if (iteration > 0 && (trackPrimaryChanges || trackSecondaryChanges))
        Result.getInstance().resetScores()

      if (iteration == 0)
        SecondaryIndex.init(trackSecondaryChanges, trackPrimaryChanges, trackUpdateTimes, secondaryIndexFile, false)
      else
        SecondaryIndex.init(trackSecondaryChanges, trackPrimaryChanges, trackUpdateTimes, secondaryIndexFile, true)

      sleep()
      val startTime = System.currentTimeMillis();
      sleep()

      conf.setAppName(appName + iteration)
      val sc = SparkContext.getOrCreate(conf)
      val igsi = new IGSI(database, trackPrimaryChanges, trackUpdateTimes)
      val inputFile = inputFolder + File.separator + iterator.next()


      //parse n-triple file to RDD of GraphX Edges
      val edges = sc.textFile(inputFile).filter(line => !line.trim.isEmpty).map(line => NTripleParser.parse(line))
      //build graph from vertices and edges from edges
      val graph = RDFGraphParser.parse(edges)
      val partionedgraph = graph.partitionBy(RandomVertexCut, minPartitions);

      //Schema summarization:
      val schemaExtraction = indexModel
      val schemaElements = partionedgraph.aggregateMessages[(Int, mutable.HashSet[SchemaElement])](
        triplet => schemaExtraction.sendMessage(triplet),
        (a, b) => schemaExtraction.mergeMessage(a, b))

      //merge all instances with same schema
      val aggregatedSchemaElements = schemaElements.values //.reduceByKey(_ ++ _)

      //  (merge) schema elements TODO: conserve payload information
      val tmp = aggregatedSchemaElements.values.map(set => {
        val iter = set.iterator
        val se = iter.next()
        while (iter.hasNext)
          se.instances.addAll(iter.next().instances)
        se
      })
      //stream save in parallel (faster than individual add)
      igsi.saveRDD(tmp, (x: Iterator[SchemaElement]) => x)

      if (trackPrimaryChanges || trackSecondaryChanges)
        updateResult.mergeAll(Result.getInstance())

      val deleteIterator = SecondaryIndex.getInstance().getSchemaElementsToBeRemoved().iterator()
      val schemaIDsToBeDeleted = new java.util.HashSet[Integer]()
      while (deleteIterator.hasNext) {
        val schemaID = deleteIterator.next();
        val imprintsResult = SecondaryIndex.getInstance().getSummarizedInstances(schemaID);
        if (imprintsResult._result == null || imprintsResult._result.size() <= 0)
          schemaIDsToBeDeleted.add(schemaID)
        if (trackPrimaryChanges || trackSecondaryChanges)
          updateResult.mergeAll(imprintsResult)
      }
      val deleteResult = OrientConnector.getInstance(database, trackPrimaryChanges, trackUpdateTimes).bulkDeleteSchemaElements(schemaIDsToBeDeleted);

      val deleteResult2 = OrientConnector.getInstance(database, trackPrimaryChanges, trackUpdateTimes).removeOldImprintsAndElements(startTime)
      //ChangeTracker.getInstance.incSchemaStructureDeleted(removedSchemaElements)

      //sc.stop
      sc.stop()
      val secondaryBytes = SecondaryIndex.getInstance().persist()

      if (trackPrimaryChanges || trackSecondaryChanges) {
        updateResult.mergeAll(deleteResult)
        updateResult.mergeAll(deleteResult2)

        val trackStart = System.currentTimeMillis();
        logger.info("Exporting changes at " + trackStart)

        updateResult._changeTracker.exportToCSV(logChangesDir + File.separator + config.getString(config.VARS.spark_name) + "-changes.csv", iteration)
        val writer = new BufferedWriter(new FileWriter(logChangesDir + File.separator + config.getString(config.VARS.spark_name) + "-update-time-and-space.csv", iteration > 0))
        if (iteration == 0) {
          writer.write("Iteration,SecondaryIndex Read time (ms),SecondaryIndex Write time (ms),SecondaryIndex Del time (ms),SecondaryIndex Total time (ms),SE links,Imprint links,Checksum links," +
            "Sec. Index Size (bytes),Schema Elements (SE),Schema Relations (SR),Index Size (bytes),Graph Size (bytes)," +
            "SG Read time (ms),SG Write time (ms),SG Del time (ms)")
          writer.newLine()
        }

        val indexBytes = 0
        //val indexBytes = OrientDbOptwithMem.getInstance(database, trackChanges).sizeOnDisk()
        logger.info("Start counting schema elements after " + (System.currentTimeMillis() - trackStart) + "ms")
        val indexSize = OrientConnector.getInstance(database, trackPrimaryChanges, trackUpdateTimes).countSchemaElementsAndLinks()
        logger.info("Finished counting after " + (System.currentTimeMillis() - trackStart + "ms"))
        val graphBytes = 0 //new File(inputFile).length()
        writer.write(iteration + "," + updateResult._timeSpentReadingSecondaryIndex + "," + updateResult._timeSpentWritingSecondaryIndex
          + "," + updateResult._timeSpentDeletingSecondaryIndex + "," + (
          updateResult._timeSpentReadingSecondaryIndex + updateResult._timeSpentWritingSecondaryIndex + updateResult._timeSpentDeletingSecondaryIndex) +
          "," + SecondaryIndex.getInstance().getSchemaLinks + "," + SecondaryIndex.getInstance().getImprintLinks + "," + SecondaryIndex.getInstance().getSchemaToImprintLinks +
          "," + secondaryBytes + "," + indexSize(0) + "," + indexSize(1) + "," + indexBytes + "," + graphBytes +
          "," + updateResult._timeSpentReadingPrimaryIndex +
          "," + updateResult._timeSpentWritingPrimaryIndex +
          "," + updateResult._timeSpentDeletingPrimaryIndex)
        writer.newLine()
        writer.close()
        logger.info("Finished exporting after a total of " + (System.currentTimeMillis() - trackStart) + "ms")
      }
      OrientConnector.getInstance(database, trackPrimaryChanges, trackUpdateTimes).close()
      logger.info(s"Iteration ${iteration} completed.")

      if (alsoBatch) {
        logger.info("Computing batch now.")
        val confBatch = conf.clone()
        confBatch.setAppName(appName + "_batch_" + iteration)
        val scBatch = SparkContext.getOrCreate(confBatch)
        OrientConnector.create(database + "_batch", true)
        if (trackPrimaryChanges || trackSecondaryChanges)
          Result.getInstance().resetScores()

        SecondaryIndex.deactivate()

        //parse n-triple file to RDD of GraphX Edges
        val edgesBatch = scBatch.textFile(inputFile).filter(line => !line.trim.isEmpty).map(line => NTripleParser.parse(line))
        //build graph from vertices and edges from edges
        val graphBatch = RDFGraphParser.parse(edgesBatch)
        val partionedGraphBatch = graphBatch.partitionBy(RandomVertexCut, minPartitions);

        //Schema Summarization:
        val schemaExtraction = indexModel
        val schemaElementsBatch = partionedGraphBatch.aggregateMessages[(Int, mutable.HashSet[SchemaElement])](
          triplet => schemaExtraction.sendMessage(triplet),
          (a, b) => schemaExtraction.mergeMessage(a, b))

        //merge all instances with same schema
        val aggregatedSchemaElementsBatch = schemaElementsBatch.values //.reduceByKey(_ ++ _)

        // batch writing
        val igsiBatch = new IGSI(database + "_batch", trackPrimaryChanges, trackUpdateTimes)
        val tmp = aggregatedSchemaElementsBatch.values.map(set => {
          val iter = set.iterator
          val se = iter.next()
          while (iter.hasNext)
            se.instances.addAll(iter.next().instances)
          se
        })
        igsiBatch.saveRDD(tmp, (x: Iterator[SchemaElement]) => x)

        logger.info("Trying to stop batch context")
        scBatch.stop()
        logger.info("Batch context stopped")

        //val goldSize = OrientDbOptwithMem.getInstance(database + "_batch", trackChanges).sizeOnDisk();
        OrientConnector.getInstance(database + "_batch", trackPrimaryChanges, trackUpdateTimes).close()
        OrientConnector.removeInstance(database + "_batch")
        //println(s"Batch computation ${iteration} also completed! Compare sizes: batch: ${goldSize} vs.  incr. ${indexBytes}")
      }
      iteration += 1
    }

    updateResult._changeTracker
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

    //recommended to wait 1sec after timestamp since orientdb measures in seconds (not ms)
    pipeline.start()
  }
}
