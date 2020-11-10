
import java.io.{BufferedWriter, File, FileWriter}

import database._
import input.{NTripleParser, RDFGraphParser}
import org.apache.log4j.LogManager
import org.apache.spark.graphx.PartitionStrategy.RandomVertexCut
import org.apache.spark.{SparkConf, SparkContext}
import schema.{SE_ClassCollection, SchemaElement}
import utils.MyHash

import scala.collection.mutable

class ConfigPipeline(config: MyConfig, skipSnapshots: Int = 0, endEarly: Int = Int.MaxValue) {


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
  val trackTertiaryChanges =
    if (config.exists(config.VARS.igsi_trackTertiaryChanges))
      config.getBoolean(config.VARS.igsi_trackTertiaryChanges)
    else
      false

  // ------- spark ------- //
  val sparkMaster =
    if (config.exists(config.VARS.spark_master))
      config.getString(config.VARS.spark_master)
    else
      "local[*]"

  val sparkWorkDir =
    if (config.exists(config.VARS.spark_work_dir))
      config.getString(config.VARS.spark_work_dir)
    else
      "/tmp/"

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


  val datasourcePayload =
    if (config.exists(config.VARS.schema_payload))
      config.getBoolean(config.VARS.schema_payload)
    else
      true




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
  val deltaGraphUpdates =
    if (config.exists(config.VARS.igsi_deltaGraphUpdates))
      config.getBoolean(config.VARS.igsi_deltaGraphUpdates)
    else
      false


  val alsoBatch =
    if (config.exists(config.VARS.igsi_alsoBatch))
      config.getBoolean(config.VARS.igsi_alsoBatch)
    else
      true

  val onlyBatch =
    if (config.exists(config.VARS.igsi_onlyBatch))
      config.getBoolean(config.VARS.igsi_onlyBatch)
    else
      false

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
    set("spark.executor.heartbeatInterval", "60s"). //1m
    set("spark.network.timeout", "360s"). //6m
    set("spark.eventLog.enabled", "true").
    set("spark.eventLog.logStageExecutorMetrics", "true").
    set("spark.eventLog.dir", sparkEventDir).
    set("spark.driver.memory", maxMemory).
    set("spark.executor.memory", maxMemory).
    set("spark.memory.offHeap.enabled", "true").
    set("spark.memory.offHeap.size", "8589934592").
    set("spark.driver.maxResultSize", "0").
    set("spark.core.max", maxCores).
    set("spark.executor.core", maxCores).
    set("spark.local.dir", sparkWorkDir).
    set("spark.worker.dir", sparkWorkDir)



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
    val maxCoresInt = Integer.valueOf(maxCores)
    var iteration = 0
    val iterator: java.util.Iterator[String] = inputFiles.iterator()

    val secondaryIndexFile = "secondaryIndex.ser.gz"
    val updateResult: Result[Boolean] = new Result[Boolean](trackUpdateTimes, trackPrimaryChanges || trackSecondaryChanges)

    var schemaStats: java.util.Map[Integer, (Integer, Integer)] = new java.util.HashMap[Integer, (Integer, Integer)]();
    while (iterator.hasNext && iteration <= endEarly) {
      if (skipSnapshots <= iteration) {
      var newEdgesFile: String = null
      var removeEdgesFile: String = null
      if (deltaGraphUpdates && iteration > 0) {
        val fileName = iterator.next()
        newEdgesFile = inputFolder + File.separator + "additions_" + fileName
        removeEdgesFile = inputFolder + File.separator + "removals_" + fileName
      } else
        newEdgesFile = inputFolder + File.separator + iterator.next()

      if (!onlyBatch) {
        if (trackPrimaryChanges || trackSecondaryChanges)
          updateResult.resetScores()

        if (iteration == 0 || iteration == skipSnapshots)
          OrientConnector.create(database, config.getBoolean(config.VARS.igsi_clearRepo))
        else
          OrientConnector.getInstance(database, trackPrimaryChanges, trackUpdateTimes, maxCoresInt).open(maxCoresInt)

        if (iteration > 0 && (trackPrimaryChanges || trackSecondaryChanges))
          Result.getInstance().resetScores()


        if (iteration == 0 || iteration == skipSnapshots)
          OrientConnector.getInstance(database, trackPrimaryChanges, trackUpdateTimes, maxCoresInt).
            setSecondaryIndex(SecondaryIndex.
              instantiate(trackSecondaryChanges, trackPrimaryChanges, trackUpdateTimes, secondaryIndexFile, false))
        else
          OrientConnector.getInstance(database, trackPrimaryChanges, trackUpdateTimes, maxCoresInt).
            setSecondaryIndex(SecondaryIndex.
              instantiate(trackSecondaryChanges, trackPrimaryChanges, trackUpdateTimes, secondaryIndexFile, true))

        if (indexModel.equals(SE_ClassCollection))
          OrientConnector.getInstance(database, trackPrimaryChanges, trackUpdateTimes, maxCoresInt).setAllowOrphans(true)
        sleep()
        val startTime = System.currentTimeMillis();
        sleep()

        //only do this when snapshot is not skipped

        conf.setAppName(appName + iteration)
        //change the name of the log file for each run
        OrientConnector.initLogger(appName + iteration)

        val sc = new SparkContext(conf)
        val igsi = new IGSI(database, trackPrimaryChanges, trackUpdateTimes)


        //parse n-triple file to RDD of GraphX Edges
        val inputEdges = sc.textFile(newEdgesFile).filter(line => !line.trim.isEmpty).map(line => NTripleParser.parse(line))
        val removalEdges =
          if (deltaGraphUpdates)
            sc.textFile(removeEdgesFile).filter(line => !line.trim.isEmpty).map(line => NTripleParser.parse(line))
          else
            null

        var edges = inputEdges
        //TODO: delta graph updates?
        val graphDatabase: OrientConnector = OrientConnector.getInstance(database, trackPrimaryChanges, trackUpdateTimes, maxCoresInt)
        val secondaryIndex = graphDatabase.getSecondaryIndex
        if (deltaGraphUpdates && iteration > 0) {
          //removes all edges from vertices that are known
          edges = inputEdges.filter(edge => {
            val outInstance = MyHash.md5HashString(edge.attr._1)
            !secondaryIndex.containsImprint(outInstance)
          })
        }

        //build graph from vertices and edges from edges
        val graph = RDFGraphParser.parse(edges)
        val partionedgraph = graph.partitionBy(RandomVertexCut, minPartitions);

        //TODO: delta graph updates?
        if (deltaGraphUpdates && iteration > 0) {
          val knownInstanceEdges = inputEdges.filter(edge => {
            val outInstance = MyHash.md5HashString(edge.attr._1)
            secondaryIndex.containsImprint(outInstance)
          })

          val updateGraph = RDFGraphParser.parse(knownInstanceEdges)
          val instances = updateGraph.triplets.map(t => (t.attr._1, new TripletWrapper(t))).reduceByKey(_.merge(_)).values
          igsi.updateDelta(instances, (x: Iterator[TripletWrapper]) => x, true, datasourcePayload, maxCoresInt)

          val deleteGraph = RDFGraphParser.parse(removalEdges)
          val removalInstances = deleteGraph.triplets.map(t => (t.attr._1, new TripletWrapper(t))).reduceByKey(_.merge(_)).values
          igsi.updateDelta(removalInstances, (x: Iterator[TripletWrapper]) => x, false, datasourcePayload, maxCoresInt)
        }

        //Schema summarization:
        val schemaExtraction = indexModel
        //TODO: k-bisimulation
        val schemaElements = partionedgraph.aggregateMessages[(Int, mutable.HashSet[SchemaElement])](
          triplet => schemaExtraction.sendMessage(triplet),
          (a, b) => schemaExtraction.mergeMessage(a, b))

        //merge all instances with same schema
        val aggregatedSchemaElements = schemaElements.values //.reduceByKey(_ ++ _)

        //  (merge) schema elements
        val tmp = aggregatedSchemaElements.values.map(set => {
          val iter = set.iterator
          val se = iter.next()
          while (iter.hasNext)
            se.merge(iter.next())
          se
        })
        //tmp.foreach(f => println(f))
        //stream save in parallel (faster than individual add)
        logger.info("Find and Merge Phase")
        igsi.saveRDD(tmp, (x: Iterator[SchemaElement]) => x, false, datasourcePayload, maxCoresInt)

        if (trackPrimaryChanges || trackSecondaryChanges)
          updateResult.mergeAll(Result.getInstance())

        val deleteIterator = OrientConnector.getInstance(database, trackPrimaryChanges, trackUpdateTimes, maxCoresInt).
          getSecondaryIndex.getSchemaElementsToBeRemoved().iterator()
        val schemaIDsToBeDeleted = new java.util.HashSet[Integer]()
        while (deleteIterator.hasNext) {
          val schemaID = deleteIterator.next();
          val imprintsResult = OrientConnector.getInstance(database, trackPrimaryChanges, trackUpdateTimes, maxCoresInt).
            getSecondaryIndex.getSummarizedInstances(schemaID);
          if (imprintsResult._result == null || imprintsResult._result.size() <= 0)
            schemaIDsToBeDeleted.add(schemaID)
          if (trackPrimaryChanges || trackSecondaryChanges)
            updateResult.mergeAll(imprintsResult)
        }
        val deleteResult = OrientConnector.getInstance(database, trackPrimaryChanges, trackUpdateTimes, maxCoresInt).bulkDeleteSchemaElements(schemaIDsToBeDeleted);


        if (!deltaGraphUpdates)
          deleteResult.mergeAll(OrientConnector.getInstance(database, trackPrimaryChanges, trackUpdateTimes, maxCoresInt).removeOldImprintsAndElements(startTime))
        //ChangeTracker.getInstance.incSchemaStructureDeleted(removedSchemaElements)

        //sc.stop
        logger.info("Stopping spark")
        sc.stop()
        logger.info("Spark stopped")
        val secondaryBytes = OrientConnector.getInstance(database, trackPrimaryChanges, trackUpdateTimes, maxCoresInt).
          getSecondaryIndex.persist()
        logger.info("Secondary index persistet")

        if (trackPrimaryChanges || trackSecondaryChanges) {
          updateResult.mergeAll(deleteResult)


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
          val indexSize = OrientConnector.getInstance(database, trackPrimaryChanges, trackUpdateTimes, maxCoresInt).countSchemaElementsAndLinks()
          logger.info("Finished counting after " + (System.currentTimeMillis() - trackStart + "ms"))
          val graphBytes = 0 //new File(inputFile).length()
          val secondaryIndex = OrientConnector.getInstance(database, trackPrimaryChanges, trackUpdateTimes, maxCoresInt).
            getSecondaryIndex
          writer.write(iteration + "," + updateResult._timeSpentReadingSecondaryIndex + "," + updateResult._timeSpentWritingSecondaryIndex
            + "," + updateResult._timeSpentDeletingSecondaryIndex + "," + (
            updateResult._timeSpentReadingSecondaryIndex + updateResult._timeSpentWritingSecondaryIndex + updateResult._timeSpentDeletingSecondaryIndex) +
            "," + secondaryIndex.getSchemaLinks + "," + secondaryIndex.getImprintLinks + "," + secondaryIndex.getSchemaToImprintLinks +
            "," + secondaryBytes + "," + indexSize(0) + "," + indexSize(1) + "," + indexBytes + "," + graphBytes +
            "," + updateResult._timeSpentReadingPrimaryIndex +
            "," + updateResult._timeSpentWritingPrimaryIndex +
            "," + updateResult._timeSpentDeletingPrimaryIndex)
          writer.newLine()
          writer.close()
          logger.info("Finished exporting after a total of " + (System.currentTimeMillis() - trackStart) + "ms")
        }
        if (trackTertiaryChanges) {
          val countStats = OrientConnector.getInstance(database, trackPrimaryChanges, trackUpdateTimes, maxCoresInt).completenessAnalysisExport()
          val countStatsWriter = new BufferedWriter(new FileWriter(logChangesDir + File.separator + config.getString(config.VARS.spark_name) +
            "-completenessAnalysisExport" + iteration + ".csv"))
          countStatsWriter.write("Schema hash,instance count\n")
          countStatsWriter.write(countStats)
          countStatsWriter.close()


          val tmpStats = OrientConnector.getInstance(database, trackPrimaryChanges, trackUpdateTimes, maxCoresInt).getIndexInformation()
          if (schemaStats == null)
            schemaStats = tmpStats
          else
            schemaStats.putAll(tmpStats)
        }

        OrientConnector.getInstance(database, trackPrimaryChanges, trackUpdateTimes, maxCoresInt).close()
        logger.info(s"Iteration ${iteration} completed.")
      }


      if (alsoBatch) {
        logger.info("Computing batch now.")
        val confBatch = conf.clone()
        confBatch.setAppName(appName + "_batch_" + iteration)
        OrientConnector.initLogger(appName + "_batch_" + iteration)

        val scBatch = new SparkContext(confBatch)
        OrientConnector.create(database + "_batch", true)
        if (trackPrimaryChanges || trackSecondaryChanges)
          Result.getInstance().resetScores()

        //        secondaryIndex.deactivate()

        //parse n-triple file to RDD of GraphX Edges
        val edgesBatch = scBatch.textFile(newEdgesFile).filter(line => !line.trim.isEmpty).map(line => NTripleParser.parse(line))
        //build graph from vertices and edges from edges
        val graphBatch = RDFGraphParser.parse(edgesBatch)
        val partionedGraphBatch = graphBatch.partitionBy(RandomVertexCut, minPartitions);

        //Schema Summarization:
        val schemaExtraction = indexModel
        val schemaElementsBatch = partionedGraphBatch.aggregateMessages[(Int, mutable.HashSet[SchemaElement])](
          triplet => schemaExtraction.sendMessage(triplet),
          (a, b) => schemaExtraction.mergeMessage(a, b))

        //merge all instances with same schema
        //Aggregating in spark super slow, DB is faster
        val aggregatedSchemaElementsBatch = schemaElementsBatch.values //.reduceByKey(_ ++ _)

        // batch writing
        val igsiBatch = new IGSI(database + "_batch", trackPrimaryChanges, trackUpdateTimes)
        val tmp = aggregatedSchemaElementsBatch.values.map(set => {
          val iter = set.iterator
          val se = iter.next()
          while (iter.hasNext)
            se.merge(iter.next())
          se
        })
        //        tmp.foreach(t => println(t))
        igsiBatch.saveRDD(tmp, (x: Iterator[SchemaElement]) => x, true, datasourcePayload, maxCoresInt)

        logger.info("Trying to stop batch context")
        scBatch.stop()
        logger.info("Batch context stopped")

        //val goldSize = OrientDbOptwithMem.getInstance(database + "_batch", trackChanges).sizeOnDisk();
        OrientConnector.getInstance(database + "_batch", trackPrimaryChanges, trackUpdateTimes, maxCoresInt).close()
        OrientConnector.removeInstance(database + "_batch")
        //println(s"Batch computation ${iteration} also completed! Compare sizes: batch: ${goldSize} vs.  incr. ${indexBytes}")
      }
    }else
      iterator.next()
      iteration += 1
    }

    if (trackTertiaryChanges) {
      val schemaStatsWriter = new BufferedWriter(new FileWriter(logChangesDir + File.separator + config.getString(config.VARS.spark_name) +
        "-schemaInfoExport.csv"))
      schemaStatsWriter.write("Schema hash,Type count,Property count\n")
      schemaStats.entrySet().forEach(entry => {
        schemaStatsWriter.write(entry.getKey + "," + entry.getValue._1 + "," + entry.getValue._2 + "\n")
      })
      schemaStatsWriter.close()
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
    } else {
      var pipeline: ConfigPipeline = null
      println("Conf:" + args(0))
      if (args.size > 1) {
        if (args.size > 2){
          println("Experiment with interval [" + args(1) + ","+ args(2) + "] snapshots")
          pipeline = new ConfigPipeline(new MyConfig(args(0)), args(1).toInt, args(2).toInt)
        }else{
          println("skipping the first " + args(1) + " snapshots")
          pipeline = new ConfigPipeline(new MyConfig(args(0)), args(1).toInt)

        }
      } else
        pipeline = new ConfigPipeline(new MyConfig(args(0)))

      //recommended to wait 1sec after timestamp since orientdb measures in seconds (not ms)
      pipeline.start()

    }

  }
}
