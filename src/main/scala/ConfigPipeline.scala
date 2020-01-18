
import java.io.{BufferedWriter, File, FileWriter}

import database._
import input.{NTripleParser, RDFGraphParser}
import org.apache.spark.graphx.PartitionStrategy.RandomVertexCut
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import schema.SchemaElement

import scala.collection.mutable

class ConfigPipeline(config: MyConfig) {

  //    create tmp directory
  val logDir: File = new File(config.getString(config.VARS.spark_log_dir))
  if (!logDir.exists())
    logDir.mkdirs()

  var maxMemory = "1g"
  if (config.getString(config.VARS.spark_memory) != null)
    maxMemory = config.getString(config.VARS.spark_memory)

  var maxCores = "4"
  if (config.getString(config.VARS.spark_cores) != null)
    maxCores = config.getString(config.VARS.spark_cores)
  //delete output directory
  val conf = new SparkConf().
    setMaster(config.getString(config.VARS.spark_master)).
    set("spark.executor.heartbeatInterval", "10000s").
    set("spark.network.timeout", "86400s").
    set("spark.eventLog.enabled", "true").
    set("spark.eventLog.dir", config.getString(config.VARS.spark_log_dir)).
    set("spark.driver.memory", maxMemory).
    set("spark.executor.memory", maxMemory).
    set("spark.driver.maxResultSize", "0").
    set("spark.core.max", maxCores).
    set("spark.executor.core", maxCores)
//    .set("spark.driver.allowMultipleContexts", "true")


  val inputFiles: java.util.List[String] = config.getStringList(config.VARS.input_filename)
  Constants.TYPE = config.getString(config.VARS.input_graphLabel)

  if (config.getString(config.VARS.input_namespace) != null)
    NTripleParser.baseURI = config.getString(config.VARS.input_namespace)

  if (config.getString(config.VARS.input_defaultSource) != null)
    NTripleParser.defaultSource = config.getString(config.VARS.input_defaultSource)


  //OUT
  OrientDbOptwithMem.URL = config.getString(config.VARS.db_url)
  OrientDbOptwithMem.USERNAME = config.getString(config.VARS.db_user)
  OrientDbOptwithMem.PASSWORD = config.getString(config.VARS.db_password)

  val database = config.getString(config.VARS.db_name)
  val trackChanges = config.getBoolean(config.VARS.igsi_trackChanges)

  var logChangesDir: String = null
  if (trackChanges == true) {
    logChangesDir = config.getString(config.VARS.igsi_logChangesDir)
    val file: File = new File(logChangesDir)
    if (!file.exists) file.mkdirs
  }

  val minWait = config.getLong(config.VARS.igsi_minWait)
  val minPartitions = config.getInt(config.VARS.spark_partitions);

  def start(alsoBatch: Boolean = true): ChangeTracker = {
    var iteration = 0
    val iterator: java.util.Iterator[String] = inputFiles.iterator()
    val inputFolder = config.getString(config.VARS.input_folder)
    val secondaryIndexFile = "secondaryIndex.ser.gz"
    while (iterator.hasNext) {
      if (iteration == 0)
        OrientDbOptwithMem.create(database, config.getBoolean(config.VARS.igsi_clearRepo))
      else
        OrientDbOptwithMem.getInstance(database, trackChanges).open()


      if (iteration > 0 && trackChanges)
        ChangeTracker.getInstance().resetScores()


      if (iteration == 0)
        SecondaryIndexMem.init(trackChanges, secondaryIndexFile, false)
      else
        SecondaryIndexMem.init(trackChanges, secondaryIndexFile, true)
      if (minWait > 0) {
        println("waiting for " + minWait + " ms")
        Thread.sleep(minWait)
        println("...continuing!")
      }
      val startTime = System.currentTimeMillis();
      if (minWait > 0) {
        println("waiting for " + minWait + " ms")
        Thread.sleep(minWait)
        println("...continuing!")
      }

      conf.setAppName(config.getString(config.VARS.spark_name) + iteration)
      val sc = SparkContext.getOrCreate(conf)

      val igsi = new IGSI(database, trackChanges)
      val inputFile = inputFolder + File.separator + iterator.next()

      //parse n-triple file to RDD of GraphX Edges
      val edges = sc.textFile(inputFile).filter(line => !line.trim.isEmpty).map(line => NTripleParser.parse(line))
      //build graph from vertices and edges from edges
      val graph = RDFGraphParser.parse(edges)
      val partionedgraph = graph.partitionBy(RandomVertexCut, minPartitions);

      val schemaExtraction = config.INDEX_MODELS.get(config.getString(config.VARS.schema_indexModel))
      //Schema Summarization:
      val schemaElements = partionedgraph.aggregateMessages[(Int, mutable.HashSet[SchemaElement])](
        triplet => schemaExtraction.sendMessage(triplet),
        (a, b) => schemaExtraction.mergeMessage(a, b))

      //merge all instances with same schema
      val aggregatedSchemaElements = schemaElements.values//.reduceByKey(_ ++ _)
      //      println(s"Schema Elements: ${aggregatedSchemaElements.size}")

      //  (incremental) writing
      val tmp = aggregatedSchemaElements.values.map(set => {
        val iter = set.iterator
        val se = iter.next()
        while (iter.hasNext)
          se.instances.addAll(iter.next().instances)
        se
      })
      igsi.saveRDD(tmp, (x: Iterator[SchemaElement]) => x)
//
 //     aggregatedSchemaElements.values.foreach(tuple => igsi.tryAddOptimized(tuple))

      val deleteIterator = SecondaryIndexMem.getInstance().getSchemaElementsToBeRemoved().iterator()
      val schemaIDsToBeDeleted = new java.util.HashSet[Integer]()
      while (deleteIterator.hasNext) {
        val schemaID = deleteIterator.next();
        val imprints = SecondaryIndexMem.getInstance().getSummarizedInstances(schemaID);
        if (imprints == null || imprints.size() <= 0)
          schemaIDsToBeDeleted.add(schemaID)
      }
      OrientDbOptwithMem.getInstance(database, trackChanges).bulkDeleteSchemaElements(schemaIDsToBeDeleted);

      OrientDbOptwithMem.getInstance(database, trackChanges).removeOldImprintsAndElements(startTime)


      //sc.stop
      println("Trying to stop incr. context")
      sc.stop()
      println("Incr. context stopped")
      val secondaryBytes = SecondaryIndexMem.getInstance().persist()

      if (trackChanges) {
        val trackStart = System.currentTimeMillis();

        println("Exporting changes at " + trackStart)

        ChangeTracker.getInstance().exportToCSV(logChangesDir + File.separator + config.getString(config.VARS.spark_name) + "-changes.csv", iteration)
        val writer = new BufferedWriter(new FileWriter(logChangesDir + File.separator + config.getString(config.VARS.spark_name) + "-update-time-and-space.csv", iteration > 0))
        if (iteration == 0) {
          writer.write("Iteration,Update time,Reading time,Waiting time,Total time,SE links,Imprint links,Checksum links," +
            "Sec. Index Size (bytes),Schema Elements (SE),Schema Relations (SR),Index Size (bytes),Graph Size (bytes)," +
            "SG Add time (ms),SG Del time (ms),SG Read time (ms)")
          writer.newLine()
        }

        val indexBytes = 0
        //val indexBytes = OrientDbOptwithMem.getInstance(database, trackChanges).sizeOnDisk()
        println("Start counting schema elements after " + (System.currentTimeMillis() - trackStart) + "ms")
        val indexSize = OrientDbOptwithMem.getInstance(database, trackChanges).countSchemaElementsAndLinks()
        println("Finished counting after " + (System.currentTimeMillis() - trackStart + "ms"))
        val graphBytes = 0//new File(inputFile).length()
        writer.write(iteration+","+SecondaryIndexMem.getInstance().getTimeSpentUpdating + "," + SecondaryIndexMem.getInstance().getTimeSpentReading
          + "," + SecondaryIndexMem.getInstance().getTimeSpentWaiting + "," + (
          SecondaryIndexMem.getInstance().getTimeSpentUpdating + SecondaryIndexMem.getInstance().getTimeSpentReading + SecondaryIndexMem.getInstance().getTimeSpentWaiting) +
          "," + SecondaryIndexMem.getInstance().getSchemaLinks + "," + SecondaryIndexMem.getInstance().getImprintLinks + "," + SecondaryIndexMem.getInstance().getSchemaToImprintLinks +
          "," + secondaryBytes + "," + indexSize(0) + "," + indexSize(1) + "," + indexBytes + "," + graphBytes +
          "," + OrientDbOptwithMem.getInstance(database, trackChanges).getTimeSpentAdding +
          "," + OrientDbOptwithMem.getInstance(database, trackChanges).getTimeSpentDeleting +
          "," + OrientDbOptwithMem.getInstance(database, trackChanges).getTimeSpentReading)
        writer.newLine()
        writer.close()
        println("Finsihed exporting after a total of " + (System.currentTimeMillis() - trackStart) + "ms")
      }
      OrientDbOptwithMem.getInstance(database, trackChanges).close()
      println(s"Iteration ${iteration} completed.")

      if (alsoBatch){
        println("Computing batch now.")

        val confBatch = conf.clone()
        confBatch.setAppName(config.getString(config.VARS.spark_name) + "_batch_" + iteration)
        val scBatch = SparkContext.getOrCreate(confBatch)
        OrientDbOptwithMem.create(database + "_batch", true)
        if (trackChanges)
          ChangeTracker.getInstance().resetScores()

        SecondaryIndexMem.deactivate()

        //parse n-triple file to RDD of GraphX Edges
        val edgesBatch = scBatch.textFile(inputFile).filter(line => !line.trim.isEmpty).map(line => NTripleParser.parse(line))
        //build graph from vertices and edges from edges
        val graphBatch = RDFGraphParser.parse(edgesBatch)
        val partionedGraphBatch = graphBatch.partitionBy(RandomVertexCut, minPartitions);

        //Schema Summarization:
        val schemaElementsBatch = partionedGraphBatch.aggregateMessages[(Int, mutable.HashSet[SchemaElement])](
          triplet => schemaExtraction.sendMessage(triplet),
          (a, b) => schemaExtraction.mergeMessage(a, b))

        //merge all instances with same schema
        val aggregatedSchemaElementsBatch = schemaElementsBatch.values//.reduceByKey(_ ++ _)
        //      println(s"Schema Elements: ${aggregatedSchemaElements.size}")

        // batch writing
        val igsiBatch = new IGSI(database + "_batch", trackChanges)
        val tmp = aggregatedSchemaElementsBatch.values.map(set => {
          val iter = set.iterator
          val se = iter.next()
          while (iter.hasNext)
            se.instances.addAll(iter.next().instances)
          se
        })
        igsiBatch.saveRDD(tmp, (x: Iterator[SchemaElement]) => x)

        //  (incremental) writing
        //aggregatedSchemaElementsBatch.values.foreach(tuple => igsiBatch.tryAddOptimized(tuple))

        println("Trying to stop batch context")
        scBatch.stop()
        println("Batch context stopped")

//        scBatch.stop()
//        while (!scBatch.isStopped)
//          wait(1000)

        //val goldSize = OrientDbOptwithMem.getInstance(database + "_batch", trackChanges).sizeOnDisk();

        OrientDbOptwithMem.getInstance(database + "_batch", trackChanges).close()
        OrientDbOptwithMem.removeInstance(database + "_batch")
        //println(s"Batch computation ${iteration} also completed! Compare sizes: batch: ${goldSize} vs.  incr. ${indexBytes}")
      }

      iteration += 1
    }

    ChangeTracker.getInstance()
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
