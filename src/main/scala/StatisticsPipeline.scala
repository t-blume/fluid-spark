
import java.io.{BufferedWriter, File, FileWriter}

import input.{NTripleParser, RDFGraphParser}
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.PartitionStrategy.RandomVertexCut
import org.apache.spark.{SparkConf, SparkContext}


class StatisticsPipeline(maxMemory: String = "200g",
                         maxCores: String = "40",
                         typeLabel: String = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                         baseURI: String = "http://informatik.uni-kiel.de/fluid#",
                         inputFiles: Array[File]) extends Serializable {


  //delete output directory
  val conf = new SparkConf().setAppName("StatisticsPipeline").
    setMaster("local[*]").
    set("spark.driver.memory", maxMemory).
    set("spark.executor.memory", maxMemory).
    set("spark.core.max", maxCores).
    set("spark.executor.core", maxCores).
    set("spark.driver.maxResultSize", "0").
    set("spark.sparkContext.setCheckpointDir", ".")


  RDFGraphParser.classSignal = typeLabel
  NTripleParser.baseURI = baseURI

  def start() = {
    var iteration: Int = 0

    for (inputFile: File <- inputFiles) {
      println(inputFile)
      val sc = new SparkContext(conf)

      //parse n-triple file to RDD of GraphX Edges
      val edges = sc.textFile(inputFile.getPath).filter(line => !line.trim.isEmpty).map(line => NTripleParser.parse(line))

      //build graph from vertices and edges from edges
      val graph = RDFGraphParser.parse(edges)
      val partionedgraph: Graph[Set[(String, String)], (String, String, String, String)] = graph.partitionBy(RandomVertexCut, 40);
      partionedgraph.cache()


      val max_degree = partionedgraph.degrees.values.max()
      val max_indegree = partionedgraph.inDegrees.values.max()
      val max_outdegree = partionedgraph.outDegrees.values.max()

      val avg_degree = partionedgraph.degrees.values.mean()
      val avg_indegree = partionedgraph.inDegrees.values.mean()
      val avg_outdegree = partionedgraph.outDegrees.values.mean()

      val std_degree = partionedgraph.degrees.values.stdev()
      val std_indegree = partionedgraph.inDegrees.values.stdev()
      val std_outdegree = partionedgraph.outDegrees.values.stdev()

      val writer = new BufferedWriter(new FileWriter(inputFile + "-degree.csv"))
      writer.write("avg_degree,std_degree,max_degree,avg_indegree,std_indegree,max_indegree,avg_outdegree,std_outdegree,max_outdegree")
      writer.newLine()


      writer.write(avg_degree + "," + std_degree + "," + max_degree + "," +
        avg_indegree + "," + std_indegree + "," + max_indegree + "," +
        avg_outdegree + "," + std_outdegree + "," + max_outdegree)
      writer.newLine()
      writer.close()
      sc.stop
      iteration = iteration + 1
    }

    //ChangeTracker.getInstance()
  }
}


object StatisticsMain {
  def main(args: Array[String]) {

    var inputFiles: Array[File] = new Array[File](1)
    if (args.size < 2) {
      println("Usage: <type> <file|directory>")
      return
    } else {
      val file = new File(args(1))
      if (file.isDirectory) {
        println("Using all files in directory \"" + args(1) + "\"")
        file.list().foreach(f => println(f +":" + new File(file.getAbsolutePath + File.separator + f).isDirectory))
        inputFiles = file.listFiles().filter(p => p.getName.contains("data") && p.getName.endsWith(".gz"))
        val subDirs = file.list().map(p => new File(file.getAbsolutePath + File.separator + p)).filter(p => p.isDirectory)
        subDirs.foreach(d => println(d.listFiles().filter(p => p.getName.contains("data") && p.getName.endsWith(".gz")).foreach(f => println(f))))
        subDirs.foreach(d => inputFiles = inputFiles ++ d.listFiles().filter(p => p.getName.contains("data") && p.getName.endsWith(".gz")))
        inputFiles.foreach(f => println(f))
      } else {
        println("Using the single input file \"" + args(1) + "\"")
        inputFiles(0) = file
      }
    }

    val pipeline: StatisticsPipeline = new StatisticsPipeline(typeLabel = args(0), inputFiles = inputFiles)

    //recommended to wait 1sec after timestamp since time is measured in seconds (not ms)
    pipeline.start()
    //pipeline.start("tmp-stats.csv")
  }
}
