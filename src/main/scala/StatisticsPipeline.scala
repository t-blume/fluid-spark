
import java.io.{BufferedWriter, File, FileWriter}
import java.util

import database._
import input.{NTripleParser, RDFGraphParser}
import org.apache.spark.graphx.PartitionStrategy.RandomVertexCut
import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable


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


  Constants.TYPE = typeLabel
  NTripleParser.baseURI = baseURI

  def extractNamespace(s: String): String = {
    if (s == null)
      ""
    else {
      val split = s.split("(/|#)")
      var ns = ""
      for (i <- 0 to split.length - 2)
        ns += split(i)
      if (ns.startsWith("http:"))
        ns = ns.replaceFirst("http:", "")
      if (ns.startsWith("www."))
        ns = ns.replaceFirst("www.", "")
      ns
    }
  }

  def max(a: Int, b: Int): Int = {
    if (a > b) a else b
  }

  def min(a: Int, b: Int): Int = {
    if (a < b) a else b
  }

  def sum(a: Int, b: Int): Int = {
    a + b
  }

  def maxSeq(a: Iterable[Int]): Int = {
    var max = 0
    for (v: Int <- a) {
      if (v > max)
        max = v
    }
    max
  }

  def sumSeq(a: Iterable[Int]): Int = {
    var sum = 0
    for (v: Int <- a)
      sum = sum + v
    sum
  }

  def maxPath(a: (VertexId, ShortestPaths.SPMap)): Int = {
    maxSeq(a._2.values)
  }


  def standardDeviation(values: Iterable[Int]): Array[Double] = {
    var sum: Double = 0
    for (v: Int <- values)
      sum = sum + v
    val mean = sum / values.size.asInstanceOf[Double]
    var sd: Double = 0.0
    for (v: Int <- values)
      sd = sd + Math.pow(v - mean, 2)

    sd = sd / values.size
    sd = Math.sqrt(sd)
    Array(mean, sd)
  }

  def standardDeviation2(values: util.Collection[mutable.HashSet[String]]): Array[Double] = {
    var sum: Double = 0.0
    values.forEach(v => sum = sum + v.size)

    val mean = sum / values.size.asInstanceOf[Double]
    var sd: Double = 0.0
    values.forEach(v => sd = sd + Math.pow(v.size - mean, 2))

    sd = sd / values.size
    sd = Math.sqrt(sd)
    Array(mean, sd, sum)
  }

  def standardDeviation3(values: Array[mutable.HashSet[String]]): Array[Double] = {
    var sum: Double = 0.0
    for (v <- values)
      sum = sum + v.size

    val mean = sum / values.size.asInstanceOf[Double]
    var sd: Double = 0.0
    for (v <- values)
      sd = sd + Math.pow(v.size - mean, 2)

    sd = sd / values.size
    sd = Math.sqrt(sd)
    Array(mean, sd, sum)
  }

  def start(outFile: String) = {
    var iteration: Int = 0
    for (inputFile: File <- inputFiles) {
      val sc = new SparkContext(conf)

      //parse n-triple file to RDD of GraphX Edges
      val edges = sc.textFile(inputFile.getPath).filter(line => !line.trim.isEmpty).map(line => NTripleParser.parse(line))

      //build graph from vertices and edges from edges
      val graph = RDFGraphParser.parse(edges)

      val partionedgraph: Graph[Set[(String, String)], (String, String, String, String)] = graph.partitionBy(RandomVertexCut, 40);
      partionedgraph.cache()


      val instancesWithProperties = partionedgraph.triplets.map(triplet => (triplet.srcId, Set(triplet.attr))).reduceByKey(_ ++ _)
      val incomingProperties = partionedgraph.triplets.map(triplet => (triplet.dstId, Set(triplet.attr))).reduceByKey(_ ++ _)

      val instanceIDs = instancesWithProperties.keys.collect()


      val numberOfEdges = partionedgraph.numEdges
      val numberOfVertices = partionedgraph.numVertices
      val numberOfInstances = instanceIDs.size

      val types = partionedgraph.vertices.filter(D => instanceIDs.contains(D._1)).values.map(values => {
        val types = new mutable.HashSet[String]()
        if (values != null) {
          values.foreach(v => types.add(v._1))
        }
        types
      })

      val numberOfTypes = types.reduce(_++_).size
      val devTypes = standardDeviation(types.map(V => V.size).collect())
      val devProps = standardDeviation(instancesWithProperties.map(T => T._2.size).collect())
      val devIncomingProps = standardDeviation(incomingProperties.map(T => T._2.size).collect())

      val sourcesAndProperties = instancesWithProperties.map(T => {
        val sources = new mutable.HashSet[String]()
        val properties = new mutable.HashSet[String]()
        T._2.foreach(t => {
          sources.add(t._4)
          properties.add(t._2)
        })
        (T._1, sources, properties)
      })
      val uniqueProperties = sourcesAndProperties.flatMap(V => V._3).distinct()


      val propertySources = sourcesAndProperties.map(V => (V._1, V._2))

      val vertexSources = partionedgraph.vertices.filter(D => instanceIDs.contains(D._1)).map(v => {
        val sources = new mutable.HashSet[String]()
        if (v._2 != null) {
          v._2.foreach(v => {
            sources.add(v._2)
          })
        }
        (v._1, sources)
      })
      val sources = (vertexSources ++ propertySources).reduceByKey(_ ++ _)

      val uniqueSources = sources.flatMap(V => V._2).distinct()

      val devSources = standardDeviation3(sources.map(V => V._2).collect())

      // OPTIONALLY
      //      val connectedComponents = partionedgraph.asInstanceOf[Graph[Set[(String, String)], (String, String, String, String)]].connectedComponents()
      //      //
      //      val componentKeys = connectedComponents.vertices.map(V => (V._2, Set(V._1))).reduceByKey(_ ++ _)
      //      writer.write("|CC| = " + componentKeys.count() + "\n")
      //
      //
      //      //only calculate paths for instances
      //      val shortestPaths = ShortestPaths.run(partionedgraph, partionedgraph.vertices.filter(D => instanceIDs.contains(D._1)).map(V => V._1).collect().toSeq)
      //
      //      //longest shortest path for paths > 0
      //      val paths = shortestPaths.vertices.map(V => V._2.filter(p => p._2 > 0))
      //      val diameter = maxSeq(paths.map(M => maxSeq(M.values)).collect())
      //      val sumOfAllPathLengths = sumSeq(paths.map(M => sumSeq(M.values)).collect())
      //      val numberOfPaths = paths.map(M => M.values.size).reduce(sum)
      //
      //      println("sumOfAllPathLengths: " + sumOfAllPathLengths)
      //      println("numberOfPaths: " + numberOfPaths)
      //      writer.write("Diameter = " + diameter + "\n")
      //      writer.write("Avg. Path length: " + (sumOfAllPathLengths.asInstanceOf[Double] / numberOfPaths.asInstanceOf[Double]) + "\n")


      //      val shortestPaths = ShortestPaths.run(partionedgraph, partionedgraph.vertices.keys.collect().toSeq)
      //
      //      writer.write("Diameter = " + maxSeq(shortestPaths.vertices.map(maxPath).collect().toSeq))

      //output graph stats
      val out = new File(outFile)
      val writer = new BufferedWriter(new FileWriter(out, iteration > 0))

      if (iteration == 0) {
        //print header
        writer.write("File,|V|,|E|,|Instances|,Avg. Types,SD Types,Avg. Props,SD Props.,Avg. Inc. Props,SD Inc. Props.,Avg. Sources,SD Sources,Unique Types,Unique Props.,Unique Sources\n")
      }
      writer.write(inputFile.getName + "," + numberOfVertices + "," + numberOfEdges + "," + numberOfInstances +
        "," + devTypes(0) + "," + devTypes(1) + "," + devProps(0) + "," + devProps(1) + "," + devIncomingProps(0) + "," + devIncomingProps(1) +
        "," + devSources(0) + "," + devSources(1) + "," + numberOfTypes + "," + uniqueProperties.count() +
        "," + uniqueSources.count() + "\n")
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
      if(file.isDirectory){
        println("Using all files in directory \"" + args(1) + "\"")
        inputFiles = file.listFiles()
      }else{
        println("Using the single input file \"" + args(1) + "\"")
        inputFiles(0) = file
      }
    }


    val pipeline: StatisticsPipeline = new StatisticsPipeline(typeLabel = args(0), inputFiles = inputFiles)

    //recommended to wait 1sec after timestamp since time is measured in seconds (not ms)
    pipeline.start(new File(args(1)).getName + "-stats.csv")
  }
}
