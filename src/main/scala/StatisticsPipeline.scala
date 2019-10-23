
import java.io.{BufferedWriter, File, FileWriter}

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
                         inputFiles: Array[String]) extends Serializable {


  //delete output directory
  val conf = new SparkConf().setAppName("StatisticsPipeline").
    setMaster("local[*]").
    set("spark.driver.memory", maxMemory).
    set("spark.executor.memory", maxMemory).
    set("spark.core.max", maxCores).
    set("spark.executor.core", maxCores).
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


  def standardDeviation(values: Iterable[Int], mean: Double): Double = {
    var sum: Double = 0
    for (v: Int <- values)
      sum = sum + Math.pow(v - mean, 2)

    sum = sum / values.size
    Math.sqrt(sum)
  }

  def start() = {
    for (inputFile: String <- inputFiles) {
      val sc = new SparkContext(conf)

      //parse n-triple file to RDD of GraphX Edges
      val edges = sc.textFile(inputFile).filter(line => !line.trim.isEmpty).map(line => NTripleParser.parse(line))

      //build graph from vertices and edges from edges
      val graph = RDFGraphParser.parse(edges)

      val partionedgraph: Graph[Set[(String, String)], (String, String, String, String)] = graph.partitionBy(RandomVertexCut, 40);
      partionedgraph.cache()
      //output graph stats
      val out = new File(new File(inputFile).getName + "-stats.txt")
      val writer = new BufferedWriter(new FileWriter(out))


      val instancesWithProperties = partionedgraph.triplets.map(triplet => (triplet.srcId, Set(triplet.attr))).reduceByKey(_ ++ _)
      val instanceIDs = instancesWithProperties.keys.collect()


      val numberOfEdges = partionedgraph.numEdges
      val numberOfVertices = partionedgraph.numVertices
      val numberOfInstances = instanceIDs.size

      writer.write("|E| = " + numberOfEdges + "\n")
      writer.write("|V| = " + numberOfVertices + "\n")
      writer.write("|Instances| =  " + numberOfInstances + "\n")

      //filtered by instance
      val inDegrees = partionedgraph.inDegrees.filter(D => instanceIDs.contains(D._1))
      val outDegrees = partionedgraph.outDegrees.filter(D => instanceIDs.contains(D._1))
      val degrees = partionedgraph.degrees.filter(D => instanceIDs.contains(D._1))


      //SUM
      val sumInDegrees = inDegrees.values.reduce(sum)
      val sumOutDegrees = outDegrees.values.reduce(sum)
      val sumDegrees = degrees.values.reduce(sum)
      //MAX
      val maxInDegree = inDegrees.values.reduce(max)
      val maxOutDegree = outDegrees.values.reduce(max)
      val maxDegree = degrees.values.reduce(max)


      //AVERAGE and STANDARD DEVIATION
      val avgInDegree: Double = sumInDegrees.asInstanceOf[Double] / numberOfInstances.asInstanceOf[Double]
      val devInDegree: Double = standardDeviation(inDegrees.values.collect(), avgInDegree)

      val avgOutDegree: Double = sumOutDegrees.asInstanceOf[Double] / numberOfInstances.asInstanceOf[Double]
      val devOutDegree: Double = standardDeviation(outDegrees.values.collect(), avgOutDegree)

      val avgDegree: Double = sumDegrees.asInstanceOf[Double] / numberOfInstances.asInstanceOf[Double]
      val devDegree: Double = standardDeviation(outDegrees.values.collect(), avgDegree)

      writer.write("avgInDegree = " + avgInDegree + " (SD = " + devInDegree + ") \n")
      writer.write("maxInDegree = " + maxInDegree + "\n")

      writer.write("avgOutDegree: " + avgOutDegree + " (SD = " + devOutDegree + ") \n")
      writer.write("maxOutDegree = " + maxOutDegree + "\n")

      writer.write("avgDegree =  " + avgDegree + " (SD = " + devDegree + ") \n")
      writer.write("maxDegree = " + maxDegree + "\n")


      val typeSets = partionedgraph.vertices.filter(D => instanceIDs.contains(D._1)).values.map(S => if (S != null) S.size else 0)

      val sumTypes = typeSets.reduce(sum)
      val minTypes = typeSets.reduce(min)
      val maxTypes = typeSets.reduce(max)
      val avgTypes = sumTypes.asInstanceOf[Double] / numberOfVertices.asInstanceOf[Double]
      val devTypes = standardDeviation(typeSets.collect(), avgTypes)

      writer.write("avgTypes = " + avgTypes + " (SD = " + devTypes + ") \n")
      writer.write("minTypes = " + minTypes + "\n")
      writer.write("maxTypes = " + maxTypes + "\n")


      val allTypeSets = partionedgraph.vertices.map(S => if (S != null) S else (S._1, mutable.HashSet[(String, String)]()))

      val allTypes = new mutable.HashSet[String]()
      val allNamespaces = new mutable.HashSet[String]()
      val allDataSources = new mutable.HashSet[String]()
      val dataSourcesByInstance = new mutable.HashMap[VertexId, mutable.HashSet[String]]()
      val namespacesByInstance = new mutable.HashMap[VertexId, mutable.HashSet[String]]()

      //this is needed since triplets only considers vertices with edges.
      //there can be vertices with no "properties", only "types"
      allTypeSets.collect().foreach(S => {
        if (S._2 != null && S._2.size > 0) {
          for (s <- S._2) {
            allTypes.add(s._1)
            allNamespaces.add(extractNamespace(s._1))
            allDataSources.add(s._2)
          }
        }
      })

      val allProperties = new mutable.HashSet[String]()

      partionedgraph.triplets.collect().foreach(T => {
        allProperties.add(T.attr._2)
        allNamespaces.add(extractNamespace(T.attr._2))
        allDataSources.add(T.attr._4)

        var tmp: mutable.HashSet[String] = null
        if (dataSourcesByInstance.contains(T.srcId))
          tmp = dataSourcesByInstance.get(T.srcId).get
        else
          tmp = new mutable.HashSet[String]()

        tmp = tmp + T.attr._4
        dataSourcesByInstance.put(T.srcId, tmp)

        if (namespacesByInstance.contains(T.srcId))
          tmp = namespacesByInstance.get(T.srcId).get
        else
          tmp = new mutable.HashSet[String]()

        tmp = tmp + extractNamespace(T.attr._2)
        namespacesByInstance.put(T.srcId, tmp)

        if (T.srcAttr != null && T.srcAttr.size > 0) {
          val localDataSources = new mutable.HashSet[String]()
          val localNamespaces = new mutable.HashSet[String]()
          for (s <- T.srcAttr) {
            localNamespaces.add(extractNamespace(s._1))
            localDataSources.add(s._2)
          }

          if (dataSourcesByInstance.contains(T.srcId))
            localDataSources ++ dataSourcesByInstance.get(T.srcId).get

          dataSourcesByInstance.put(T.srcId, localDataSources)

          if (namespacesByInstance.contains(T.srcId))
            localNamespaces ++ namespacesByInstance.get(T.srcId).get

          namespacesByInstance.put(T.srcId, localNamespaces)
        }

        //instance definition, if it has outgoing properties!
      })

      writer.write("Unique Types: " + allTypes.size + "\n")
      writer.write("Unique Properties: " + allProperties.size + "\n")
      writer.write("Unique Data Sources: " + allDataSources.size + "\n")
      writer.write("Unique Namespaces: " + allNamespaces.size + "\n")

      val namespaceSizes = namespacesByInstance.values.map(S => S.size)
      val sumNS = namespaceSizes.reduce(sum)
      val minNS = namespaceSizes.reduce(min)
      val maxNS = namespaceSizes.reduce(max)
      val avgNS = sumNS.asInstanceOf[Double] / numberOfInstances.asInstanceOf[Double]
      val devNS = standardDeviation(namespaceSizes, avgNS)


      val dataSourceSizes = dataSourcesByInstance.values.map(S => S.size)
      val sumDS = dataSourceSizes.reduce(sum)
      val minDS = dataSourceSizes.reduce(min)
      val maxDS = dataSourceSizes.reduce(max)
      val avgDS = sumDS.asInstanceOf[Double] / numberOfInstances.asInstanceOf[Double]
      val devDS = standardDeviation(dataSourceSizes, avgDS)
      val moreThan1DS = dataSourcesByInstance.values.map(V => {
        if (V.size > 1) 1 else 0
      }).reduce(sum)

      writer.write("Avg. Namespaces per Instance = " + avgNS + " (SD = " + devNS + ") \n")
      writer.write("Min Namespaces per Instance = " + minNS + "\n")
      writer.write("Max Namespaces per Instance = " + maxNS + "\n")
      writer.write("Avg. Datasources per Instance = " + avgDS + " (SD = " + devDS + ") \n")
      writer.write("Min Datasources per Instance = " + minDS + "\n")
      writer.write("Max Datasources per Instance = " + maxDS + "\n")

      writer.write("Instance distributively defined = " + moreThan1DS + "\n")


      val connectedComponents = partionedgraph.asInstanceOf[Graph[Set[(String, String)], (String, String, String, String)]].connectedComponents()
      //
      val componentKeys = connectedComponents.vertices.map(V => (V._2, Set(V._1))).reduceByKey(_ ++ _)
      writer.write("|CC| = " + componentKeys.count() + "\n")


      //only calculate paths for instances
      val shortestPaths = ShortestPaths.run(partionedgraph, partionedgraph.vertices.filter(D => instanceIDs.contains(D._1)).map(V => V._1).collect().toSeq)

      //longest shortest path for paths > 0
      val paths = shortestPaths.vertices.map(V => V._2.filter(p => p._2 > 0))
      val diameter = maxSeq(paths.map(M => maxSeq(M.values)).collect())
      val sumOfAllPathLengths = sumSeq(paths.map(M => sumSeq(M.values)).collect())
      val numberOfPaths = paths.map(M => M.values.size).reduce(sum)

      println("sumOfAllPathLengths: " + sumOfAllPathLengths)
      println("numberOfPaths: " + numberOfPaths)
      writer.write("Diameter = " + diameter + "\n")
      writer.write("Avg. Path length: " + (sumOfAllPathLengths.asInstanceOf[Double] / numberOfPaths.asInstanceOf[Double]) + "\n")


      //      val shortestPaths = ShortestPaths.run(partionedgraph, partionedgraph.vertices.keys.collect().toSeq)
      //
      //      writer.write("Diameter = " + maxSeq(shortestPaths.vertices.map(maxPath).collect().toSeq))
      writer.close()
      sc.stop
    }

    ChangeTracker.getInstance()
  }
}


object StatisticsMain {
  def main(args: Array[String]) {

    // this can be set into the JVM environment variables, you can easily find it on google
    if (args.isEmpty) {
      println("Need input file")
      return
    } else
      println("Input file:" + args(0))

    val pipeline: StatisticsPipeline = new StatisticsPipeline(inputFiles = args)

    //recommended to wait 1sec after timestamp since time is measured in seconds (not ms)
    pipeline.start()
  }
}
