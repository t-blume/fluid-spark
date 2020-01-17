import com.arangodb.spark.{ArangoSpark, WriteOptions, createArangoBuilder, createWriteOptions}
import com.arangodb.{ArangoDB, ArangoDBException}
import input.{NTripleParser, RDFGraphParser}
import org.apache.spark.graphx.PartitionStrategy.RandomVertexCut
import org.apache.spark.rdd.RDD
import schema.{SE_SchemEX, SchemaElement}

import scala.collection.mutable
//import database.MyConfig
import org.apache.spark.{SparkConf, SparkContext}

object ArangoPlayground {

  val conf = new SparkConf().
    setMaster("local[*]").
    set("spark.executor.heartbeatInterval", "10000s").
    set("spark.network.timeout", "86400s").
    set("spark.eventLog.enabled", "true").
    set("spark.eventLog.dir", "/tmp/spark-events").
    set("spark.driver.memory", "1g").
    set("spark.executor.memory", "1g").
    set("spark.driver.maxResultSize", "0").
    set("spark.core.max", "1").
    set("spark.executor.core", "1").
    set("arangodb.host", "127.0.0.1").
    set("arangodb.port", "8529").
    set("arangodb.database", "FLUID").
    set("arangodb.collection", "playground")

  def setupDB(conf: SparkConf): Unit = {
    val arangoDB = new ArangoDB.Builder().host(conf.get("arangodb.host"), conf.get("arangodb.port").toInt).build()
    try {
      arangoDB.db(conf.get("arangodb.database")).collection(conf.get("arangodb.collection")).drop()
    } catch {
      case e: ArangoDBException =>
    }
    arangoDB.db(conf.get("arangodb.database")).createCollection(conf.get("arangodb.collection"))
  }


  def start(): Unit = {
    setupDB(conf)
    conf.setAppName("Playground")
    val sc = SparkContext.getOrCreate(conf)
    val inputFile = "resources/manual-test-0.nq"
    val edges = sc.textFile(inputFile).filter(line => !line.trim.isEmpty).map(line => NTripleParser.parse(line))
    val graph = RDFGraphParser.parse(edges)
    val partionedgraph = graph.partitionBy(RandomVertexCut, 10);


//    val builder = new ArangoDBConfigurationBuilder
//    builder.graph("schema")
//      .withVertexCollection(Constants.CLASS_SCHEMA_ELEMENT)
//      .withEdgeCollection(Constants.CLASS_SCHEMA_RELATION)
//      .configureEdge(Constants.CLASS_SCHEMA_RELATION,Constants.CLASS_SCHEMA_ELEMENT, Constants.CLASS_SCHEMA_ELEMENT)
//
//    // create a ArangoDB graph// create a ArangoDB graph
//
//    val arangoConf = builder.build



    // Add vertices// Add vertices


    val schemaExtraction = SE_SchemEX
    //Schema Summarization:
    val schemaElements = partionedgraph.aggregateMessages[(Int, mutable.HashSet[SchemaElement])](
      triplet => schemaExtraction.sendMessage(triplet),
      (a, b) => schemaExtraction.mergeMessage(a, b))

    val aggregatedSchemaElements = schemaElements.values.reduceByKey(_ ++ _).values.map(set => set.iterator.next())
    //      println(s"Schema Elements: ${aggregatedSchemaElements.size}")
//    aggregatedSchemaElements.foreach(a => {
//      val arangoGraph = GraphFactory.open(arangoConf)
//      val gts = new GraphTraversalSource(arangoGraph)
//
//      // Clone to avoid setup time
//      var g = gts.clone
//      g.addV(Constants.CLASS_SCHEMA_ELEMENT).property(T.key, a.hashCode()).property(Constants.PROPERTY_SCHEMA_VALUES, a.payload)
//      //val v2 = g.addV(Constants.CLASS_SCHEMA_ELEMENT).property(T.id, "3").property("name", "lop").property("lang", "java").next()
//
//      // Add edges
//      g.close()
//      //val e1 = g.addE(Constants.CLASS_SCHEMA_RELATION).from(v1).to(v2).property(T.id, "9").property("weight", 0.4)
//      //gts.close()
//      arangoGraph.close()
//    })

    ArangoSpark.save(aggregatedSchemaElements, conf.get("arangodb.collection"), WriteOptions(conf.get("arangodb.database")))

    sc.stop()

  }


  private def saveRDD[T](rdd: RDD[T], collection: String, options: WriteOptions, map: Iterator[T] => Iterator[Any]): Unit = {
    val writeOptions = createWriteOptions(options, rdd.sparkContext.getConf)
    rdd.foreachPartition { p =>
      if (p.nonEmpty) {
        val arangoDB = createArangoBuilder(writeOptions).build()
        val col = arangoDB.db(writeOptions.database).collection(collection)
        col.insertDocuments(map(p).toList.asJava)
        arangoDB.shutdown()
      }
    }
  }

  def main(args: Array[String]) {
    val playground = ArangoPlayground

    //recommended to wait 1sec after timestamp since time is measured in seconds (not ms)
    playground.start()
  }

}