import org.apache.spark.{SparkConf, SparkContext}

object Item {



  def main(args:Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val data = sc.textFile("resources/timbl-500.nq")


    val items = data.map(Edge.fromNQuad(_)).map(x => (x.start, List(x))).reduceByKey(_++_).collect()


    for (item <- items) {
      println(item._1 + ": ")
      item._2.foreach(x => println("\t" + x))
    }




//      reduceByKey( _ + _ ).
//      sortBy( z => (z._2, z._1), ascending = false ).saveAsTextFile("resources/out.txt")
//
//
//    val words = data.flatMap(line => line.split(" "))
//
//    words.collect()
//    val analyze = new Analyze
//
//    words.foreach(W => analyze.update(W, 1))
//
//    analyze.getStats.forEach((k,v) => println(k + ": " + v))
  }



}
