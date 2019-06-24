package classes

import java.io.File
import java.util

import com.typesafe.config.ConfigFactory
import schema.{SE_SchemEX, SchemaExtraction}

class MyConfig(fileName: String) {

  object VARS {
    /** *********************************/
    /** * CONFIG: ***/
    //SPARK:
    val spark_name = "spark.name"
    val spark_master = "spark.master"
    val spark_log_dir = "spark.logDir"
    //OrientDB
    val db_url = "database.url"
    val db_name = "database.name"
    val db_user = "database.username"
    val db_password = "database.password"
    //input
    val input_filename = "input.filename"
    val input_graphLabel = "input.graphLabel"
    //schema
    val schema_indexModel = "schema.indexModel"
    //Experimental Setup
    val igsi_batch_computation = "igsi.batchComputation" //delete repo if exists + create a new one
    /** *********************************/

  }


  val INDEX_MODELS: util.HashMap[String, SchemaExtraction] = new util.HashMap[String, SchemaExtraction]
  INDEX_MODELS.put("schemex", SE_SchemEX)


  val config = ConfigFactory.parseFile(new File(fileName))
  //  val config = ConfigFactory.load(fileName)

  config.entrySet().forEach(E => println(E))

  def getString(name: String): String = {
    config.getString(name)
  }

  def getBoolean(name: String): Boolean = {
    config.getBoolean(name)
  }
}
