package database

import java.io.File
import java.util

import com.typesafe.config.ConfigFactory
import schema.{SE_AttributeCollection, SE_ClassCollection, SE_ComplexAttributeClassCollection, SchemaExtraction}

class MyConfig(fileName: String) {

  object VARS {
    /** *********************************/
    /** * CONFIG: ***/
    //SPARK:
    val spark_name = "spark.name"
    val spark_master = "spark.master"
    val spark_work_dir = "spark.workDir"
    val spark_log_dir = "spark.logDir"
    val spark_memory = "spark.memory"
    val spark_cores = "spark.cores"
    val spark_partitions = "spark.partitions"
    //OrientDB
    val db_url = "database.url"
    val db_name = "database.name"
    val db_user = "database.username"
    val db_password = "database.password"
    val db_fakeMode = "database.fakeMode"
    //input
    val input_filename = "input.filename"
    val input_folder = "input.folder"
    val input_namespace = "input.namespace"
    val input_defaultSource = "input.defaultSource"
    //schema
    val schema_indexModel = "schema.indexModel"
    val schema_undirected = "schema.undirected"
    val schema_classSignal = "schema.classSignal"
    val schema_payload = "schema.payload"
    //Experimental Setup
    val igsi_deltaGraphUpdates = "igsi.deltaGraphUpdates"
    val igsi_clearRepo = "igsi.clearRepo" //delete repo if exists + create a new one
    val igsi_trackUpdateTimes = "igsi.trackUpdateTimes"
    val igsi_trackPrimaryChanges = "igsi.trackPrimaryChanges"
    val igsi_trackSecondaryChanges = "igsi.trackSecondaryChanges"
    val igsi_trackTertiaryChanges = "igsi.trackTertiaryChanges"
    val igsi_trackVHIMemory = "igsi.trackVHIMemory"
    val igsi_alsoBatch = "igsi.alsoBatch"
    val igsi_onlyBatch = "igsi.onlyBatch"
    val igsi_minWait = "igsi.minWait"
    val igsi_logChangesDir = "igsi.logChangesDir"
    /** *********************************/

  }


  val INDEX_MODELS: util.HashMap[String, SchemaExtraction] = new util.HashMap[String, SchemaExtraction]
  INDEX_MODELS.put("complex-attribute_class-collection", SE_ComplexAttributeClassCollection)
  INDEX_MODELS.put("attribute-collection", SE_AttributeCollection)
  INDEX_MODELS.put("class-collection", SE_ClassCollection)


  val config = ConfigFactory.parseFile(new File(fileName))
  //  val config = ConfigFactory.load(fileName)

  config.entrySet().forEach(E => println(E))

  def exists(name: String): Boolean = {
    config.hasPath(name);
  }
  def getStringList(name: String): java.util.List[String] = {
    config.getStringList(name)
  }
  def getString(name: String): String = {
    config.getString(name)
  }

  def getBoolean(name: String): Boolean = {

    config.getBoolean(name)
  }

  def getLong(name: String): Long = {
    config.getLong(name)
  }

  def getInt(name: String): Int = {
    config.getInt(name)
  }
}
